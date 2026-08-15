package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

type StreamInfo struct {
	Name      string `json:"name"`
	Output    string `json:"output"`
	StartTime string `json:"startTime"`
	PID       int    `json:"pid"`
}

type Recorder struct {
	mu         sync.Mutex
	config     *NVRConfig
	processes  map[string]*exec.Cmd
	streamInfo map[string]*StreamInfo
	active     bool
	duration   int
	epoch      int64
}

func NewRecorder(config *NVRConfig) *Recorder {
	return &Recorder{
		config:     config,
		processes:  make(map[string]*exec.Cmd),
		streamInfo: make(map[string]*StreamInfo),
		duration:   607,
	}
}

func (r *Recorder) StartRecording(durations ...int) {
	dur := r.duration
	if len(durations) > 0 && durations[0] > 0 {
		dur = durations[0]
	}

	log.Printf("Starting recording cycle (%ds)", dur)

	go2cfg, err := loadGo2RTCConfig(r.config.Go2RTCConfigPath)
	if err != nil {
		log.Printf("Error loading go2rtc config: %v", err)
		return
	}

	now := time.Now()
	year := now.Format("2006")
	month := now.Format("01")
	day := now.Format("02")
	currentTime := now.Format("15-04")

	r.mu.Lock()
	r.active = true
	r.epoch++
	myEpoch := r.epoch
	r.mu.Unlock()

	for streamName := range go2cfg.Streams {
		go r.recordStream(streamName, year, month, day, currentTime, dur, myEpoch)
	}

	go func() {
		time.Sleep(time.Duration(dur) * time.Second)
		r.mu.Lock()
		if r.epoch == myEpoch {
			r.active = false
		}
		r.mu.Unlock()
	}()
}

func (r *Recorder) recordStream(streamName, year, month, day, currentTime string, duration int, myEpoch int64) {
	directory := filepath.Join(r.config.BaseDir, streamName, year, month, day)
	if err := os.MkdirAll(directory, 0755); err != nil {
		log.Printf("Error creating directory %s: %v", directory, err)
		return
	}

	outputFile := filepath.Join(directory, fmt.Sprintf("%s.mp4", currentTime))

	cmd := exec.Command("ffmpeg",
		"-hide_banner", "-loglevel", "warning", "-threads", "2",
		"-avoid_negative_ts", "make_zero",
		"-fflags", "+nobuffer+genpts+discardcorrupt",
		"-flags", "low_delay",
		"-rtsp_transport", "tcp",
		"-use_wallclock_as_timestamps", "1",
		"-i", fmt.Sprintf("%s/%s", r.config.StreamServer, streamName),
		"-reset_timestamps", "1", "-strftime", "1",
		"-c:v", "copy", "-c:a", "aac", "-strict", "experimental",
		"-movflags", "+faststart",
		"-t", fmt.Sprintf("%d", duration),
		outputFile,
	)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	r.mu.Lock()
	r.processes[streamName] = cmd
	r.streamInfo[streamName] = &StreamInfo{
		Name:      streamName,
		Output:    outputFile,
		StartTime: time.Now().Format("15:04:05"),
		PID:       0,
	}
	r.mu.Unlock()

	log.Printf("Recording stream %s to %s", streamName, outputFile)

	if err := cmd.Start(); err != nil {
		log.Printf("Error starting ffmpeg for %s: %v", streamName, err)
		r.mu.Lock()
		if r.epoch == myEpoch {
			delete(r.processes, streamName)
			delete(r.streamInfo, streamName)
		}
		r.mu.Unlock()
		return
	}

	r.mu.Lock()
	if info, ok := r.streamInfo[streamName]; ok && cmd.Process != nil {
		info.PID = cmd.Process.Pid
	}
	r.mu.Unlock()

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	select {
	case err := <-done:
		if err != nil {
			log.Printf("Error recording stream %s: %v", streamName, err)
		} else {
			log.Printf("Stream %s recording finished successfully", streamName)
		}
	case <-time.After(time.Duration(duration+30) * time.Second):
		log.Printf("Stream %s: duration+30s reached, sending SIGTERM", streamName)
		gracefulStop(cmd, 15*time.Second)
		log.Printf("Stream %s recording completed", streamName)
	}

	r.mu.Lock()
	if r.epoch == myEpoch {
		delete(r.processes, streamName)
		delete(r.streamInfo, streamName)
	}
	r.mu.Unlock()
}

func gracefulStop(cmd *exec.Cmd, grace time.Duration) {
	if cmd.Process == nil {
		return
	}
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)

	timer := time.NewTimer(grace)
	defer timer.Stop()

	select {
	case <-timer.C:
		log.Printf("Process %d did not exit within %v, sending SIGKILL", cmd.Process.Pid, grace)
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
}

func (r *Recorder) StartRecordingStream(name string) {
	dur := r.duration

	now := time.Now()
	year := now.Format("2006")
	month := now.Format("01")
	day := now.Format("02")
	currentTime := now.Format("15-04")

	r.mu.Lock()
	r.active = true
	r.epoch++
	myEpoch := r.epoch
	r.mu.Unlock()

	go r.recordStream(name, year, month, day, currentTime, dur, myEpoch)

	go func() {
		time.Sleep(time.Duration(dur) * time.Second)
		r.mu.Lock()
		if r.epoch == myEpoch {
			r.active = false
		}
		r.mu.Unlock()
	}()
}

func (r *Recorder) StopRecordingStream(name string) {
	r.mu.Lock()
	cmd, ok := r.processes[name]
	if ok {
		delete(r.processes, name)
		delete(r.streamInfo, name)
	}
	r.mu.Unlock()

	if ok && cmd != nil && cmd.Process != nil {
		log.Printf("Stopping recording stream %s (PID %d)", name, cmd.Process.Pid)
		gracefulStop(cmd, 5*time.Second)
	}
}

func (r *Recorder) StopRecording() {
	r.mu.Lock()
	cmds := make([]*exec.Cmd, 0, len(r.processes))
	for _, cmd := range r.processes {
		cmds = append(cmds, cmd)
	}
	r.processes = make(map[string]*exec.Cmd)
	r.streamInfo = make(map[string]*StreamInfo)
	r.active = false
	r.mu.Unlock()

	for _, cmd := range cmds {
		if cmd != nil && cmd.Process != nil {
			log.Printf("Stopping recording (PID %d)", cmd.Process.Pid)
			gracefulStop(cmd, 5*time.Second)
		}
	}
}

func (r *Recorder) IsRecording() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.active
}

func (r *Recorder) GetStatus() map[string]any {
	r.mu.Lock()
	defer r.mu.Unlock()

	streams := make([]string, 0, len(r.processes))
	for name := range r.processes {
		streams = append(streams, name)
	}

	info := make([]*StreamInfo, 0, len(r.streamInfo))
	for _, s := range r.streamInfo {
		info = append(info, s)
	}

	return map[string]any{
		"recording": r.active,
		"streams":   streams,
		"duration":  r.duration,
		"processes": info,
	}
}
