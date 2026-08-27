package recorder

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"simple_nvr/internal/config"
)

type StreamInfo struct {
	Name      string `json:"name"`
	Output    string `json:"output"`
	StartTime string `json:"startTime"`
	PID       int    `json:"pid"`
	Duration  int    `json:"duration"`
	Healthy   bool   `json:"healthy"`
}

type Recorder struct {
	mu         sync.Mutex
	config     *config.NVRConfig
	processes  map[string]*exec.Cmd
	streamInfo map[string]*StreamInfo
	active     bool
	duration   int
	epoch      int64
	failCount  map[string]int
}

func NewRecorder(cfg *config.NVRConfig) *Recorder {
	r := &Recorder{
		config:     cfg,
		processes:  make(map[string]*exec.Cmd),
		streamInfo: make(map[string]*StreamInfo),
		duration:   607,
		failCount:  make(map[string]int),
	}
	go r.healthCheck()
	return r
}

func (r *Recorder) StartRecording(durations ...int) {
	r.startRecordingAt(time.Now(), durations...)
}

func (r *Recorder) StartRecordingScheduled(scheduledTime time.Time, durations ...int) {
	r.startRecordingAt(scheduledTime, durations...)
}

func (r *Recorder) startRecordingAt(scheduledTime time.Time, durations ...int) {
	dur := r.duration
	if len(durations) > 0 && durations[0] > 0 {
		dur = durations[0]
	}

	log.Printf("Starting recording cycle (%ds) scheduled at %s", dur, scheduledTime.Format("15:04:05"))

	go2cfg, err := config.LoadGo2RTCConfig(r.config.Go2RTCConfigPath)
	if err != nil {
		log.Printf("Error loading go2rtc config: %v", err)
		return
	}

	now := time.Now()
	year := scheduledTime.Format("2006")
	month := scheduledTime.Format("01")
	day := scheduledTime.Format("02")
	currentTime := now.Format("15-04")

	r.mu.Lock()
	r.active = true
	r.epoch++
	myEpoch := r.epoch
	r.failCount = make(map[string]int)
	r.mu.Unlock()

	delay := 0
	for streamName := range go2cfg.Streams {
		time.Sleep(time.Duration(delay) * time.Millisecond)
		go r.recordStream(streamName, year, month, day, currentTime, dur, myEpoch)
		delay += 250
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

	err := r.runFFmpeg(streamName, outputFile, duration, myEpoch)
	if err != nil {
		log.Printf("Error recording stream %s: %v", streamName, err)
	} else {
		log.Printf("Stream %s recording finished successfully", streamName)
	}
}

func (r *Recorder) runFFmpeg(streamName, outputFile string, duration int, myEpoch int64) error {
	timeoutSec := duration + 60
	cmd := exec.Command("timeout", strconv.Itoa(timeoutSec),
		"ionice", "-c3",
		"ffmpeg",
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
		Duration:  duration,
		Healthy:   true,
	}
	r.mu.Unlock()

	log.Printf("Recording stream %s to %s", streamName, outputFile)

	if err := cmd.Start(); err != nil {
		log.Printf("Error starting ffmpeg for %s: %v", streamName, err)
		r.mu.Lock()
		delete(r.processes, streamName)
		delete(r.streamInfo, streamName)
		r.mu.Unlock()
		return err
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

	var err error
	select {
	case err = <-done:
		if err != nil {
			return err
		}
		log.Printf("Stream %s recording finished successfully", streamName)
	case <-time.After(time.Duration(duration+30) * time.Second):
		log.Printf("Stream %s: duration+30s reached, sending SIGTERM", streamName)
		GracefulStop(cmd, 30*time.Second)
		log.Printf("Stream %s recording completed", streamName)
	}

	r.mu.Lock()
	if r.epoch == myEpoch {
		delete(r.processes, streamName)
		delete(r.streamInfo, streamName)
	}
	r.mu.Unlock()

	return err
}

func GracefulStop(cmd *exec.Cmd, grace time.Duration) {
	if cmd.Process == nil {
		return
	}
	pid := cmd.Process.Pid
	pgid := -pid

	if err := syscall.Kill(pid, 0); err != nil {
		log.Printf("[stop] PID %d already dead: %v", pid, err)
		return
	}

	if state, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid)); err == nil {
		for _, line := range strings.Split(string(state), "\n") {
			if strings.HasPrefix(line, "State:") {
				log.Printf("[stop] PID %d state: %s", pid, strings.TrimSpace(line))
				break
			}
		}
	}

	log.Printf("[stop] Sending SIGTERM to pgid %d (PID %d)", pgid, pid)
	err := syscall.Kill(pgid, syscall.SIGTERM)
	log.Printf("[stop] SIGTERM result: %v", err)

	timer := time.NewTimer(grace)
	defer timer.Stop()

	exited := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(exited)
	}()

	select {
	case <-exited:
		log.Printf("[stop] PID %d exited gracefully after SIGTERM", pid)
	case <-timer.C:
		if state, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid)); err == nil {
			for _, line := range strings.Split(string(state), "\n") {
				if strings.HasPrefix(line, "State:") {
					log.Printf("[stop] PID %d still alive after %v, state: %s", pid, grace, strings.TrimSpace(line))
					break
				}
			}
		}
		log.Printf("[stop] Sending SIGKILL to pgid %d", pgid)
		_ = syscall.Kill(pgid, syscall.SIGKILL)
		<-exited
		log.Printf("[stop] PID %d killed by SIGKILL", pid)
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
	r.failCount = make(map[string]int)
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
		GracefulStop(cmd, 5*time.Second)
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
			GracefulStop(cmd, 5*time.Second)
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

func (r *Recorder) ActiveRecordings() map[string]bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	paths := make(map[string]bool, len(r.streamInfo))
	for _, s := range r.streamInfo {
		if s.Output != "" {
			paths[s.Output] = true
		}
	}
	return paths
}

func (r *Recorder) Config() *config.NVRConfig {
	return r.config
}

func (r *Recorder) StartHealthCheck() {
	go r.healthCheck()
}

func (r *Recorder) healthCheck() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		r.mu.Lock()
		if !r.active {
			r.mu.Unlock()
			continue
		}

		currentEpoch := r.epoch
		infos := make([]*StreamInfo, 0, len(r.streamInfo))
		for _, s := range r.streamInfo {
			infos = append(infos, s)
		}
		r.mu.Unlock()

		for _, info := range infos {
			r.checkStream(info, currentEpoch)
		}

		go2cfg, err := config.LoadGo2RTCConfig(r.config.Go2RTCConfigPath)
		if err == nil {
			for streamName := range go2cfg.Streams {
				r.mu.Lock()
				_, exists := r.streamInfo[streamName]
				epoch := r.epoch
				failCnt := r.failCount[streamName]
				r.mu.Unlock()
				if !exists && epoch == currentEpoch {
				if failCnt >= 3 {
					continue
				}
					log.Printf("Health check: stream %s not in processes (fail %d/3), restarting", streamName, failCnt+1)
					r.mu.Lock()
					r.failCount[streamName] = failCnt + 1
					r.mu.Unlock()
					r.restartStream(streamName, currentEpoch)
				}
			}
		}
	}
}

func (r *Recorder) checkStream(info *StreamInfo, currentEpoch int64) {
	r.mu.Lock()
	cmd, ok := r.processes[info.Name]
	epoch := r.epoch
	failCnt := r.failCount[info.Name]
	r.mu.Unlock()

	if !ok || epoch != currentEpoch {
		return
	}

	healthy := true

	if cmd.Process != nil {
		if err := syscall.Kill(cmd.Process.Pid, 0); err != nil {
			healthy = false
		}
	}

	r.mu.Lock()
	if s, ok := r.streamInfo[info.Name]; ok {
		s.Healthy = healthy
	}
	r.mu.Unlock()

	if !healthy {
		if failCnt >= 3 {
			return
		}

		log.Printf("Health check: stream %s unhealthy (fail %d/3), restarting", info.Name, failCnt+1)
		r.mu.Lock()
		r.failCount[info.Name] = failCnt + 1
		r.mu.Unlock()

		r.restartStream(info.Name, currentEpoch)
	}
}

func (r *Recorder) restartStream(name string, currentEpoch int64) {
	r.mu.Lock()
	if r.epoch != currentEpoch {
		r.mu.Unlock()
		return
	}
	oldCmd, ok := r.processes[name]
	if ok {
		delete(r.processes, name)
		delete(r.streamInfo, name)
	}
	r.mu.Unlock()

	if ok && oldCmd != nil && oldCmd.Process != nil {
		log.Printf("Health check: stopping unhealthy process for %s (PID %d)", name, oldCmd.Process.Pid)
		GracefulStop(oldCmd, 5*time.Second)
	}

	now := time.Now()
	year := now.Format("2006")
	month := now.Format("01")
	day := now.Format("02")
	currentTime := now.Format("15-04")

	log.Printf("Health check: restarting recording for %s", name)
	go r.recordStream(name, year, month, day, currentTime, r.duration, currentEpoch)
}
