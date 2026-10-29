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
	Epoch     int64  `json:"-"`
}

type Recorder struct {
	mu            sync.Mutex
	config        *config.NVRConfig
	processes     map[string]*exec.Cmd
	streamInfo    map[string]*StreamInfo
	active        bool
	duration      int
	epoch         int64
	failCount     map[string]int
	segfaultTotal int
	// stopHealth stops the health-check loop of the current cycle;
	// healthOpen tells whether that channel is still open (closing an
	// already closed channel panics, and Stop may race with a new Start).
	stopHealth chan struct{}
	healthOpen bool
}

func NewRecorder(cfg *config.NVRConfig) *Recorder {
	r := &Recorder{
		config:     cfg,
		processes:  make(map[string]*exec.Cmd),
		streamInfo: make(map[string]*StreamInfo),
		duration:   607,
		failCount:  make(map[string]int),
		stopHealth: make(chan struct{}),
	}
	return r
}

// beginCycleLocked prepares per-cycle state: marks recording active, bumps the
// epoch (so stale timers and health checks become no-ops), resets the failure
// counters and opens a fresh health-check stop channel.
// The caller must hold r.mu.
func (r *Recorder) beginCycleLocked() (int64, chan struct{}) {
	r.active = true
	r.epoch++
	r.failCount = make(map[string]int)
	r.closeHealthLocked()
	r.stopHealth = make(chan struct{})
	r.healthOpen = true
	return r.epoch, r.stopHealth
}

// closeHealthLocked stops the health-check loop of the current cycle, if it
// has not been stopped yet. The caller must hold r.mu.
func (r *Recorder) closeHealthLocked() {
	if r.healthOpen {
		close(r.stopHealth)
		r.healthOpen = false
	}
}

// scheduleCycleEnd deactivates the recorder after the cycle duration,
// unless a newer cycle has started in the meantime.
func (r *Recorder) scheduleCycleEnd(myEpoch int64, dur int) {
	go func() {
		time.Sleep(time.Duration(dur) * time.Second)
		r.mu.Lock()
		if r.epoch == myEpoch {
			r.active = false
		}
		r.mu.Unlock()
	}()
}

// scheduleHealthCheck starts the health-check loop shortly after the cycle
// begins, giving ffmpeg time to spawn first.
func (r *Recorder) scheduleHealthCheck(myEpoch int64, stopCh chan struct{}) {
	go func() {
		time.Sleep(10 * time.Second)
		r.mu.Lock()
		if r.epoch != myEpoch {
			r.mu.Unlock()
			return
		}
		r.mu.Unlock()
		r.healthCheckLoop(stopCh)
	}()
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
	myEpoch, stopCh := r.beginCycleLocked()
	r.mu.Unlock()

	delay := 0
	for streamName := range go2cfg.Streams {
		time.Sleep(time.Duration(delay) * time.Millisecond)
		go r.recordStream(streamName, year, month, day, currentTime, dur, myEpoch)
		delay += 250
	}

	r.scheduleCycleEnd(myEpoch, dur)
	r.scheduleHealthCheck(myEpoch, stopCh)
}

// restartServiceFn restarts the systemd unit; replaced in tests.
var restartServiceFn = func() error {
	_, err := exec.Command("systemctl", "restart", "simple-nvr").CombinedOutput()
	return err
}

// recordStream runs one recording cycle for a single stream and logs the
// outcome (including the global segfault counter that triggers a restart).
func (r *Recorder) recordStream(streamName, year, month, day, currentTime string, duration int, myEpoch int64) {
	startTime := time.Now()
	directory := filepath.Join(r.config.BaseDir, streamName, year, month, day)
	if err := os.MkdirAll(directory, 0755); err != nil {
		log.Printf("Error creating directory %s: %v", directory, err)
		return
	}

	outputFile := filepath.Join(directory, fmt.Sprintf("%s.mp4", currentTime))

	err := r.runFFmpeg(streamName, outputFile, duration, myEpoch)
	elapsed := time.Since(startTime).Truncate(time.Millisecond)
	if err != nil {
		log.Printf("Error recording stream %s: %v (ran %v)", streamName, err, elapsed)

		if strings.Contains(err.Error(), "segmentation fault") {
			r.mu.Lock()
			r.segfaultTotal++
			cnt := r.segfaultTotal
			r.mu.Unlock()
			log.Printf("Stream %s: segfault count %d/3 (global)", streamName, cnt)
			if cnt >= 3 {
				log.Printf("WARNING: %d ffmpeg segfaults with no successful recording in between — likely a broken ffmpeg build or exhausted resources", cnt)
				if err := restartServiceFn(); err != nil {
					// No systemd (e.g. a container with restart: always):
					// exiting here would only trigger a restart loop without
					// fixing ffmpeg. Keep running; the health check retries
					// the failed streams.
					log.Printf("Service restart unavailable (%v); continuing — health check will retry streams", err)
				} else {
					log.Printf("Service restart triggered")
				}
			}
		}
	} else {
		log.Printf("Stream %s recording finished successfully (ran %v, expected %ds)", streamName, elapsed, duration)
		r.mu.Lock()
		r.segfaultTotal = 0
		r.mu.Unlock()
	}
	if elapsed < 5*time.Second {
		log.Printf("WARNING: stream %s exited after only %v — possible camera/connection issue", streamName, elapsed)
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
		Epoch:     myEpoch,
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

	// Cleanup in every exit path (success, failure, SIGKILL) so that stale
	// entries never linger in the maps. The guard is the cmd identity, not the
	// epoch: a health-check restart reuses the same epoch, so an epoch check
	// would let this run delete the bookkeeping of the process that replaced
	// it — orphaning a live ffmpeg that keeps recording untracked.
	defer r.cleanupStream(streamName, cmd)

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
	case <-time.After(time.Duration(duration+15) * time.Second):
		log.Printf("Stream %s: ffmpeg did not exit after %ds, sending SIGTERM", streamName, duration+15)
		if cmd.Process != nil {
			syscall.Kill(cmd.Process.Pid, syscall.SIGTERM)
		}
		select {
		case err = <-done:
			if err != nil {
				return err
			}
			log.Printf("Stream %s exited after SIGTERM", streamName)
		case <-time.After(20 * time.Second):
			log.Printf("Stream %s: still alive after SIGTERM, sending SIGKILL", streamName)
			if cmd.Process != nil {
				pgid := -cmd.Process.Pid
				syscall.Kill(pgid, syscall.SIGKILL)
			}
			<-done
			log.Printf("Stream %s killed by SIGKILL", streamName)
			return fmt.Errorf("recording killed by SIGKILL after %ds", duration+35)
		}
	}

	return err
}

// cleanupStream drops this run's bookkeeping for streamName, but only while
// the maps still point at this run's cmd. A health-check restart replaces the
// entry with a fresh cmd under the same epoch; the replaced run must leave the
// successor's entry alone, or the live successor is orphaned and the health
// check keeps spawning replacements forever.
func (r *Recorder) cleanupStream(streamName string, cmd *exec.Cmd) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.processes[streamName] == cmd {
		delete(r.processes, streamName)
		delete(r.streamInfo, streamName)
	}
}

// processRunning reports whether pid is still executing. A zombie (exited but
// not yet reaped) counts as not running: the process itself has terminated and
// the reaper (runFFmpeg's single Wait) will collect it.
func processRunning(pid int) bool {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return false // no such process, or already reaped
	}
	// /proc/<pid>/stat is "pid (comm) state ..."; comm may contain spaces and
	// parentheses, so parse the state after the final ") ".
	s := string(data)
	i := strings.LastIndex(s, ") ")
	if i < 0 || i+2 >= len(s) {
		return true // unparseable: err on the side of "still alive"
	}
	return s[i+2] != 'Z'
}

// waitForDeath polls until pid terminates or the timeout elapses, reporting
// whether it died in time.
func waitForDeath(pid int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for processRunning(pid) {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(25 * time.Millisecond)
	}
	return true
}

// GracefulStop asks the process group to terminate, escalating SIGTERM →
// SIGKILL, and waits for the processes to die. It deliberately does NOT call
// cmd.Wait(): the goroutine that started the process (runFFmpeg) owns the
// single Wait, and a second concurrent Wait on the same *exec.Cmd is a data
// race. Death is observed through /proc instead.
func GracefulStop(cmd *exec.Cmd, grace time.Duration) {
	if cmd.Process == nil {
		return
	}
	pid := cmd.Process.Pid
	pgid := -pid

	if !processRunning(pid) {
		log.Printf("[stop] PID %d already dead", pid)
		return
	}

	log.Printf("[stop] Sending SIGTERM to pgid %d (PID %d)", pgid, pid)
	if err := syscall.Kill(pgid, syscall.SIGTERM); err != nil {
		log.Printf("[stop] SIGTERM result: %v", err)
	}

	if waitForDeath(pid, grace) {
		log.Printf("[stop] PID %d exited gracefully after SIGTERM", pid)
		return
	}

	log.Printf("[stop] PID %d still alive after %v, sending SIGKILL", pid, grace)
	_ = syscall.Kill(pgid, syscall.SIGKILL)
	if waitForDeath(pid, 10*time.Second) {
		log.Printf("[stop] PID %d killed by SIGKILL", pid)
	} else {
		log.Printf("[stop] PID %d did not die after SIGKILL", pid)
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
	myEpoch, stopCh := r.beginCycleLocked()
	r.mu.Unlock()

	go r.recordStream(name, year, month, day, currentTime, dur, myEpoch)

	r.scheduleCycleEnd(myEpoch, dur)
	r.scheduleHealthCheck(myEpoch, stopCh)
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
	r.closeHealthLocked()
	r.mu.Unlock()

	log.Printf("Stopping %d recording processes...", len(cmds))
	var wg sync.WaitGroup
	for _, cmd := range cmds {
		if cmd != nil && cmd.Process != nil {
			wg.Add(1)
			go func(c *exec.Cmd) {
				defer wg.Done()
				log.Printf("Stopping recording (PID %d)", c.Process.Pid)
				GracefulStop(c, 15*time.Second)
			}(cmd)
		}
	}
	wg.Wait()
	log.Printf("All recordings stopped")
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

func (r *Recorder) healthCheckLoop(stopCh chan struct{}) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
		}

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
					time.Sleep(2 * time.Second)
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
		time.Sleep(2 * time.Second)
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
