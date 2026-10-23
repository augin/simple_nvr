package recorder

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"simple_nvr/internal/config"
)

// newTestRecorder builds a Recorder rooted in a temp dir with an empty
// go2rtc config (so cycles start no stream goroutines).
func newTestRecorder(t *testing.T) *Recorder {
	t.Helper()
	base := t.TempDir()
	go2rtcPath := filepath.Join(base, "go2rtc.yaml")
	if err := os.WriteFile(go2rtcPath, []byte("streams: {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := &config.NVRConfig{
		BaseDir:          base,
		StreamServer:     "http://127.0.0.1:1984",
		Go2RTCConfigPath: go2rtcPath,
	}
	return NewRecorder(cfg)
}

func TestBeginCycleOpensAndClosesHealth(t *testing.T) {
	r := newTestRecorder(t)

	r.mu.Lock()
	e1, ch1 := r.beginCycleLocked()
	if !r.healthOpen || r.active != true || e1 != 1 {
		t.Errorf("beginCycle state: open=%v active=%v epoch=%d", r.healthOpen, r.active, e1)
	}
	// Повторный цикл закрывает старый канал и открывает новый (раньше — panic
	// "close of closed channel" при Stop без пересоздания канала).
	e2, ch2 := r.beginCycleLocked()
	if e2 != 2 || ch1 == ch2 {
		t.Errorf("second cycle: epoch=%d sameChan=%v", e2, ch1 == ch2)
	}
	select {
	case <-ch1:
	default:
		t.Error("previous health channel should be closed")
	}
	r.closeHealthLocked()
	if r.healthOpen {
		t.Error("health channel should be closed")
	}
	// Повторное закрытие не паникует.
	r.closeHealthLocked()
	r.mu.Unlock()
}

func TestStopTwiceAndRestartDoesNotPanic(t *testing.T) {
	r := newTestRecorder(t)

	r.StartRecording(607)
	if !r.IsRecording() {
		t.Fatal("recorder should be active after StartRecording")
	}
	r.StopRecording()
	if r.IsRecording() {
		t.Fatal("recorder should be inactive after StopRecording")
	}
	// Второй Stop и повторный Start раньше падали на close закрытого канала.
	r.StopRecording()
	r.StartRecording(607)
	if !r.IsRecording() {
		t.Fatal("recorder should be active after restart")
	}
	r.StopRecording()
}

func TestStartRecordingBadGo2RTCConfig(t *testing.T) {
	r := newTestRecorder(t)
	r.config.Go2RTCConfigPath = filepath.Join(t.TempDir(), "missing.yaml")
	r.StartRecording(607)
	if r.IsRecording() {
		t.Error("start should bail out before activating when go2rtc config is unreadable")
	}
	r.StopRecording() // must not panic
}

func TestGetStatusAndActiveRecordings(t *testing.T) {
	r := newTestRecorder(t)

	status := r.GetStatus()
	if status["recording"] != false || status["duration"] != 607 {
		t.Errorf("status = %v", status)
	}
	if streams, ok := status["streams"].([]string); !ok || len(streams) != 0 {
		t.Errorf("streams = %v", status["streams"])
	}

	r.mu.Lock()
	r.processes["cam1"] = exec.Command("true")
	r.streamInfo["cam1"] = &StreamInfo{Name: "cam1", Output: "/tmp/a.mp4", Healthy: true, Epoch: 1}
	r.processes["cam2"] = exec.Command("true")
	r.streamInfo["cam2"] = &StreamInfo{Name: "cam2", Output: "", Healthy: false, Epoch: 1}
	r.mu.Unlock()

	status = r.GetStatus()
	if got := status["recording"]; got != false {
		t.Errorf("recording = %v", got)
	}
	info := status["processes"].([]*StreamInfo)
	if len(info) != 2 {
		t.Errorf("processes = %v", info)
	}

	active := r.ActiveRecordings()
	if len(active) != 1 || !active["/tmp/a.mp4"] {
		t.Errorf("ActiveRecordings = %v", active)
	}

	if r.Config() != r.config {
		t.Error("Config should return the underlying config")
	}
}

func TestStopRecordingStreamUnknown(t *testing.T) {
	r := newTestRecorder(t)
	r.StopRecordingStream("nope") // no panic, no side effects
	if len(r.ActiveRecordings()) != 0 {
		t.Error("unexpected active recordings")
	}
}

func TestGracefulStop(t *testing.T) {
	// nil process — no-op.
	GracefulStop(&exec.Cmd{}, time.Second)

	// Already dead process — no panic.
	dead := exec.Command("true")
	if err := dead.Start(); err != nil {
		t.Fatal(err)
	}
	dead.Wait()
	GracefulStop(dead, time.Second)

	// Live process is killed within the grace period.
	live := exec.Command("sleep", "30")
	live.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := live.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if live.Process != nil {
			_ = syscall.Kill(-live.Process.Pid, syscall.SIGKILL)
		}
	}()
	start := time.Now()
	GracefulStop(live, 5*time.Second)
	if elapsed := time.Since(start); elapsed > 4*time.Second {
		t.Errorf("GracefulStop took %v, expected prompt SIGTERM exit", elapsed)
	}
}

func TestHealthCheckLoopStopsOnClosedChannel(t *testing.T) {
	r := newTestRecorder(t)
	r.mu.Lock()
	_, stopCh := r.beginCycleLocked()
	r.mu.Unlock()

	done := make(chan struct{})
	go func() {
		r.healthCheckLoop(stopCh)
		close(done)
	}()

	r.mu.Lock()
	r.closeHealthLocked()
	r.mu.Unlock()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("healthCheckLoop did not stop after channel close")
	}
}

// --- ffmpeg процесс ---------------------------------------------------------

// installStubs кладёт во временный каталог заглушки timeout/ionice/ffmpeg и
// возвращает каталог в PATH. Файл argsPath получает аргументы "timeout",
// mode управляет поведением (ok|fail|segv).
func installStubs(t *testing.T, argsPath, mode string) {
	t.Helper()
	dir := t.TempDir()
	write := func(name, body string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\n"+body+"\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	write("timeout", `echo "$@" > "`+argsPath+`"
case "$STUB_MODE" in
  segv) kill -SEGV $$ ;;
  *) exit ${STUB_EXIT:-0} ;;
esac`)
	write("ionice", `exit 0`)
	write("ffmpeg", `exit 0`)
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("STUB_MODE", mode)
}

func TestRunFFmpegSuccess(t *testing.T) {
	r := newTestRecorder(t)
	argsPath := filepath.Join(t.TempDir(), "args")
	installStubs(t, argsPath, "ok")

	out := filepath.Join(t.TempDir(), "out.mp4")
	if err := r.runFFmpeg("cam1", out, 5, 1); err != nil {
		t.Fatalf("runFFmpeg: %v", err)
	}

	// После успешного завершения процесс убран из maps.
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.processes) != 0 || len(r.streamInfo) != 0 {
		t.Errorf("processes=%v streamInfo=%v", r.processes, r.streamInfo)
	}

	args, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatal(err)
	}
	got := string(args)
	for _, want := range []string{"65", "ionice", "ffmpeg", "-rtsp_transport", "tcp", "http://127.0.0.1:1984/cam1", out} {
		if !strings.Contains(got, want) {
			t.Errorf("ffmpeg args missing %q: %s", want, got)
		}
	}
}

func TestRunFFmpegFailureCleansUp(t *testing.T) {
	r := newTestRecorder(t)
	argsPath := filepath.Join(t.TempDir(), "args")
	installStubs(t, argsPath, "fail")
	t.Setenv("STUB_EXIT", "7")

	err := r.runFFmpeg("cam1", filepath.Join(t.TempDir(), "out.mp4"), 5, 1)
	if err == nil {
		t.Fatal("expected error from failing ffmpeg")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.processes) != 0 || len(r.streamInfo) != 0 {
		t.Errorf("failed run must clean maps: processes=%v streamInfo=%v", r.processes, r.streamInfo)
	}
}

func TestRecordStreamSegfaultRestartsService(t *testing.T) {
	r := newTestRecorder(t)
	argsPath := filepath.Join(t.TempDir(), "args")
	installStubs(t, argsPath, "segv")

	var mu sync.Mutex
	restarts, exits := 0, 0
	oldRestart, oldExit := restartServiceFn, exitFn
	restartServiceFn = func() error { mu.Lock(); restarts++; mu.Unlock(); return nil }
	exitFn = func(int) { mu.Lock(); exits++; mu.Unlock() }
	defer func() { restartServiceFn, exitFn = oldRestart, oldExit }()

	// Первые два segfault — только счётчик.
	for i := 0; i < 2; i++ {
		r.recordStream("cam1", "2026", "10", "21", "12-00-00", 5, 1)
	}
	mu.Lock()
	if restarts != 0 || exits != 0 {
		t.Errorf("after 2 segfaults: restarts=%d exits=%d", restarts, exits)
	}
	mu.Unlock()

	// Третий — перезапуск сервиса (и без systemd — выход из процесса).
	r.recordStream("cam1", "2026", "10", "21", "12-00-00", 5, 1)
	mu.Lock()
	if restarts != 1 {
		t.Errorf("restarts = %d, want 1", restarts)
	}
	if exits != 0 {
		t.Errorf("exits = %d, want 0 when systemctl works", exits)
	}
	mu.Unlock()

	// Успешный запуск сбрасывает глобальный счётчик segfault.
	t.Setenv("STUB_MODE", "ok")
	r.recordStream("cam1", "2026", "10", "21", "12-00-00", 5, 1)
	t.Setenv("STUB_MODE", "segv")
	for i := 0; i < 3; i++ {
		r.recordStream("cam1", "2026", "10", "21", "12-00-00", 5, 1)
	}
	mu.Lock()
	defer mu.Unlock()
	if restarts != 2 {
		t.Errorf("restarts = %d, want 2 (counter was reset)", restarts)
	}
}

func TestRecordStreamSegfaultExitsWithoutSystemd(t *testing.T) {
	r := newTestRecorder(t)
	argsPath := filepath.Join(t.TempDir(), "args")
	installStubs(t, argsPath, "segv")

	oldRestart, oldExit := restartServiceFn, exitFn
	restartServiceFn = func() error { return errors.New("systemctl unavailable") }
	exited := make(chan int, 1)
	exitFn = func(code int) { exited <- code }
	defer func() { restartServiceFn, exitFn = oldRestart, oldExit }()

	for i := 0; i < 3; i++ {
		r.recordStream("cam1", "2026", "10", "21", "12-00-00", 5, 1)
	}
	select {
	case code := <-exited:
		if code != 1 {
			t.Errorf("exit code = %d, want 1", code)
		}
	default:
		t.Error("expected exitFn to be called when systemctl is unavailable")
	}
}

func TestRecordStreamCreatesOutputDir(t *testing.T) {
	r := newTestRecorder(t)
	argsPath := filepath.Join(t.TempDir(), "args")
	installStubs(t, argsPath, "ok")

	r.recordStream("cam1", "2026", "10", "21", "12-00-00", 5, 1)
	want := filepath.Join(r.config.BaseDir, "cam1", "2026", "10", "21")
	if _, err := os.Stat(want); err != nil {
		t.Errorf("output dir not created: %v", err)
	}
	args, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(args), filepath.Join(want, "12-00-00.mp4")) {
		t.Errorf("output path missing in args: %s", args)
	}
}

func TestRecordStreamMissingFFmpeg(t *testing.T) {
	r := newTestRecorder(t)
	// PATH указывает на пустой каталог — timeout не найден.
	t.Setenv("PATH", t.TempDir())
	if err := r.runFFmpeg("cam1", "out.mp4", 5, 1); err == nil {
		t.Fatal("expected error when ffmpeg tooling is missing")
	}
}
