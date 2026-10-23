package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"simple_nvr/internal/alarm"
	"simple_nvr/internal/auth"
	"simple_nvr/internal/config"
	"simple_nvr/internal/logs"
	"simple_nvr/internal/storage"
)

// fakeRecorder records calls instead of running ffmpeg.
type fakeRecorder struct {
	mu          sync.Mutex
	recording   bool
	started     []string
	stopped     []string
	startCalls  int
	stopCalls   int
	activePaths map[string]bool
}

func (f *fakeRecorder) StartRecording(durations ...int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recording = true
	f.startCalls++
}

func (f *fakeRecorder) StopRecording() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recording = false
	f.stopCalls++
}

func (f *fakeRecorder) IsRecording() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.recording
}

func (f *fakeRecorder) GetStatus() map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	return map[string]any{"recording": f.recording}
}

func (f *fakeRecorder) ActiveRecordings() map[string]bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]bool, len(f.activePaths))
	for k, v := range f.activePaths {
		out[k] = v
	}
	return out
}

func (f *fakeRecorder) StartRecordingStream(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.started = append(f.started, name)
}

func (f *fakeRecorder) StopRecordingStream(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stopped = append(f.stopped, name)
}

// testEnv bundles an API wired to temp dirs and stubs.
type testEnv struct {
	t          *testing.T
	api        *API
	rec        *fakeRecorder
	users      *auth.UserStore
	baseDir    string
	archiveDir string
	configPath string
	go2rtcPath string
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	dir := t.TempDir()
	baseDir := filepath.Join(dir, "recordings")
	archiveDir := filepath.Join(dir, "archive")
	for _, d := range []string{baseDir, archiveDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	go2rtcPath := filepath.Join(dir, "go2rtc.yaml")
	if err := os.WriteFile(go2rtcPath, []byte("streams: {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	configPath := filepath.Join(dir, "nvr.yaml")
	cfg := &config.NVRConfig{
		BaseDir:              baseDir,
		ArchiveDir:           archiveDir,
		Go2RTCConfigPath:     go2rtcPath,
		DefaultCameraLimitGB: 50,
		GlobalSizeGB:         200,
		HTTPPort:             8080,
		AlarmPort:            15002,
		KioskPort:            8181,
		StreamServer:         "rtsp://127.0.0.1:8554",
		UsersFile:            filepath.Join(dir, "users.json"),
		CameraLimits:         map[string]int{},
		CameraDayLimits:      map[string]int{},
	}

	rec := &fakeRecorder{activePaths: map[string]bool{}}
	users := auth.NewUserStore(cfg.UsersFile)
	alarmSrv := alarm.NewAlarmServer(cfg, nil)
	alarmSrv.SetEventDir(filepath.Join(dir, "alarms"))
	hikvision := alarm.NewHikvisionAlarmServer(cfg, alarmSrv, nil)

	apiSrv := NewAPI(Options{
		Config:         cfg,
		ConfigPath:     configPath,
		Recorder:       rec,
		Storage:        storage.NewStorage(cfg),
		Alarm:          alarmSrv,
		HikvisionAlarm: hikvision,
		LogBuffer:      logs.NewLogBuffer(10),
		UserStore:      users,
		Version:        "test",
	})

	return &testEnv{
		t:          t,
		api:        apiSrv,
		rec:        rec,
		users:      users,
		baseDir:    baseDir,
		archiveDir: archiveDir,
		configPath: configPath,
		go2rtcPath: go2rtcPath,
	}
}

// serve mounts the API on a test server. A test-only middleware injects the
// request identity from the X-Test-User / X-Test-Role headers, standing in
// for the real RequireAuth middleware.
func (e *testEnv) serve() *httptest.Server {
	mux := http.NewServeMux()
	e.api.RegisterRoutes(mux)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if user := r.Header.Get("X-Test-User"); user != "" {
			r = auth.WithIdentity(r, user, r.Header.Get("X-Test-Role"))
		}
		mux.ServeHTTP(w, r)
	}))
	e.t.Cleanup(srv.Close)
	return srv
}

// adminRequest builds a client-usable request carrying an admin identity via
// the test middleware's headers.
func adminRequest(method, url string, body string) *http.Request {
	var r *http.Request
	if body != "" {
		r, _ = http.NewRequest(method, url, strings.NewReader(body))
	} else {
		r, _ = http.NewRequest(method, url, nil)
	}
	r.Header.Set("X-Test-User", "admin")
	r.Header.Set("X-Test-Role", auth.RoleAdmin)
	return r
}

// userRequest builds a client-usable request carrying a plain-user identity.
func userRequest(method, url string, body string) *http.Request {
	var r *http.Request
	if body != "" {
		r, _ = http.NewRequest(method, url, strings.NewReader(body))
	} else {
		r, _ = http.NewRequest(method, url, nil)
	}
	r.Header.Set("X-Test-User", "bob")
	r.Header.Set("X-Test-Role", auth.RoleUser)
	return r
}

// decodeJSON performs the request and decodes the response body.
func decodeJSON(t *testing.T, resp *http.Response, v any) {
	t.Helper()
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		t.Fatalf("decode response: %v", err)
	}
}

// writeMP4 creates a fake recording file under base/<camera>/<folder>.
func (e *testEnv) writeMP4(camera, folder, name string, size int) string {
	e.t.Helper()
	path := filepath.Join(e.baseDir, camera, folder, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		e.t.Fatal(err)
	}
	if err := os.WriteFile(path, make([]byte, size), 0o644); err != nil {
		e.t.Fatal(err)
	}
	return path
}
