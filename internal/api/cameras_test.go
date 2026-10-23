package api

import (
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"testing"

	"simple_nvr/internal/config"
	"simple_nvr/internal/recorder"
)

func TestHandleCamerasListsNames(t *testing.T) {
	env := newTestEnv(t)
	// StreamOrder is derived from the key order inside "streams", so the
	// listing must follow cam2, cam1.
	go2yaml := "streams:\n  cam2: dvrip://user:pass@10.0.0.6?channel=1&subtype=0\n  cam1: rtsp://user:pass@10.0.0.5:554/stream0\n"
	if err := os.WriteFile(env.go2rtcPath, []byte(go2yaml), 0o644); err != nil {
		t.Fatal(err)
	}

	srv := env.serve()
	resp, err := http.Get(srv.URL + "/api/cameras")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var body struct {
		Cameras []string `json:"cameras"`
	}
	decodeJSON(t, resp, &body)
	if len(body.Cameras) != 2 || body.Cameras[0] != "cam2" || body.Cameras[1] != "cam1" {
		t.Errorf("cameras = %v, want ordered [cam2 cam1]", body.Cameras)
	}
}

func TestHandleCamerasMissingConfig(t *testing.T) {
	env := newTestEnv(t)
	if err := os.Remove(env.go2rtcPath); err != nil {
		t.Fatal(err)
	}

	srv := env.serve()
	resp, err := http.Get(srv.URL + "/api/cameras")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// A missing go2rtc config is reported inline, not as an HTTP error.
	var body struct {
		Cameras []string `json:"cameras"`
		Error   string   `json:"error"`
	}
	decodeJSON(t, resp, &body)
	if body.Error == "" {
		t.Error("expected inline error")
	}
}

func TestHandleGo2RTCCamerasGet(t *testing.T) {
	env := newTestEnv(t)
	env.api.config.CameraLimits["cam1"] = 123
	go2yaml := "streams:\n  cam1: rtsp://admin:secret@192.168.1.10:554/stream0\n  cam2: dvrip://admin:secret@192.168.1.11?channel=3&subtype=0\n"
	if err := os.WriteFile(env.go2rtcPath, []byte(go2yaml), 0o644); err != nil {
		t.Fatal(err)
	}

	srv := env.serve()
	resp, err := http.Get(srv.URL + "/api/go2rtc/cameras")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var cams []struct {
		Name    string `json:"name"`
		Type    string `json:"type"`
		IP      string `json:"ip"`
		LimitGB int    `json:"limit_gb"`
		Channel string `json:"channel"`
	}
	decodeJSON(t, resp, &cams)
	if len(cams) != 2 {
		t.Fatalf("cameras = %+v", cams)
	}

	if cams[0].Type != "rtsp" || cams[0].IP != "192.168.1.10" || cams[0].LimitGB != 123 {
		t.Errorf("cam1 = %+v", cams[0])
	}
	if cams[1].Type != "dvrip" || cams[1].IP != "192.168.1.11" || cams[1].Channel != "3" {
		t.Errorf("cam2 = %+v", cams[1])
	}
}

func TestHandleGo2RTCCamerasAddAndDelete(t *testing.T) {
	env := newTestEnv(t)
	srv := env.serve()

	// Adding requires admin.
	body := `{"name":"front","type":"rtsp","user":"u","pass":"p","ip":"10.1.1.1","port":"8554"}`
	resp, err := http.Post(srv.URL+"/api/go2rtc/cameras", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("anon add status = %d, want 403", resp.StatusCode)
	}

	// Admin adds successfully.
	req := adminRequest(http.MethodPost, srv.URL+"/api/go2rtc/cameras", body)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("admin add status = %d", resp.StatusCode)
	}

	// The stream URL keeps the historic "user:pass@" format.
	go2cfg, err := config.LoadGo2RTCConfig(env.go2rtcPath)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := go2cfg.Streams["front"].(string)
	if got != "rtsp://u:p@10.1.1.1:8554/stream0" {
		t.Errorf("stream url = %q", got)
	}
	if len(go2cfg.StreamOrder) != 1 || go2cfg.StreamOrder[0] != "front" {
		t.Errorf("order = %v", go2cfg.StreamOrder)
	}
	if env.api.config.CameraLimits["front"] != env.api.config.DefaultCameraLimitGB {
		t.Errorf("limit not defaulted: %v", env.api.config.CameraLimits)
	}
	if len(env.rec.started) != 1 || env.rec.started[0] != "front" {
		t.Errorf("recorder start calls = %v", env.rec.started)
	}

	// Unknown type is rejected.
	req = adminRequest(http.MethodPost, srv.URL+"/api/go2rtc/cameras", `{"name":"x","type":"gopher","ip":"1.2.3.4"}`)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("bad type status = %d, want 400", resp.StatusCode)
	}

	// Missing name/ip.
	req = adminRequest(http.MethodPost, srv.URL+"/api/go2rtc/cameras", `{"name":"","type":"rtsp"}`)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("missing fields status = %d, want 400", resp.StatusCode)
	}

	// Admin deletes.
	req = adminRequest(http.MethodDelete, srv.URL+"/api/go2rtc/cameras?name=front", "")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("delete status = %d", resp.StatusCode)
	}

	go2cfg, err = config.LoadGo2RTCConfig(env.go2rtcPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := go2cfg.Streams["front"]; ok {
		t.Error("stream should be deleted")
	}
	if len(env.rec.stopped) != 1 || env.rec.stopped[0] != "front" {
		t.Errorf("recorder stop calls = %v", env.rec.stopped)
	}

	// Deleting an unknown camera is a 404.
	req = adminRequest(http.MethodDelete, srv.URL+"/api/go2rtc/cameras?name=ghost", "")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("ghost delete status = %d, want 404", resp.StatusCode)
	}

	// Empty name.
	req = adminRequest(http.MethodDelete, srv.URL+"/api/go2rtc/cameras", "")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("empty delete status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleGo2RTCCamerasMethodNotAllowed(t *testing.T) {
	env := newTestEnv(t)
	srv := env.serve()

	req := adminRequest(http.MethodPut, srv.URL+"/api/go2rtc/cameras", "")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", resp.StatusCode)
	}
}

func TestHandleGo2RTCReorder(t *testing.T) {
	env := newTestEnv(t)
	go2yaml := "streams:\n  a: rtsp://u:p@10.0.0.1/stream0\n  b: rtsp://u:p@10.0.0.2/stream0\n  c: rtsp://u:p@10.0.0.3/stream0\n"
	if err := os.WriteFile(env.go2rtcPath, []byte(go2yaml), 0o644); err != nil {
		t.Fatal(err)
	}

	srv := env.serve()

	req := adminRequest(http.MethodPost, srv.URL+"/api/go2rtc/reorder", `{"from":0,"to":2}`)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("reorder status = %d", resp.StatusCode)
	}

	go2cfg, err := config.LoadGo2RTCConfig(env.go2rtcPath)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"b", "c", "a"}
	if strings.Join(go2cfg.StreamOrder, ",") != strings.Join(want, ",") {
		t.Errorf("order = %v, want %v", go2cfg.StreamOrder, want)
	}

	// Out of range positions.
	req = adminRequest(http.MethodPost, srv.URL+"/api/go2rtc/reorder", `{"from":0,"to":99}`)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("bad position status = %d, want 400", resp.StatusCode)
	}

	// Negative position.
	req = adminRequest(http.MethodPost, srv.URL+"/api/go2rtc/reorder", `{"from":-1,"to":0}`)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("negative position status = %d, want 400", resp.StatusCode)
	}
}

func TestCameraInfoOf(t *testing.T) {
	tests := []struct {
		name    string
		stream  any
		wantIP  string
		wantTyp string
	}{
		{"rtsp with creds", "rtsp://u:p@10.0.0.9:554/stream0", "10.0.0.9", "rtsp"},
		{"no creds no ip", "rtsp://10.0.0.9/stream0", "", "rtsp"},
		{"dvrip", "dvrip://u:p@10.0.0.8?channel=2&subtype=0", "10.0.0.8", "dvrip"},
		{"onvif", "onvif://u:p@10.0.0.7", "10.0.0.7", "onvif"},
		{"isapi", "isapi://u:p@10.0.0.6", "10.0.0.6", "isapi"},
		{"unknown", "gopher://x", "", "unknown"},
		{"nil stream", nil, "", "unknown"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cameraInfoOf("cam", tt.stream, 10)
			if got.IP != tt.wantIP {
				t.Errorf("ip = %q, want %q", got.IP, tt.wantIP)
			}
			if got.Type != tt.wantTyp {
				t.Errorf("type = %q, want %q", got.Type, tt.wantTyp)
			}
		})
	}
}

func TestHandleStatusOrdersProcesses(t *testing.T) {
	env := newTestEnv(t)
	go2yaml := "streams:\n  b: rtsp://u:p@10.0.0.2/stream0\n  a: rtsp://u:p@10.0.0.1/stream0\n"
	if err := os.WriteFile(env.go2rtcPath, []byte(go2yaml), 0o644); err != nil {
		t.Fatal(err)
	}

	// Feed the recorder status through the real recorder type so the
	// type assertion in HandleStatus matches production wiring.
	env.api.recorder = newStatusRecorder([]string{"a", "b"})

	srv := env.serve()
	resp, err := http.Get(srv.URL + "/api/status")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var body struct {
		TotalStreams int `json:"totalStreams"`
		Processes    []struct {
			Name string `json:"name"`
		} `json:"processes"`
	}
	decodeJSON(t, resp, &body)
	if body.TotalStreams != 2 {
		t.Errorf("totalStreams = %d", body.TotalStreams)
	}
	if len(body.Processes) != 2 || body.Processes[0].Name != "b" || body.Processes[1].Name != "a" {
		t.Errorf("processes = %+v, want [b a]", body.Processes)
	}
}

func TestHandleRecordStartStop(t *testing.T) {
	env := newTestEnv(t)
	srv := env.serve()

	// Admin starts.
	req := adminRequest(http.MethodPost, srv.URL+"/api/record/start", "")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if !env.rec.IsRecording() {
		t.Error("recorder should be recording")
	}

	// Second start reports "already recording".
	req = adminRequest(http.MethodPost, srv.URL+"/api/record/start", "")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]string
	decodeJSON(t, resp, &body)
	if body["status"] != "already recording" {
		t.Errorf("status = %q", body["status"])
	}

	// GET is rejected.
	resp, err = http.Get(srv.URL + "/api/record/start")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("get status = %d, want 405", resp.StatusCode)
	}

	// Anonymous stop is rejected.
	resp, err = http.Post(srv.URL+"/api/record/stop", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("anon stop status = %d, want 403", resp.StatusCode)
	}

	// Admin stops.
	req = adminRequest(http.MethodPost, srv.URL+"/api/record/stop", "")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if env.rec.IsRecording() {
		t.Error("recorder should be stopped")
	}
}

func TestHandleGetAndSaveConfig(t *testing.T) {
	env := newTestEnv(t)
	srv := env.serve()

	// Save as admin.
	env.api.config.BaseDir = "/tmp/base-before"
	payload, err := json.Marshal(env.api.config)
	if err != nil {
		t.Fatal(err)
	}
	var parsed map[string]any
	if err := json.Unmarshal(payload, &parsed); err != nil {
		t.Fatal(err)
	}
	parsed["base_dir"] = "/tmp/base-after"

	body, err := json.Marshal(parsed)
	if err != nil {
		t.Fatal(err)
	}
	req := adminRequest(http.MethodPost, srv.URL+"/api/config", string(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("save status = %d", resp.StatusCode)
	}
	if env.api.config.BaseDir != "/tmp/base-after" {
		t.Errorf("BaseDir = %q", env.api.config.BaseDir)
	}

	// Config was persisted.
	saved, err := config.LoadNVRConfig(env.configPath)
	if err != nil {
		t.Fatal(err)
	}
	if saved.BaseDir != "/tmp/base-after" {
		t.Errorf("saved BaseDir = %q", saved.BaseDir)
	}

	// Anonymous save is rejected.
	req = adminRequest(http.MethodPost, srv.URL+"/api/config", "{}")
	req.Header.Del("X-Test-Role")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("anon save status = %d, want 403", resp.StatusCode)
	}

	// Malformed JSON.
	req = adminRequest(http.MethodPost, srv.URL+"/api/config", "{nope")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("bad json status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleCamerasStorage(t *testing.T) {
	env := newTestEnv(t)
	env.writeMP4("cam1", "2024/01/15", "080000.mp4", 128)

	srv := env.serve()
	resp, err := http.Get(srv.URL + "/api/storage/cameras")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var body map[string]map[string]any
	decodeJSON(t, resp, &body)
	if _, ok := body["cam1"]; !ok {
		t.Errorf("storage = %v", body)
	}
}

// statusRecorder reports processes in the shape HandleStatus expects from
// the real recorder, so the ordering logic is exercised end to end.
type statusRecorder struct {
	fakeRecorder
	names []string
}

func newStatusRecorder(names []string) *statusRecorder {
	return &statusRecorder{names: names}
}

func (s *statusRecorder) GetStatus() map[string]any {
	procs := make([]*recorder.StreamInfo, 0, len(s.names))
	for _, n := range s.names {
		procs = append(procs, &recorder.StreamInfo{Name: n})
	}
	return map[string]any{"recording": false, "processes": procs}
}

func TestMain(m *testing.M) {
	// go2rtc handlers read and write /usr/bin and /etc/systemd; tests avoid
	// touching those paths, and the GitHub lookup is stubbed per test.
	githubLatestRelease = func() (string, string, bool) { return "", "", false }
	os.Exit(m.Run())
}
