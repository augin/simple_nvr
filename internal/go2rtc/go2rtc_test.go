package go2rtc

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAPIBase(t *testing.T) {
	cases := map[string]string{
		"rtsp://127.0.0.1:8554":   "http://127.0.0.1:1984",
		"rtsp://192.168.1.5:8554": "http://192.168.1.5:1984",
		"":                        "http://localhost:1984",
		"not a url":               "http://localhost:1984",
		"rtsp://cam.local:8554":   "http://cam.local:1984",
	}
	for in, want := range cases {
		if got := APIBase(in); got != want {
			t.Errorf("APIBase(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestClientRestart(t *testing.T) {
	var gotPath, gotMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotMethod = r.URL.Path, r.Method
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := &Client{BaseURL: srv.URL, HTTP: srv.Client()}
	if err := c.Restart(); err != nil {
		t.Fatalf("Restart: %v", err)
	}
	if gotPath != "/api/restart" || gotMethod != http.MethodPost {
		t.Errorf("got %s %s", gotMethod, gotPath)
	}
}

func TestClientRestartFailures(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	if err := (&Client{BaseURL: srv.URL, HTTP: srv.Client()}).Restart(); err == nil {
		t.Error("expected error for non-200 response")
	}

	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	dead.Close()
	if err := (&Client{BaseURL: dead.URL, HTTP: dead.Client()}).Restart(); err == nil {
		t.Error("expected error for connection failure")
	}
}

func TestClientInfo(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"version":"1.9.0","rtsp":{"listen":":8554"}}`))
	}))
	defer srv.Close()

	info, err := (&Client{BaseURL: srv.URL, HTTP: srv.Client()}).Info()
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info["version"] != "1.9.0" {
		t.Errorf("version = %v", info["version"])
	}
	rtsp, ok := info["rtsp"].(map[string]any)
	if !ok || rtsp["listen"] != ":8554" {
		t.Errorf("rtsp = %v", info["rtsp"])
	}

	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusNotFound)
	}))
	defer bad.Close()
	if _, err := (&Client{BaseURL: bad.URL, HTTP: bad.Client()}).Info(); err == nil {
		t.Error("expected error for 404")
	}
}

func TestClientDefaultHTTP(t *testing.T) {
	// Client without an explicit HTTP client must still build requests.
	c := &Client{BaseURL: "http://127.0.0.1:1"} // nothing listens here
	if err := c.Restart(); err == nil {
		t.Error("expected connection error")
	}
	if c.httpClient() == nil {
		t.Error("httpClient returned nil")
	}
}

func TestCameraType(t *testing.T) {
	cases := map[string]string{
		"rtsp://admin:pass@1.2.3.4:554/stream0": TypeRTSP,
		"dvrip://admin:pass@1.2.3.4?channel=0":  TypeDVRIP,
		"onvif://admin:pass@1.2.3.4":            TypeONVIF,
		"isapi://admin:pass@1.2.3.4":            TypeISAPI,
		"http://example.com/feed":               TypeUnknown,
		"":                                      TypeUnknown,
	}
	for in, want := range cases {
		if got := CameraType(in); got != want {
			t.Errorf("CameraType(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestCameraIP(t *testing.T) {
	cases := map[string]string{
		"rtsp://admin:pass@192.168.1.10:554/stream0": "192.168.1.10",
		"dvrip://admin:pass@192.168.1.11?channel=0":  "192.168.1.11",
		"rtsp://camera.local/stream0":                "camera.local",
		"rtsp://10.0.0.1:554/stream0":                "10.0.0.1",
		"":                                           "",
		"://broken":                                  "",
	}
	for in, want := range cases {
		if got := CameraIP(in); got != want {
			t.Errorf("CameraIP(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestCameraChannel(t *testing.T) {
	if got := CameraChannel("dvrip://u:p@1.2.3.4?channel=3&subtype=0"); got != "3" {
		t.Errorf("channel = %q, want 3", got)
	}
	if got := CameraChannel("dvrip://u:p@1.2.3.4"); got != "" {
		t.Errorf("missing channel = %q, want empty", got)
	}
	if got := CameraChannel("rtsp://u:p@1.2.3.4/stream0?channel=9"); got != "" {
		t.Errorf("non-dvrip channel = %q, want empty", got)
	}
}

func TestBuildURL(t *testing.T) {
	cases := []struct {
		cameraType, user, pass, ip, port, channel string
		want                                      string
	}{
		{TypeRTSP, "admin", "pw", "1.2.3.4", "", "", "rtsp://admin:pw@1.2.3.4:554/stream0"},
		{TypeRTSP, "admin", "pw", "1.2.3.4", "8554", "", "rtsp://admin:pw@1.2.3.4:8554/stream0"},
		{TypeRTSP, "", "", "1.2.3.4", "", "", "rtsp://:@1.2.3.4:554/stream0"},
		{TypeDVRIP, "admin", "pw", "1.2.3.4", "", "", "dvrip://admin:pw@1.2.3.4?channel=0&subtype=0"},
		{TypeDVRIP, "admin", "pw", "1.2.3.4", "", "2", "dvrip://admin:pw@1.2.3.4?channel=2&subtype=0"},
		{TypeONVIF, "admin", "pw", "1.2.3.4", "", "", "onvif://admin:pw@1.2.3.4"},
		{TypeISAPI, "admin", "pw", "1.2.3.4", "", "", "isapi://admin:pw@1.2.3.4"},
	}
	for _, tc := range cases {
		got, err := BuildURL(tc.cameraType, tc.user, tc.pass, tc.ip, tc.port, tc.channel)
		if err != nil {
			t.Errorf("BuildURL(%s): %v", tc.cameraType, err)
			continue
		}
		if got != tc.want {
			t.Errorf("BuildURL(%s) = %q, want %q", tc.cameraType, got, tc.want)
		}
	}

	if _, err := BuildURL("http", "u", "p", "1.2.3.4", "", ""); err == nil {
		t.Error("expected error for unsupported type")
	}
}
