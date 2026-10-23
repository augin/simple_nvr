package api

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"simple_nvr/internal/auth"
)

func TestSafeJoinRejectsTraversal(t *testing.T) {
	base := "/var/lib/simple-nvr"

	tests := []struct {
		name  string
		elems []string
	}{
		{"dotdot", []string{".."}},
		{"dotdot camera", []string{"..", "2024", "01", "15", "x.mp4"}},
		{"embedded dotdot", []string{"cam/../../etc", "passwd"}},
		{"absolute", []string{"/etc/passwd"}},
		{"middle dotdot", []string{"cam", "2024/../..", "x.mp4"}},
		{"trailing dotdot", []string{"cam", "2024", "01", "15", ".."}},
		{"empty segment", []string{"cam//x.mp4"}},
		{"dot segment", []string{"cam/./x.mp4"}},
		{"null byte", []string{"cam\x00/x.mp4"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, err := safeJoin(base, tt.elems...); err == nil {
				t.Errorf("safeJoin(%q) = %q, want error", tt.elems, got)
			}
		})
	}
}

func TestSafeJoinAcceptsNormalPaths(t *testing.T) {
	base := "/var/lib/simple-nvr"

	tests := []struct {
		elems []string
		want  string
	}{
		{[]string{"cam1"}, filepath.Join(base, "cam1")},
		{[]string{"cam1", "2024/01/15", "file.mp4"}, filepath.Join(base, "cam1", "2024", "01", "15", "file.mp4")},
		{[]string{"cam1", "2024", "01", "15", "080000.mp4"}, filepath.Join(base, "cam1", "2024", "01", "15", "080000.mp4")},
		{[]string{"Front Door"}, filepath.Join(base, "Front Door")},
	}
	for _, tt := range tests {
		got, err := safeJoin(base, tt.elems...)
		if err != nil {
			t.Errorf("safeJoin(%q): %v", tt.elems, err)
			continue
		}
		if got != tt.want {
			t.Errorf("safeJoin(%q) = %q, want %q", tt.elems, got, tt.want)
		}
	}
}

func TestHHMMSS(t *testing.T) {
	tests := []struct {
		in   float64
		want string
	}{
		{0, "000000"},
		{1, "000001"},
		{59.9, "000059"},
		{60, "000100"},
		{3599, "005959"},
		{3600, "010000"},
		{3661, "010101"},
		{86399, "235959"},
	}
	for _, tt := range tests {
		if got := hhmmss(tt.in); got != tt.want {
			t.Errorf("hhmmss(%v) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestPathUnderBase(t *testing.T) {
	base := t.TempDir()

	inside := filepath.Join(base, "cam", "a.mp4")
	if !pathUnderBase(base, inside) {
		t.Errorf("pathUnderBase should accept %q", inside)
	}

	// Sibling directory sharing the prefix must be rejected.
	sibling := base + "evil" + string(filepath.Separator) + "a.mp4"
	if pathUnderBase(base, sibling) {
		t.Errorf("pathUnderBase should reject sibling %q", sibling)
	}

	if pathUnderBase(base, filepath.Join(base, "..", "escape.mp4")) {
		t.Error("pathUnderBase should reject parent traversal")
	}

	if pathUnderBase(base, filepath.Join(base, "cam", "..", "..", "escape.mp4")) {
		t.Error("pathUnderBase should reject nested traversal")
	}
}

func TestRequireAdminRole(t *testing.T) {
	admin := auth.WithIdentity(httptest.NewRequest(http.MethodGet, "/", nil), "admin", auth.RoleAdmin)
	if !requireAdminRole(admin) {
		t.Error("admin request should pass requireAdminRole")
	}

	plain := auth.WithIdentity(httptest.NewRequest(http.MethodGet, "/", nil), "bob", auth.RoleUser)
	if requireAdminRole(plain) {
		t.Error("user request should not pass requireAdminRole")
	}

	anon := httptest.NewRequest(http.MethodGet, "/", nil)
	if requireAdminRole(anon) {
		t.Error("anonymous request should not pass requireAdminRole")
	}
}

func TestWriteJSONAndError(t *testing.T) {
	rec := httptest.NewRecorder()
	writeJSON(rec, http.StatusOK, map[string]string{"a": "b"})
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("content type = %q", ct)
	}
	if rec.Code != http.StatusOK {
		t.Errorf("code = %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"a":"b"`) {
		t.Errorf("body = %s", rec.Body.String())
	}

	rec = httptest.NewRecorder()
	writeError(rec, http.StatusBadRequest, "boom")
	if rec.Code != http.StatusBadRequest {
		t.Errorf("code = %d", rec.Code)
	}
	if got := strings.TrimSpace(rec.Body.String()); got != `{"error":"boom"}` {
		t.Errorf("body = %q", got)
	}
}

func TestApplyConfig(t *testing.T) {
	dst := newTestEnv(t).api.config

	src := *dst
	src.BaseDir = "/new/base"
	src.ArchiveDir = "/new/archive"
	src.StreamServer = "rtsp://10.0.0.1:8554"
	src.DefaultCameraLimitGB = 99
	src.GlobalSizeGB = 500
	src.HTTPPort = 0 // must not clobber
	src.AlarmPort = 0
	src.MQTTPort = 0
	src.KioskPort = 0

	applyConfig(&src, dst)

	if dst.BaseDir != "/new/base" {
		t.Errorf("BaseDir = %q", dst.BaseDir)
	}
	if dst.DefaultCameraLimitGB != 99 {
		t.Errorf("DefaultCameraLimitGB = %d", dst.DefaultCameraLimitGB)
	}
	if dst.HTTPPort == 0 {
		t.Error("HTTPPort must survive a zero value in the request")
	}
	if dst.AlarmPort == 0 {
		t.Error("AlarmPort must survive a zero value in the request")
	}
	if dst.KioskPort == 0 {
		t.Error("KioskPort must survive a zero value in the request")
	}
}

func TestHandleVersion(t *testing.T) {
	env := newTestEnv(t)
	srv := env.serve()

	resp, err := http.Get(srv.URL + "/api/version")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var body map[string]string
	decodeJSON(t, resp, &body)
	if body["version"] != "test" {
		t.Errorf("version = %q", body["version"])
	}
}
