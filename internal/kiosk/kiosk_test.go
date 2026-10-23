package kiosk

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"simple_nvr/internal/config"
)

// newTestKiosk builds a KioskServer whose asset paths point into a temp dir
// and whose mainAddr targets the given backend.
func newTestKiosk(t *testing.T, backendURL string) *KioskServer {
	t.Helper()
	dir := t.TempDir()
	staticDir := filepath.Join(dir, "static")
	if err := os.MkdirAll(staticDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(staticDir, "app.js"), []byte("// js"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(staticDir, "favicon.ico"), []byte("ico"), 0o644); err != nil {
		t.Fatal(err)
	}
	templatePath := filepath.Join(dir, "index.html")
	if err := os.WriteFile(templatePath, []byte("<html><head><title>{{VERSION}}</title></head><body></body></html>"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := &config.NVRConfig{HTTPPort: 8080}
	ks := NewKioskServer(cfg, "9.9.9")
	ks.mainAddr = backendURL
	ks.staticPath = staticDir
	ks.templatePath = templatePath
	return ks
}

func TestIndexInjectsKioskFlagAndVersion(t *testing.T) {
	ks := newTestKiosk(t, "http://127.0.0.1:1")
	srv := httptest.NewServer(ks.Handler())
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Errorf("content type = %q", ct)
	}
	if !strings.Contains(string(body), "__kioskMode=true") {
		t.Error("kiosk flag not injected")
	}
	if !strings.Contains(string(body), "9.9.9") {
		t.Error("version not substituted")
	}
	if strings.Contains(string(body), "{{VERSION}}") {
		t.Error("placeholder left in page")
	}
}

func TestAuthCheckIsAlwaysAnonymous(t *testing.T) {
	ks := newTestKiosk(t, "http://127.0.0.1:1")
	srv := httptest.NewServer(ks.Handler())
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/api/auth/check")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if string(body) != `{"auth_required":false}` {
		t.Errorf("auth check = %s", body)
	}
}

func TestStaticFileServed(t *testing.T) {
	ks := newTestKiosk(t, "http://127.0.0.1:1")
	srv := httptest.NewServer(ks.Handler())
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/static/app.js")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if string(body) != "// js" {
		t.Errorf("static body = %q", body)
	}
}

func TestFaviconServed(t *testing.T) {
	ks := newTestKiosk(t, "http://127.0.0.1:1")
	srv := httptest.NewServer(ks.Handler())
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/favicon.ico")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if string(body) != "ico" {
		t.Errorf("favicon body = %q", body)
	}
}

func TestOtherPathsProxyToBackend(t *testing.T) {
	var gotPath, gotKioskHeader string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotKioskHeader = r.Header.Get("X-Kiosk-Proxy")
		_, _ = io.WriteString(w, "backend")
	}))
	t.Cleanup(backend.Close)

	ks := newTestKiosk(t, backend.URL)
	srv := httptest.NewServer(ks.Handler())
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/api/cameras")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if gotPath != "/api/cameras" {
		t.Errorf("backend saw path %q", gotPath)
	}
	if gotKioskHeader != "1" {
		t.Errorf("X-Kiosk-Proxy = %q", gotKioskHeader)
	}
	if string(body) != "backend" {
		t.Errorf("proxied body = %q", body)
	}
}

func TestMissingTemplateReturns500(t *testing.T) {
	ks := newTestKiosk(t, "http://127.0.0.1:1")
	ks.templatePath = filepath.Join(t.TempDir(), "nope.html")
	srv := httptest.NewServer(ks.Handler())
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("status = %d", resp.StatusCode)
	}
}

func TestStartDisabledIsNoop(t *testing.T) {
	// KioskEnabled=false — Start не должен открывать порт. Берём свободный
	// порт: если бы сервер поднялся, соединение бы удалось.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()

	cfg := &config.NVRConfig{KioskEnabled: false, KioskPort: port, HTTPPort: 8080}
	ks := NewKioskServer(cfg, "0")
	ks.Start()

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/", port))
	if err == nil {
		resp.Body.Close()
		t.Error("kiosk server should not start when disabled")
	}
}
