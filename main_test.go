package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNextCycleTick(t *testing.T) {
	loc := time.UTC
	cases := []struct {
		name string
		now  time.Time
		want time.Time
	}{
		{"mid-interval", time.Date(2024, 5, 5, 10, 23, 45, 0, loc), time.Date(2024, 5, 5, 10, 30, 0, 0, loc)},
		{"on-10-minute mark", time.Date(2024, 5, 5, 10, 20, 0, 0, loc), time.Date(2024, 5, 5, 10, 30, 0, 0, loc)},
		{"50-minute rolls to next hour", time.Date(2024, 5, 5, 10, 50, 12, 0, loc), time.Date(2024, 5, 5, 11, 0, 0, 0, loc)},
		{"59-minute rolls to next hour", time.Date(2024, 5, 5, 10, 59, 59, 0, loc), time.Date(2024, 5, 5, 11, 0, 0, 0, loc)},
		{"end of day rolls to midnight", time.Date(2024, 5, 5, 23, 50, 1, 0, loc), time.Date(2024, 5, 6, 0, 0, 0, 0, loc)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := nextCycleTick(tc.now)
			if !got.Equal(tc.want) {
				t.Errorf("nextCycleTick(%v) = %v, want %v", tc.now, got, tc.want)
			}
			// Отметка всегда кратна 10 минутам и не раньше now.
			if got.Minute()%10 != 0 || got.Second() != 0 {
				t.Errorf("tick %v is not a clean 10-minute mark", got)
			}
			if got.Before(tc.now) {
				t.Errorf("tick %v is before now %v", got, tc.now)
			}
		})
	}
}

func TestFindStaticDirFallsBackToDot(t *testing.T) {
	// os.Executable в тесте указывает на временный бинарь без templates/,
	// поэтому ожидаем фолбэк на текущую директорию.
	if got := findStaticDir(); got == "" {
		t.Error("findStaticDir returned empty path")
	}
}

func TestFindConfigFallsBack(t *testing.T) {
	if got := findConfig(); got == "" {
		t.Error("findConfig returned empty path")
	}
}

func TestRegisterUIServesIndexAndAssets(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "templates"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "static"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "templates", "index.html"),
		[]byte("<html><body>version {{VERSION}}</body></html>"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "static", "app.js"), []byte("console.log(1)"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "static", "favicon.ico"), []byte("icon"), 0o644); err != nil {
		t.Fatal(err)
	}

	mux := http.NewServeMux()
	registerUI(mux, dir)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	// Корень отдаёт шаблон с подставленной версией.
	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	body := readAll(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/ = %d", resp.StatusCode)
	}
	if !strings.Contains(body, "version "+version) {
		t.Errorf("version not substituted: %q", body)
	}

	// Статика и фавикон доступны.
	for _, path := range []string{"/static/app.js", "/favicon.ico"} {
		r, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		r.Body.Close()
		if r.StatusCode != http.StatusOK {
			t.Errorf("%s = %d", path, r.StatusCode)
		}
	}

	// Посторонние пути получают 404.
	for _, path := range []string{"/nope", "/templates/index.html"} {
		r, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		r.Body.Close()
		if r.StatusCode != http.StatusNotFound {
			t.Errorf("%s = %d, want 404", path, r.StatusCode)
		}
	}
}

func TestRegisterUIMissingTemplate(t *testing.T) {
	mux := http.NewServeMux()
	registerUI(mux, t.TempDir())
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("/ = %d, want 500", resp.StatusCode)
	}
}

func readAll(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
