package api

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHandleFilesListsRecordings(t *testing.T) {
	env := newTestEnv(t)
	env.writeMP4("cam1", "2024/01/15", "080000.mp4", 10)
	env.writeMP4("cam1", "2024/01/15", "081000.mp4", 10)
	env.writeMP4("cam1", "2024/01/16", "090000.mp4", 10)
	// Не-MP4 и каталоги игнорируются.
	env.writeMP4("cam1", "2024/01/15", "notes.txt", 5)

	srv := env.serve()
	resp, err := http.Get(srv.URL + "/api/files?camera=cam1")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var body map[string][]string
	decodeJSON(t, resp, &body)

	if len(body) != 2 {
		t.Fatalf("folders = %v", body)
	}
	files := body["2024/01/15"]
	if len(files) != 2 || files[0] != "080000.mp4" || files[1] != "081000.mp4" {
		t.Errorf("files = %v, want sorted [080000 081000]", files)
	}
}

func TestHandleFilesMissingCamera(t *testing.T) {
	env := newTestEnv(t)
	srv := env.serve()

	// Нет параметра — 400.
	resp, err := http.Get(srv.URL + "/api/files")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d", resp.StatusCode)
	}

	// Неизвестная камера — пустой объект, а не ошибка.
	resp, err = http.Get(srv.URL + "/api/files?camera=nosuch")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d", resp.StatusCode)
	}
	var body map[string]any
	decodeJSON(t, resp, &body)
	if len(body) != 0 {
		t.Errorf("body = %v", body)
	}
}

func TestHandleFilesRejectsTraversal(t *testing.T) {
	env := newTestEnv(t)
	// Секрет вне baseDir.
	secret := filepath.Join(filepath.Dir(env.baseDir), "secret.json")
	if err := os.WriteFile(secret, []byte("top"), 0o644); err != nil {
		t.Fatal(err)
	}

	srv := env.serve()
	resp, err := http.Get(srv.URL + "/api/files?camera=../")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleVideoServesFile(t *testing.T) {
	env := newTestEnv(t)
	path := env.writeMP4("cam1", "2024/01/15", "080000.mp4", 64)

	srv := env.serve()
	resp, err := http.Get(srv.URL + "/api/video/cam1/2024/01/15/080000.mp4")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "video/mp4" {
		t.Errorf("content type = %q", ct)
	}
	if resp.ContentLength != 64 {
		_ = path
		t.Errorf("length = %d", resp.ContentLength)
	}
}

func TestHandleVideoNotFoundAndTraversal(t *testing.T) {
	env := newTestEnv(t)
	srv := env.serve()

	resp, err := http.Get(srv.URL + "/api/video/cam1/2024/01/15/missing.mp4")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("missing status = %d", resp.StatusCode)
	}

	// An HTTP client normalises ".." before it reaches the server, so the
	// handler's own defence is exercised with a direct call.
	req := httptest.NewRequest(http.MethodGet, "/api/video/cam1/../../etc/passwd", nil)
	rec := httptest.NewRecorder()
	env.api.HandleVideo(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("traversal status = %d, want 400", rec.Code)
	}
}

func TestHandleArchiveVideoAndDelete(t *testing.T) {
	env := newTestEnv(t)

	archFile := filepath.Join(env.archiveDir, "cam1", "2024/01/15", "clip.mp4")
	if err := os.MkdirAll(filepath.Dir(archFile), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(archFile, make([]byte, 32), 0o644); err != nil {
		t.Fatal(err)
	}

	srv := env.serve()

	// Отдача файла из архива.
	resp, err := http.Get(srv.URL + "/api/archive/video/cam1/2024/01/15/clip.mp4")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("archive video status = %d", resp.StatusCode)
	}

	// Удаление отклоняется без админ-роли.
	resp, err = http.Post(srv.URL+"/api/archive/delete?camera=cam1&folder=2024/01/15&file=clip.mp4", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("anon delete status = %d, want 403", resp.StatusCode)
	}

	// Удаление админом работает.
	req := adminRequest(http.MethodPost, srv.URL+"/api/archive/delete?camera=cam1&folder=2024/01/15&file=clip.mp4", "")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("admin delete status = %d", resp.StatusCode)
	}
	if _, err := os.Stat(archFile); !os.IsNotExist(err) {
		t.Error("archive file should be deleted")
	}

	// Удаление traversal-путей отклоняется.
	req = adminRequest(http.MethodPost, srv.URL+"/api/archive/delete?camera=..&folder=..&file=passwd", "")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("traversal delete status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleArchiveListsAndRejects(t *testing.T) {
	env := newTestEnv(t)

	archFile := filepath.Join(env.archiveDir, "cam1", "2024/02/01", "clip.mp4")
	if err := os.MkdirAll(filepath.Dir(archFile), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(archFile, make([]byte, 8), 0o644); err != nil {
		t.Fatal(err)
	}

	srv := env.serve()

	resp, err := http.Get(srv.URL + "/api/archive?camera=cam1")
	if err != nil {
		t.Fatal(err)
	}
	var body map[string][]string
	decodeJSON(t, resp, &body)
	if len(body["2024/02/01"]) != 1 {
		t.Errorf("archive = %v", body)
	}

	resp, err = http.Get(srv.URL + "/api/archive?camera=../..")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("traversal status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleVideoDownloadValidation(t *testing.T) {
	env := newTestEnv(t)
	srv := env.serve()

	tests := []struct {
		name string
		url  string
		want int
	}{
		{"missing params", "/api/download", http.StatusBadRequest},
		{"bad start", "/api/download?camera=c&folder=f&file=x.mp4&start=abc&end=5", http.StatusBadRequest},
		{"bad end", "/api/download?camera=c&folder=f&file=x.mp4&start=1&end=abc", http.StatusBadRequest},
		{"end before start", "/api/download?camera=c&folder=f&file=x.mp4&start=5&end=1", http.StatusBadRequest},
		{"missing file", "/api/download?camera=c&folder=2024/01/15&file=nope.mp4&start=1&end=2", http.StatusNotFound},
		{"traversal camera", "/api/download?camera=..&folder=..&file=passwd&start=1&end=2", http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := http.Get(srv.URL + tt.url)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tt.want {
				t.Errorf("status = %d, want %d", resp.StatusCode, tt.want)
			}
		})
	}
}

func TestListMP4FilesErrors(t *testing.T) {
	if _, err := listMP4Files(filepath.Join(t.TempDir(), "nope")); err == nil {
		t.Error("expected error for missing dir")
	}

	// Пустая директория — пустая карта без ошибки.
	dir := t.TempDir()
	got, err := listMP4Files(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("got %v", got)
	}
}

func TestRelDate(t *testing.T) {
	base := "/base"
	// relDate extracts the "year/month/day" folder of a recording path; it
	// only feeds the scan status line, so odd input degrades to a prefix.
	if got := relDate(base, "/base/cam/2024/01/15/x.mp4"); got != "2024/01/15" {
		t.Errorf("relDate = %q, want 2024/01/15", got)
	}
	if got := relDate(base, "/base/cam/x.mp4"); got != "" {
		t.Errorf("relDate shallow = %q, want empty", got)
	}
}

func TestPathUnderBaseSymlinkEscape(t *testing.T) {
	base := t.TempDir()
	outside := t.TempDir()
	secret := filepath.Join(outside, "secret.mp4")
	if err := os.WriteFile(secret, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	link := filepath.Join(base, "escape")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlink not supported: %v", err)
	}

	if pathUnderBase(base, filepath.Join(link, "secret.mp4")) {
		t.Error("symlink escaping base must be rejected")
	}

	if !strings.HasPrefix(secret, outside) {
		t.Fatal("test setup")
	}
}
