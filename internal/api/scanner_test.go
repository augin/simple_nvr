package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"simple_nvr/internal/auth"
)

func TestCollectMP4s(t *testing.T) {
	base := t.TempDir()
	env := &testEnv{t: t, baseDir: base}
	env.writeMP4("front", "2024/05", "a.mp4", 3)
	env.writeMP4("front", "2024/06", "b.MP4", 2) // заглавное расширение не матчится
	env.writeMP4("back", "", "c.mp4", 4)
	if err := os.WriteFile(filepath.Join(base, "notes.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "front", "2024", "05", "thumb.jpg"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	files := collectMP4s(base)
	if len(files) != 2 {
		t.Fatalf("files = %v", files)
	}
	byName := map[string]scanFile{}
	for _, f := range files {
		byName[f.name] = f
	}
	if f := byName["a.mp4"]; f.camera != "front" || f.size != 3 || f.date != "2024/05" {
		t.Errorf("a.mp4 = %+v", f)
	}
	if f := byName["c.mp4"]; f.camera != "back" || f.date != "" {
		t.Errorf("c.mp4 = %+v", f)
	}

	// Несуществующая база не паникует.
	if got := collectMP4s(filepath.Join(base, "nope")); got != nil {
		t.Errorf("missing base = %v", got)
	}
}

func TestScanStateLifecycle(t *testing.T) {
	var s scanState
	if !s.start() {
		t.Fatal("first start should succeed")
	}
	if s.start() {
		t.Fatal("second start should fail while running")
	}
	s.setTotal(5)
	s.setCurrent("front/2024/05/a.mp4")
	s.record(scanResult{camera: "front", date: "2024/05", name: "a.mp4", path: "/p/a.mp4", size: 1, broken: true, err: "boom"})
	// Повторная очередь того же файла не задваивает битый.
	s.record(scanResult{camera: "front", date: "2024/05", name: "a.mp4", path: "/p/a.mp4", size: 1, broken: true, err: "boom"})
	s.record(scanResult{camera: "front", date: "2024/05", name: "b.mp4", path: "/p/b.mp4", size: 2})

	snap := s.snapshot()
	if snap["running"] != true || snap["total"] != 5 || snap["checked"] != 3 || snap["broken"] != 1 {
		t.Fatalf("snapshot = %v", snap)
	}
	if got, _ := s.counters(); got != 3 {
		t.Errorf("checked = %d", got)
	}

	s.finish("")
	snap = s.snapshot()
	if snap["running"] != false || snap["current"] != "" {
		t.Errorf("after finish = %v", snap)
	}
	results, _ := snap["results"].([]brokenFile)
	if len(results) != 1 || results[0].Error != "boom" || results[0].File != "a.mp4" {
		t.Errorf("results = %v", results)
	}

	// Ошибка финиша попадает в снимок только после завершения.
	s.start()
	s.finish("disk gone")
	if got := s.snapshot()["error"]; got != "disk gone" {
		t.Errorf("error = %v", got)
	}
}

func TestRunScanFindsBroken(t *testing.T) {
	if _, err := exec.LookPath("ffprobe"); err != nil {
		t.Skip("ffprobe not available")
	}
	env := newTestEnv(t)
	env.writeMP4("front", "2024/05", "good.mp4", 3)
	env.writeMP4("front", "2024/05", "broken.mp4", 3)

	if !env.api.toolsScan.start() {
		t.Fatal("scan should start")
	}
	env.api.runScan()

	snap := env.api.toolsScan.snapshot()
	if snap["running"] != false {
		t.Fatalf("scan still running: %v", snap)
	}
	if got := snap["checked"].(int); got != 2 {
		t.Errorf("checked = %v", got)
	}
	results, _ := snap["results"].([]brokenFile)
	// writeMP4 пишет нулевые байта — оба файла битые для ffprobe.
	if len(results) != 2 {
		t.Fatalf("results = %v", results)
	}
}

func TestRunScanSkipsActiveRecording(t *testing.T) {
	env := newTestEnv(t)
	activePath := env.writeMP4("front", "2024/05", "live.mp4", 3)
	env.rec.activePaths[activePath] = true

	if !env.api.toolsScan.start() {
		t.Fatal("scan should start")
	}
	env.api.runScan()

	snap := env.api.toolsScan.snapshot()
	if got := snap["broken"].(int); got != 0 {
		t.Errorf("active file should be skipped, broken = %v", got)
	}
	if got := snap["checked"].(int); got != 1 {
		t.Errorf("checked = %v", got)
	}
}

func TestRunScanMissingBaseDir(t *testing.T) {
	env := newTestEnv(t)
	if err := os.RemoveAll(env.baseDir); err != nil {
		t.Fatal(err)
	}
	if !env.api.toolsScan.start() {
		t.Fatal("scan should start")
	}
	env.api.runScan()
	if err := env.api.toolsScan.snapshot()["error"].(string); err == "" {
		t.Error("expected error for missing base dir")
	}
}

func TestHandleToolsScanEndpoints(t *testing.T) {
	if _, err := exec.LookPath("ffprobe"); err != nil {
		t.Skip("ffprobe not available")
	}
	env := newTestEnv(t)
	env.writeMP4("front", "2024/05", "broken.mp4", 3)
	srv := env.serve()

	// Запуск сканирования.
	resp, err := http.DefaultClient.Do(adminRequest(http.MethodPost, srv.URL+"/api/tools/scan", ""))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("scan start = %d", resp.StatusCode)
	}

	// Пока скан крутится, повторный запуск просто сообщает статус.
	resp2, err := http.DefaultClient.Do(adminRequest(http.MethodGet, srv.URL+"/api/tools/scan/status", ""))
	if err != nil {
		t.Fatal(err)
	}
	var status map[string]any
	decodeJSON(t, resp2, &status)
	if _, ok := status["running"]; !ok {
		t.Fatalf("status = %v", status)
	}

	// Ждём завершения.
	deadline := time.Now().Add(30 * time.Second)
	for {
		r, err := http.DefaultClient.Do(adminRequest(http.MethodGet, srv.URL+"/api/tools/scan/status", ""))
		if err != nil {
			t.Fatal(err)
		}
		var st map[string]any
		decodeJSON(t, r, &st)
		if st["running"] == false {
			results, _ := st["results"].([]any)
			if len(results) != 1 {
				t.Fatalf("results = %v", st)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("scan did not finish")
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Обычный пользователь не может запускать скан.
	resp4, err := http.DefaultClient.Do(userRequest(http.MethodPost, srv.URL+"/api/tools/scan", ""))
	if err != nil {
		t.Fatal(err)
	}
	resp4.Body.Close()
	if resp4.StatusCode != http.StatusForbidden {
		t.Errorf("user scan start = %d, want 403", resp4.StatusCode)
	}
}

func TestHandleToolsRepairValidation(t *testing.T) {
	env := newTestEnv(t)
	path := env.writeMP4("front", "2024/05", "clip.mp4", 3)
	srv := env.serve()

	cases := []struct {
		name string
		body string
		want int
	}{
		{"missing path", `{}`, http.StatusBadRequest},
		{"bad json", `not json`, http.StatusBadRequest},
		{"outside base", `{"path":"/etc/passwd"}`, http.StatusBadRequest},
		{"escape", `{"path":"` + filepath.Join(env.baseDir, "..", "escape.mp4") + `"}`, http.StatusBadRequest},
		{"not found", `{"path":"` + filepath.Join(env.baseDir, "front", "nope.mp4") + `"}`, http.StatusNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := http.DefaultClient.Do(adminRequest(http.MethodPost, srv.URL+"/api/tools/repair", tc.body))
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != tc.want {
				t.Errorf("status = %d, want %d", resp.StatusCode, tc.want)
			}
		})
	}

	// Свежий файл пропускается (ещё пишется).
	resp, err := http.DefaultClient.Do(adminRequest(http.MethodPost, srv.URL+"/api/tools/repair", `{"path":"`+path+`"}`))
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	decodeJSON(t, resp, &body)
	if body["status"] != "skipped" {
		t.Errorf("fresh file = %v", body)
	}

	// Обычный пользователь не имеет доступа.
	resp2, err := http.DefaultClient.Do(userRequest(http.MethodPost, srv.URL+"/api/tools/repair", `{"path":"`+path+`"}`))
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusForbidden {
		t.Errorf("user repair = %d, want 403", resp2.StatusCode)
	}
}

// TestPathUnderBaseSymlink проверяет, что симлинк наружу из-под base
// отклоняется.
func TestPathUnderBaseSymlink(t *testing.T) {
	base := t.TempDir()
	sibling := base + "evil"
	if err := os.MkdirAll(sibling, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(sibling) })

	outside := filepath.Join(sibling, "out.mp4")
	if err := os.WriteFile(outside, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(base, "link.mp4")
	if err := os.Symlink(outside, link); err != nil {
		t.Skip("symlinks unavailable")
	}
	if pathUnderBase(base, link) {
		t.Error("symlink escaping base should fail")
	}

	// Симлинк внутри base остаётся допустимым.
	inside := filepath.Join(base, "real.mp4")
	if err := os.WriteFile(inside, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	inner := filepath.Join(base, "alias.mp4")
	if err := os.Symlink(inside, inner); err != nil {
		t.Fatal(err)
	}
	if !pathUnderBase(base, inner) {
		t.Error("symlink inside base should pass")
	}
}

// handleToolsScanStatusWhileRunning проверяет, что статус отдаётся, пока
// скан активен.
func TestHandleToolsScanStatusWhileRunning(t *testing.T) {
	env := newTestEnv(t)
	env.api.toolsScan.start()
	defer env.api.toolsScan.finish("")

	req := httptest.NewRequest(http.MethodGet, "/api/tools/scan/status", nil)
	req = auth.WithIdentity(req, "admin", auth.RoleAdmin)
	rec := httptest.NewRecorder()
	env.api.HandleToolsScanStatus(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["running"] != true {
		t.Errorf("running = %v", body["running"])
	}
}

// requireAdminRole в контексте — только роль из context.
func TestHandleToolsScanStatusForbidden(t *testing.T) {
	env := newTestEnv(t)
	req := httptest.NewRequest(http.MethodGet, "/api/tools/scan/status", nil)
	req = auth.WithIdentity(req, "bob", auth.RoleUser)
	rec := httptest.NewRecorder()
	env.api.HandleToolsScanStatus(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Errorf("user status = %d, want 403", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "error") {
		t.Errorf("body = %s", rec.Body.String())
	}
}
