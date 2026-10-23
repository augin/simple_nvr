package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"simple_nvr/internal/config"
)

// newTestConfig builds a valid config rooted in a temp dir.
func newTestConfig(t *testing.T) *config.NVRConfig {
	t.Helper()
	base := t.TempDir()
	cfg, err := config.ParseNVRConfig([]byte(
		"base_dir: " + base + "\nstream_server: rtsp://127.0.0.1:8554\ngo2rtc_config_path: /tmp/go2rtc.yaml\n"))
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	return cfg
}

// writeFile creates a file with the given content and modification time.
func writeFile(t *testing.T, path string, size int, mod time.Time) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	data := make([]byte, size)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, mod, mod); err != nil {
		t.Fatal(err)
	}
}

func TestCleanDirFreesOldestFilesFirst(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	paths := []string{
		filepath.Join(dir, "old.mp4"),
		filepath.Join(dir, "middle.mp4"),
		filepath.Join(dir, "new.mp4"),
	}
	writeFile(t, paths[0], 100, now.Add(-3*time.Hour))
	writeFile(t, paths[1], 100, now.Add(-2*time.Hour))
	writeFile(t, paths[2], 100, now.Add(-1*time.Hour))

	s := &Storage{config: &config.NVRConfig{BaseDir: t.TempDir()}}
	freed := removeUntilFreed(collectFilesSorted(dir), 150)

	if freed != 200 {
		t.Errorf("freed = %d, want 200", freed)
	}
	if _, err := os.Stat(paths[0]); err == nil {
		t.Error("oldest file must be deleted")
	}
	if _, err := os.Stat(paths[1]); err == nil {
		t.Error("second file must be deleted")
	}
	if _, err := os.Stat(paths[2]); err != nil {
		t.Error("newest file must survive")
	}
	if s.config.BaseDir == "" {
		t.Error("sanity")
	}
}

func TestRemoveUntilFreedStopsWhenNothingLeft(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "only.mp4"), 10, time.Now())

	freed := removeUntilFreed(collectFilesSorted(dir), 1<<40)
	if freed != 10 {
		t.Errorf("freed = %d, want 10", freed)
	}
	if freed := removeUntilFreed(collectFilesSorted(dir), 100); freed != 0 {
		t.Errorf("freed on empty dir = %d, want 0", freed)
	}
}

func TestCleanDirUnderLimitKeepsFiles(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "a.mp4"), 50, time.Now())
	writeFile(t, filepath.Join(dir, "b.mp4"), 50, time.Now())

	s := NewStorage(&config.NVRConfig{BaseDir: t.TempDir()})
	s.cleanDir(dir, 0) // nothing to free
	if entries, err := os.ReadDir(dir); err != nil || len(entries) != 2 {
		t.Errorf("files removed although nothing to free: %v (%d entries)", err, len(entries))
	}
}

func TestCameraLimitBytes(t *testing.T) {
	s := NewStorage(&config.NVRConfig{DefaultCameraLimitGB: 10, CameraLimits: map[string]int{"big": 50, "zero": 0}})

	if got := s.cameraLimitBytes("big"); got != 50*bytesPerGB {
		t.Errorf("camera limit = %d", got)
	}
	if got := s.cameraLimitBytes("unknown"); got != 10*bytesPerGB {
		t.Errorf("default limit = %d", got)
	}
	if got := s.cameraLimitBytes("zero"); got != 10*bytesPerGB {
		t.Errorf("zero camera limit must fall back to default, got %d", got)
	}
}

func TestCleanOldestGlobally(t *testing.T) {
	cfg := newTestConfig(t)
	cam1 := filepath.Join(cfg.BaseDir, "cam1")
	cam2 := filepath.Join(cfg.BaseDir, "cam2")
	now := time.Now()
	writeFile(t, filepath.Join(cam1, "old.mp4"), 300, now.Add(-5*time.Hour))
	writeFile(t, filepath.Join(cam2, "mid.mp4"), 300, now.Add(-4*time.Hour))
	writeFile(t, filepath.Join(cam2, "new.mp4"), 300, now.Add(-1*time.Hour))

	s := NewStorage(cfg)
	s.cleanOldestGlobally(400)

	if _, err := os.Stat(filepath.Join(cam1, "old.mp4")); err == nil {
		t.Error("oldest global file must be deleted")
	}
	if _, err := os.Stat(filepath.Join(cam2, "mid.mp4")); err == nil {
		t.Error("second oldest global file must be deleted")
	}
	if _, err := os.Stat(filepath.Join(cam2, "new.mp4")); err != nil {
		t.Error("newest file must survive")
	}
}

func TestCleanGlobalLimitSkipsWhenDisabled(t *testing.T) {
	cfg := newTestConfig(t)
	writeFile(t, filepath.Join(cfg.BaseDir, "cam", "a.mp4"), 100, time.Now())

	s := NewStorage(cfg)
	s.config.GlobalSizeGB = 0
	s.cleanGlobalLimit() // must be a no-op

	if _, err := os.Stat(filepath.Join(cfg.BaseDir, "cam", "a.mp4")); err != nil {
		t.Error("file deleted with global limit disabled")
	}
}

func TestCleanByDayLimit(t *testing.T) {
	cfg := newTestConfig(t)
	cameraDir := filepath.Join(cfg.BaseDir, "cam")
	now := time.Now()

	old := filepath.Join(cameraDir, "2020", "01", "05", "09-38.mp4")
	writeFile(t, old, 10, time.Now())
	writeFile(t, filepath.Join(cameraDir, now.Format("2006"), now.Format("01"), now.Format("02"), "10-00.mp4"), 10, now)
	// broken directory names must be ignored
	writeFile(t, filepath.Join(cameraDir, "notayear", "01", "05", "x.mp4"), 10, now)

	s := NewStorage(cfg)
	s.cleanByDayLimit(cameraDir, 7)

	if _, err := os.Stat(old); err == nil {
		t.Error("old day folder must be deleted")
	}
	if _, err := os.Stat(filepath.Join(cameraDir, "notayear")); err != nil {
		t.Error("non-numeric folder must be ignored")
	}
	if _, err := os.Stat(filepath.Join(cameraDir, now.Format("2006"))); err != nil {
		t.Error("recent day folder must survive")
	}
}

func TestCleanEmptyDirs(t *testing.T) {
	cfg := newTestConfig(t)
	base := cfg.BaseDir

	emptyLeaf := filepath.Join(base, "cam", "2020", "01", "05")
	if err := os.MkdirAll(emptyLeaf, 0755); err != nil {
		t.Fatal(err)
	}
	full := filepath.Join(base, "cam2", "2026")
	writeFile(t, filepath.Join(full, "a.mp4"), 10, time.Now())

	s := NewStorage(cfg)
	s.cleanEmptyDirs(base)

	if _, err := os.Stat(emptyLeaf); err == nil {
		t.Error("empty dirs must be removed")
	}
	if _, err := os.Stat(base); err != nil {
		t.Error("base dir must never be removed")
	}
	if _, err := os.Stat(filepath.Join(full, "a.mp4")); err != nil {
		t.Error("non-empty tree must survive")
	}
}

func TestCleanCameraFoldersAppliesDayLimitAndCleansEmptyDirs(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.CameraDayLimits = map[string]int{"cam": 7}
	now := time.Now()

	writeFile(t, filepath.Join(cfg.BaseDir, "cam", "2020", "01", "05", "09-38.mp4"), 10, time.Now())
	writeFile(t, filepath.Join(cfg.BaseDir, "cam", now.Format("2006"), now.Format("01"), now.Format("02"), "10-00.mp4"), 10, now)
	if err := os.MkdirAll(filepath.Join(cfg.BaseDir, "ghost", "empty"), 0755); err != nil {
		t.Fatal(err)
	}

	NewStorage(cfg).CleanCameraFolders()

	if _, err := os.Stat(filepath.Join(cfg.BaseDir, "cam", "2020")); err == nil {
		t.Error("day-limited folder must be deleted")
	}
	if _, err := os.Stat(filepath.Join(cfg.BaseDir, "cam", now.Format("2006"))); err != nil {
		t.Error("recent folder must survive")
	}
	if _, err := os.Stat(filepath.Join(cfg.BaseDir, "ghost")); err == nil {
		t.Error("empty camera folder must be removed")
	}
}

func TestCleanCameraFoldersMissingBaseDir(t *testing.T) {
	s := NewStorage(&config.NVRConfig{BaseDir: filepath.Join(t.TempDir(), "absent")})
	s.CleanCameraFolders() // must not panic
}

func TestGetStorageInfo(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.DefaultCameraLimitGB = 42
	cfg.GlobalSizeGB = 1000
	writeFile(t, filepath.Join(cfg.BaseDir, "cam", "a.mp4"), 2048, time.Now())
	writeFile(t, filepath.Join(cfg.BaseDir, "cam", "b.txt"), 1024, time.Now())

	info := NewStorage(cfg).GetStorageInfo()

	if info["base_dir"] != cfg.BaseDir {
		t.Errorf("base_dir = %v", info["base_dir"])
	}
	if info["file_count"] != int64(2) {
		t.Errorf("file_count = %v", info["file_count"])
	}
	if info["default_camera_limit_gb"] != 42 || info["global_size_gb"] != 1000 {
		t.Errorf("limits = %v / %v", info["default_camera_limit_gb"], info["global_size_gb"])
	}
	size, ok := info["total_size_gb"].(float64)
	if !ok {
		t.Fatalf("total_size_gb type = %T", info["total_size_gb"])
	}
	want := float64(3072) / bytesPerGB
	if size != want {
		t.Errorf("total_size_gb = %v, want %v", size, want)
	}
}

func TestGetCamerasStorageCountsOnlyMP4(t *testing.T) {
	cfg := newTestConfig(t)
	writeFile(t, filepath.Join(cfg.BaseDir, "cam1", "a.mp4"), 100, time.Now())
	writeFile(t, filepath.Join(cfg.BaseDir, "cam1", "b.MP4"), 100, time.Now())
	writeFile(t, filepath.Join(cfg.BaseDir, "cam1", "c.txt"), 100, time.Now())
	writeFile(t, filepath.Join(cfg.BaseDir, "cam2", "d.mp4"), 200, time.Now())
	writeFile(t, filepath.Join(cfg.BaseDir, "loose.mp4"), 100, time.Now()) // not a camera folder

	data := NewStorage(cfg).GetCamerasStorage()

	if len(data) != 2 {
		t.Fatalf("cameras = %d, want 2: %v", len(data), data)
	}
	if data["cam1"]["file_count"] != int64(1) {
		t.Errorf("cam1 file_count = %v", data["cam1"]["file_count"])
	}
	if data["cam2"]["file_count"] != int64(1) {
		t.Errorf("cam2 file_count = %v", data["cam2"]["file_count"])
	}
	if data["cam1"]["size_gb"].(float64) <= 0 {
		t.Errorf("cam1 size = %v", data["cam1"]["size_gb"])
	}
}

func TestGetStorageInfoMissingDir(t *testing.T) {
	s := NewStorage(&config.NVRConfig{BaseDir: filepath.Join(t.TempDir(), "absent")})
	info := s.GetStorageInfo()
	if info["file_count"] != int64(0) {
		t.Errorf("file_count = %v", info["file_count"])
	}
	if got := s.GetCamerasStorage(); len(got) != 0 {
		t.Errorf("cameras = %v", got)
	}
}

// collectFilesSorted is a small helper for tests: files sorted by mtime.
func collectFilesSorted(dir string) []fileEntry {
	files := collectFiles(dir)
	sortByModTime(files)
	return files
}
