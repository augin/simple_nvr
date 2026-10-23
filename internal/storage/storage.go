// Package storage enforces per-camera and global disk limits for recordings
// and reports storage statistics for the UI.
package storage

import (
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"simple_nvr/internal/config"
)

const (
	bytesPerGB   = 1024 * 1024 * 1024
	mp4Extension = ".mp4"
)

// fileEntry describes a regular file discovered while walking a tree.
type fileEntry struct {
	path    string
	size    int64
	modTime time.Time
}

// Storage cleans up recordings according to the configured limits.
type Storage struct {
	config *config.NVRConfig
}

// NewStorage creates a cleanup worker for the given configuration.
func NewStorage(cfg *config.NVRConfig) *Storage {
	return &Storage{config: cfg}
}

// CleanCameraFolders applies per-camera size limits, per-camera day limits and
// finally the global size limit (oldest files first).
func (s *Storage) CleanCameraFolders() {
	cameras, err := s.cameraDirs()
	if err != nil {
		log.Printf("Error reading base dir %s: %v", s.config.BaseDir, err)
		return
	}

	for _, cameraDir := range cameras {
		s.cleanEmptyDirs(cameraDir)
	}

	if cameras, err = s.cameraDirs(); err != nil {
		return
	}
	for _, cameraDir := range cameras {
		s.cleanCamera(cameraDir)
	}

	// Day limit deletions may leave new empty parents behind.
	if cameras, err = s.cameraDirs(); err == nil {
		for _, cameraDir := range cameras {
			s.cleanEmptyDirs(cameraDir)
		}
	}

	s.cleanGlobalLimit()
}

// cameraDirs lists the camera folders inside base_dir.
func (s *Storage) cameraDirs() ([]string, error) {
	entries, err := os.ReadDir(s.config.BaseDir)
	if err != nil {
		return nil, err
	}
	var dirs []string
	for _, entry := range entries {
		if entry.IsDir() {
			dirs = append(dirs, filepath.Join(s.config.BaseDir, entry.Name()))
		}
	}
	return dirs, nil
}

// cleanCamera applies the size and day limits of a single camera folder.
func (s *Storage) cleanCamera(cameraDir string) {
	camera := filepath.Base(cameraDir)
	limitBytes := s.cameraLimitBytes(camera)

	sizeBytes := dirSizeBytes(cameraDir)
	if extra := sizeBytes - limitBytes; extra > 0 {
		log.Printf("Cleaning %s (size: %.2f GB, target: %.2f GB)",
			cameraDir, toGB(sizeBytes), toGB(limitBytes))
		s.cleanDir(cameraDir, extra)
	}

	if dayLimit, ok := s.config.CameraDayLimits[camera]; ok && dayLimit > 0 {
		s.cleanByDayLimit(cameraDir, dayLimit)
	}
}

// cleanGlobalLimit frees the oldest files across all cameras when the total
// size exceeds global_size_gb.
func (s *Storage) cleanGlobalLimit() {
	if s.config.GlobalSizeGB <= 0 {
		return
	}
	limitBytes := int64(s.config.GlobalSizeGB) * bytesPerGB
	totalBytes := s.totalSizeBytes()
	if extra := totalBytes - limitBytes; extra > 0 {
		log.Printf("Global limit exceeded (total: %.2f GB, limit: %d GB), freeing %.2f GB",
			toGB(totalBytes), s.config.GlobalSizeGB, toGB(extra))
		s.cleanOldestGlobally(extra)
	}
}

// cameraLimitBytes resolves camera_limits with the default limit as fallback.
func (s *Storage) cameraLimitBytes(camera string) int64 {
	limitGB := s.config.DefaultCameraLimitGB
	if camLimit, ok := s.config.CameraLimits[camera]; ok && camLimit > 0 {
		limitGB = camLimit
	}
	return int64(limitGB) * bytesPerGB
}

// totalSizeBytes sums file sizes across all camera folders.
func (s *Storage) totalSizeBytes() int64 {
	entries, err := os.ReadDir(s.config.BaseDir)
	if err != nil {
		return 0
	}
	var total int64
	for _, entry := range entries {
		if entry.IsDir() {
			total += dirSizeBytes(filepath.Join(s.config.BaseDir, entry.Name()))
		}
	}
	return total
}

// cleanOldestGlobally deletes the oldest files (across all cameras) until
// freeBytes have been reclaimed.
func (s *Storage) cleanOldestGlobally(freeBytes int64) {
	files := collectFiles(s.config.BaseDir)
	sortByModTime(files)
	removeUntilFreed(files, freeBytes)
}

// cleanEmptyDirs removes empty subdirectories (never the base dir itself).
func (s *Storage) cleanEmptyDirs(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if entry.IsDir() {
			s.cleanEmptyDirs(filepath.Join(dir, entry.Name()))
		}
	}

	entries, err = os.ReadDir(dir)
	if err != nil || len(entries) > 0 || dir == s.config.BaseDir {
		return
	}
	if err := os.Remove(dir); err == nil {
		log.Printf("Removed empty directory %s", dir)
	}
}

// cleanDir deletes the oldest files under dir until freeBytes are reclaimed.
func (s *Storage) cleanDir(dir string, freeBytes int64) {
	files := collectFiles(dir)
	sortByModTime(files)
	removeUntilFreed(files, freeBytes)
}

// cleanByDayLimit deletes whole day folders older than daysLimit days.
func (s *Storage) cleanByDayLimit(cameraDir string, daysLimit int) {
	cutoff := time.Now().AddDate(0, 0, -daysLimit)

	for _, path := range dayDirsBefore(cameraDir, cutoff) {
		log.Printf("Day limit: deleting %s (older than %d days)", path, daysLimit)
		if err := os.RemoveAll(path); err != nil {
			log.Printf("Day limit: error deleting %s: %v", path, err)
		}
	}
}

// dayDirsBefore lists <camera>/<year>/<month>/<day> directories older than cutoff.
func dayDirsBefore(cameraDir string, cutoff time.Time) []string {
	var stale []string

	years := readSubdirs(cameraDir)
	for _, year := range years {
		yearNum, err := strconv.Atoi(year.name)
		if err != nil {
			continue
		}
		for _, month := range readSubdirs(year.path) {
			monthNum, err := strconv.Atoi(month.name)
			if err != nil {
				continue
			}
			for _, day := range readSubdirs(month.path) {
				dayNum, err := strconv.Atoi(day.name)
				if err != nil {
					continue
				}
				dayDate := time.Date(yearNum, time.Month(monthNum), dayNum, 0, 0, 0, 0, time.Local)
				if dayDate.Before(cutoff) {
					stale = append(stale, day.path)
				}
			}
		}
	}
	return stale
}

type subdir struct {
	name string
	path string
}

func readSubdirs(dir string) []subdir {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var dirs []subdir
	for _, entry := range entries {
		if entry.IsDir() {
			dirs = append(dirs, subdir{name: entry.Name(), path: filepath.Join(dir, entry.Name())})
		}
	}
	return dirs
}

// collectFiles walks root and returns every regular file.
func collectFiles(root string) []fileEntry {
	var files []fileEntry
	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		files = append(files, fileEntry{path: path, size: info.Size(), modTime: info.ModTime()})
		return nil
	})
	return files
}

func sortByModTime(files []fileEntry) {
	sort.Slice(files, func(i, j int) bool {
		return files[i].modTime.Before(files[j].modTime)
	})
}

// removeUntilFreed deletes files oldest first until freeBytes are reclaimed.
// Returns the number of bytes actually freed.
func removeUntilFreed(files []fileEntry, freeBytes int64) int64 {
	var freed int64
	for _, f := range files {
		if freed >= freeBytes {
			break
		}
		if err := os.Remove(f.path); err != nil {
			log.Printf("Error deleting file %s: %v", f.path, err)
			continue
		}
		log.Printf("Deleted %s", f.path)
		freed += f.size
	}
	return freed
}

// dirSizeBytes sums file sizes under path.
func dirSizeBytes(path string) int64 {
	var total int64
	_ = filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total
}

func toGB(bytes int64) float64 {
	return float64(bytes) / bytesPerGB
}

// GetStorageInfo reports total size, file count and configured limits.
func (s *Storage) GetStorageInfo() map[string]any {
	baseDir := s.config.BaseDir

	var totalSize, fileCount int64
	_ = filepath.Walk(baseDir, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			totalSize += info.Size()
			fileCount++
		}
		return nil
	})

	return map[string]any{
		"total_size_gb":           toGB(totalSize),
		"default_camera_limit_gb": s.config.DefaultCameraLimitGB,
		"global_size_gb":          s.config.GlobalSizeGB,
		"file_count":              fileCount,
		"base_dir":                baseDir,
	}
}

// GetCamerasStorage reports size and MP4 count per camera folder.
func (s *Storage) GetCamerasStorage() map[string]map[string]any {
	result := make(map[string]map[string]any)

	entries, err := os.ReadDir(s.config.BaseDir)
	if err != nil {
		return result
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		cameraDir := filepath.Join(s.config.BaseDir, entry.Name())

		var fileCount int64
		for _, f := range collectFiles(cameraDir) {
			if filepath.Ext(f.path) == mp4Extension {
				fileCount++
			}
		}

		result[entry.Name()] = map[string]any{
			"size_gb":    toGB(dirSizeBytes(cameraDir)),
			"file_count": fileCount,
		}
	}
	return result
}
