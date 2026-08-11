package main

import (
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

type Storage struct {
	config *NVRConfig
}

func NewStorage(config *NVRConfig) *Storage {
	return &Storage{config: config}
}

func (s *Storage) CleanCameraFolders() {
	baseDir := s.config.BaseDir

	entries, err := os.ReadDir(baseDir)
	if err != nil {
		log.Printf("Error reading base dir %s: %v", baseDir, err)
		return
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		cameraDir := filepath.Join(baseDir, entry.Name())
		s.cleanEmptyDirs(cameraDir)
	}

	entries, err = os.ReadDir(baseDir)
	if err != nil {
		return
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		cameraDir := filepath.Join(baseDir, entry.Name())
		sizeGB := s.dirSizeGB(cameraDir)
		limit := s.config.DefaultCameraLimitGB
		if camLimit, ok := s.config.CameraLimits[entry.Name()]; ok && camLimit > 0 {
			limit = camLimit
		}
		spaceToFreeGB := sizeGB - float64(limit)

		if spaceToFreeGB > 0 {
			log.Printf("Cleaning %s (size: %.2f GB, target: %d GB)", cameraDir, sizeGB, limit)
			s.cleanDir(cameraDir, spaceToFreeGB)
		}

		if s.config.CameraDayLimits != nil {
			if dayLimit, ok := s.config.CameraDayLimits[entry.Name()]; ok && dayLimit > 0 {
				s.cleanByDayLimit(cameraDir, dayLimit)
			}
		}
	}

	if s.config.GlobalSizeGB > 0 {
		totalSize := s.totalSizeGB()
		spaceToFree := totalSize - float64(s.config.GlobalSizeGB)
		if spaceToFree > 0 {
			log.Printf("Global limit exceeded (total: %.2f GB, limit: %d GB), freeing %.2f GB", totalSize, s.config.GlobalSizeGB, spaceToFree)
			s.cleanOldestGlobally(spaceToFree)
		}
	}
}

func (s *Storage) totalSizeGB() float64 {
	baseDir := s.config.BaseDir
	var totalSize int64

	entries, err := os.ReadDir(baseDir)
	if err != nil {
		return 0
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		cameraDir := filepath.Join(baseDir, entry.Name())
		filepath.Walk(cameraDir, func(_ string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}
			if !info.IsDir() {
				totalSize += info.Size()
			}
			return nil
		})
	}

	return float64(totalSize) / (1024 * 1024 * 1024)
}

func (s *Storage) cleanOldestGlobally(spaceToFreeGB float64) {
	baseDir := s.config.BaseDir

	type cameraInfo struct {
		name string
		path string
	}

	var cameras []cameraInfo
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if entry.IsDir() {
			cameras = append(cameras, cameraInfo{name: entry.Name(), path: filepath.Join(baseDir, entry.Name())})
		}
	}

	type fileInfo struct {
		path string
		size float64
		mod  int64
	}

	var allFiles []fileInfo
	for _, cam := range cameras {
		filepath.Walk(cam.path, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}
			if !info.IsDir() {
				allFiles = append(allFiles, fileInfo{
					path: path,
					size: float64(info.Size()) / (1024 * 1024 * 1024),
					mod:  info.ModTime().Unix(),
				})
			}
			return nil
		})
	}

	sort.Slice(allFiles, func(i, j int) bool {
		return allFiles[i].mod < allFiles[j].mod
	})

	for _, f := range allFiles {
		if spaceToFreeGB <= 0 {
			break
		}
		if err := os.Remove(f.path); err != nil {
			log.Printf("Error deleting file %s: %v", f.path, err)
			continue
		}
		log.Printf("Global cleanup: deleted %s", f.path)
		spaceToFreeGB -= f.size
	}
}

func (s *Storage) cleanEmptyDirs(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}

	for _, entry := range entries {
		if entry.IsDir() {
			subDir := filepath.Join(dir, entry.Name())
			s.cleanEmptyDirs(subDir)
		}
	}

	entries, err = os.ReadDir(dir)
	if err != nil {
		return
	}

	if len(entries) == 0 && dir != s.config.BaseDir {
		os.Remove(dir)
	}
}

func (s *Storage) cleanDir(dir string, spaceToFreeGB float64) {
	var files []string

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return
	}

	sort.Slice(files, func(i, j int) bool {
		si, _ := os.Stat(files[i])
		sj, _ := os.Stat(files[j])
		if si == nil || sj == nil {
			return false
		}
		return si.ModTime().Before(sj.ModTime())
	})

	for _, file := range files {
		if spaceToFreeGB <= 0 {
			break
		}
		info, err := os.Stat(file)
		if err != nil {
			continue
		}
		fileSizeGB := float64(info.Size()) / (1024 * 1024 * 1024)
		if err := os.Remove(file); err != nil {
			log.Printf("Error deleting file %s: %v", file, err)
			continue
		}
		log.Printf("Deleted %s", file)
		spaceToFreeGB -= fileSizeGB
	}
}

func (s *Storage) cleanByDayLimit(cameraDir string, daysLimit int) {
	cutoff := time.Now().AddDate(0, 0, -daysLimit)

	yearDirs, err := os.ReadDir(cameraDir)
	if err != nil {
		return
	}

	for _, yearEntry := range yearDirs {
		if !yearEntry.IsDir() {
			continue
		}
		year, err := strconv.Atoi(yearEntry.Name())
		if err != nil {
			continue
		}

		monthDirs, err := os.ReadDir(filepath.Join(cameraDir, yearEntry.Name()))
		if err != nil {
			continue
		}

		for _, monthEntry := range monthDirs {
			if !monthEntry.IsDir() {
				continue
			}
			month, err := strconv.Atoi(monthEntry.Name())
			if err != nil {
				continue
			}

			dayDirs, err := os.ReadDir(filepath.Join(cameraDir, yearEntry.Name(), monthEntry.Name()))
			if err != nil {
				continue
			}

			for _, dayEntry := range dayDirs {
				if !dayEntry.IsDir() {
					continue
				}
				day, err := strconv.Atoi(dayEntry.Name())
				if err != nil {
					continue
				}

				dayDate := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.Local)
				if dayDate.Before(cutoff) {
					dayPath := filepath.Join(cameraDir, yearEntry.Name(), monthEntry.Name(), dayEntry.Name())
					log.Printf("Day limit: deleting %s (older than %d days)", dayPath, daysLimit)
					os.RemoveAll(dayPath)
				}
			}
		}
	}
}

func (s *Storage) dirSizeGB(path string) float64 {
	var totalSize int64

	filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() {
			totalSize += info.Size()
		}
		return nil
	})

	return float64(totalSize) / (1024 * 1024 * 1024)
}

func (s *Storage) GetStorageInfo() map[string]any {
	baseDir := s.config.BaseDir

	var totalSize int64
	var fileCount int64

	filepath.Walk(baseDir, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() {
			totalSize += info.Size()
			fileCount++
		}
		return nil
	})

	totalGB := float64(totalSize) / (1024 * 1024 * 1024)

	return map[string]any{
		"total_size_gb":        totalGB,
		"default_camera_limit_gb": s.config.DefaultCameraLimitGB,
		"global_size_gb":         s.config.GlobalSizeGB,
		"file_count":           fileCount,
		"base_dir":             baseDir,
	}
}

func (s *Storage) GetCamerasStorage() map[string]map[string]any {
	baseDir := s.config.BaseDir
	result := make(map[string]map[string]any)

	entries, err := os.ReadDir(baseDir)
	if err != nil {
		return result
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		cameraDir := filepath.Join(baseDir, entry.Name())
		sizeGB := s.dirSizeGB(cameraDir)
		var fileCount int64
		filepath.Walk(cameraDir, func(_ string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}
			if !info.IsDir() && filepath.Ext(info.Name()) == ".mp4" {
				fileCount++
			}
			return nil
		})

		result[entry.Name()] = map[string]any{
			"size_gb":    sizeGB,
			"file_count": fileCount,
		}
	}

	return result
}
