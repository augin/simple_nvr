package main

import (
	"log"
	"os"
	"path/filepath"
	"sort"
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
		spaceToFreeGB := sizeGB - float64(s.config.TargetSizeGB)

		if spaceToFreeGB > 0 {
			log.Printf("Cleaning %s (size: %.2f GB, target: %d GB)", cameraDir, sizeGB, s.config.TargetSizeGB)
			s.cleanDir(cameraDir, spaceToFreeGB)
		}
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
		"total_size_gb": totalGB,
		"target_size_gb": s.config.TargetSizeGB,
		"file_count":     fileCount,
		"base_dir":       baseDir,
	}
}
