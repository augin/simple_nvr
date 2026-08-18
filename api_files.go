package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

func listMP4Files(basePath string) (map[string][]string, error) {
	result := make(map[string][]string)

	entries, err := os.ReadDir(basePath)
	if err != nil {
		return nil, err
	}

	for _, yearEntry := range entries {
		if !yearEntry.IsDir() {
			continue
		}
		yearDir := filepath.Join(basePath, yearEntry.Name())

		monthEntries, err := os.ReadDir(yearDir)
		if err != nil {
			continue
		}

		for _, monthEntry := range monthEntries {
			if !monthEntry.IsDir() {
				continue
			}
			monthDir := filepath.Join(yearDir, monthEntry.Name())

			dayEntries, err := os.ReadDir(monthDir)
			if err != nil {
				continue
			}

			for _, dayEntry := range dayEntries {
				if !dayEntry.IsDir() {
					continue
				}
				dayDir := filepath.Join(monthDir, dayEntry.Name())

				files, err := os.ReadDir(dayDir)
				if err != nil {
					continue
				}

				var mp4Files []string
				for _, file := range files {
					if !file.IsDir() && strings.HasSuffix(file.Name(), ".mp4") {
						mp4Files = append(mp4Files, file.Name())
					}
				}

				if len(mp4Files) > 0 {
					sort.Strings(mp4Files)
					folderKey := fmt.Sprintf("%s/%s/%s", yearEntry.Name(), monthEntry.Name(), dayEntry.Name())
					result[folderKey] = mp4Files
				}
			}
		}
	}

	return result, nil
}

func (a *API) HandleFiles(w http.ResponseWriter, r *http.Request) {
	camera := r.URL.Query().Get("camera")
	if camera == "" {
		http.Error(w, "camera parameter required", http.StatusBadRequest)
		return
	}

	cameraDir := filepath.Join(a.config.BaseDir, camera)
	if _, err := os.Stat(cameraDir); os.IsNotExist(err) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{})
		return
	}

	result, err := listMP4Files(cameraDir)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error reading camera dir: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func (a *API) HandleVideo(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/video/"), "/")
	if len(parts) < 5 {
		http.Error(w, "invalid video path", http.StatusBadRequest)
		return
	}

	camera := parts[0]
	year := parts[1]
	month := parts[2]
	day := parts[3]
	file := parts[4]

	videoPath := filepath.Join(a.config.BaseDir, camera, year, month, day, file)

	if _, err := os.Stat(videoPath); os.IsNotExist(err) {
		http.Error(w, "video not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "video/mp4")
	http.ServeFile(w, r, videoPath)
}

func (a *API) HandleVideoDownload(w http.ResponseWriter, r *http.Request) {
	camera := r.URL.Query().Get("camera")
	folder := r.URL.Query().Get("folder")
	file := r.URL.Query().Get("file")
	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")

	if camera == "" || folder == "" || file == "" {
		http.Error(w, "missing parameters", http.StatusBadRequest)
		return
	}

	start, err := strconv.ParseFloat(startStr, 64)
	if err != nil {
		http.Error(w, "invalid start", http.StatusBadRequest)
		return
	}
	end, err := strconv.ParseFloat(endStr, 64)
	if err != nil {
		http.Error(w, "invalid end", http.StatusBadRequest)
		return
	}
	if end <= start {
		http.Error(w, "end must be after start", http.StatusBadRequest)
		return
	}

	videoPath := filepath.Join(a.config.BaseDir, camera, folder, file)
	if _, err := os.Stat(videoPath); os.IsNotExist(err) {
		http.Error(w, "video not found", http.StatusNotFound)
		return
	}

	outName := strings.TrimSuffix(file, ".mp4") + fmt.Sprintf("_%s-%s.mp4",
		strings.ReplaceAll(fmt.Sprintf("%02d%02d%02d", int(start)/3600, (int(start)%3600)/60, int(start)%60), ":", ""),
		strings.ReplaceAll(fmt.Sprintf("%02d%02d%02d", int(end)/3600, (int(end)%3600)/60, int(end)%60), ":", ""))

	archiveDir := filepath.Join(a.config.ArchiveDir, camera, folder)
	if err := os.MkdirAll(archiveDir, 0755); err != nil {
		log.Printf("Error creating archive dir %s: %v", archiveDir, err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	archivePath := filepath.Join(archiveDir, outName)

	cmd := exec.Command("ffmpeg",
		"-hide_banner", "-loglevel", "warning",
		"-y",
		"-ss", fmt.Sprintf("%.3f", start),
		"-i", videoPath,
		"-t", fmt.Sprintf("%.3f", end-start),
		"-c", "copy",
		"-movflags", "+faststart",
		archivePath,
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		log.Printf("Error trimming video %s: %v stderr: %s", videoPath, err, stderr.String())
		http.Error(w, "trim error", http.StatusInternalServerError)
		return
	}

	fi, ferr := os.Stat(archivePath)
	if ferr != nil || fi.Size() == 0 {
		log.Printf("Trim produced empty file %s: size=%d stderr=%s", archivePath, fi.Size(), stderr.String())
		http.Error(w, "trim produced empty file", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "video/mp4")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", outName))
	http.ServeFile(w, r, archivePath)
}
