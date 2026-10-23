package api

import (
	"bytes"
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

// listMP4Files walks base/<year>/<month>/<day> and returns sorted .mp4 names
// keyed by "year/month/day".
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

		monthEntries, err := os.ReadDir(filepath.Join(basePath, yearEntry.Name()))
		if err != nil {
			continue
		}

		for _, monthEntry := range monthEntries {
			if !monthEntry.IsDir() {
				continue
			}

			dayEntries, err := os.ReadDir(filepath.Join(basePath, yearEntry.Name(), monthEntry.Name()))
			if err != nil {
				continue
			}

			for _, dayEntry := range dayEntries {
				if !dayEntry.IsDir() {
					continue
				}

				files, err := os.ReadDir(filepath.Join(basePath, yearEntry.Name(), monthEntry.Name(), dayEntry.Name()))
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
		writeError(w, http.StatusBadRequest, "camera parameter required")
		return
	}

	cameraDir, err := safeJoin(a.config.BaseDir, camera)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid camera")
		return
	}
	if _, err := os.Stat(cameraDir); os.IsNotExist(err) {
		writeJSON(w, http.StatusOK, map[string]any{})
		return
	}

	result, err := listMP4Files(cameraDir)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Error reading camera dir: %v", err))
		return
	}

	writeJSON(w, http.StatusOK, result)
}

func (a *API) HandleVideo(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/video/"), "/")
	if len(parts) < 5 {
		writeError(w, http.StatusBadRequest, "invalid video path")
		return
	}

	videoPath, err := safeJoin(a.config.BaseDir, parts[0], parts[1], parts[2], parts[3], parts[4])
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid video path")
		return
	}

	if _, err := os.Stat(videoPath); os.IsNotExist(err) {
		writeError(w, http.StatusNotFound, "video not found")
		return
	}

	w.Header().Set("Content-Type", "video/mp4")
	http.ServeFile(w, r, videoPath)
}

// hhmmss formats seconds since midnight as HHMMSS for clip file names.
func hhmmss(seconds float64) string {
	s := int(seconds)
	return fmt.Sprintf("%02d%02d%02d", s/3600, (s%3600)/60, s%60)
}

func (a *API) HandleVideoDownload(w http.ResponseWriter, r *http.Request) {
	camera := r.URL.Query().Get("camera")
	folder := r.URL.Query().Get("folder")
	file := r.URL.Query().Get("file")
	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")

	if camera == "" || folder == "" || file == "" {
		writeError(w, http.StatusBadRequest, "missing parameters")
		return
	}

	start, err := strconv.ParseFloat(startStr, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid start")
		return
	}
	end, err := strconv.ParseFloat(endStr, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid end")
		return
	}
	if end <= start {
		writeError(w, http.StatusBadRequest, "end must be after start")
		return
	}

	videoPath, err := safeJoin(a.config.BaseDir, camera, folder, file)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid path")
		return
	}
	if _, err := os.Stat(videoPath); os.IsNotExist(err) {
		writeError(w, http.StatusNotFound, "video not found")
		return
	}

	outName := strings.TrimSuffix(file, ".mp4") + fmt.Sprintf("_%s-%s.mp4", hhmmss(start), hhmmss(end))

	archiveDir, err := safeJoin(a.config.ArchiveDir, camera, folder)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid path")
		return
	}
	if err := os.MkdirAll(archiveDir, 0755); err != nil {
		log.Printf("Error creating archive dir %s: %v", archiveDir, err)
		writeError(w, http.StatusInternalServerError, "internal error")
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
		writeError(w, http.StatusInternalServerError, "trim error")
		return
	}

	// The trim can exit zero yet write nothing; a missing file must not
	// reach os.Stat results with a nil FileInfo.
	fi, err := os.Stat(archivePath)
	if err != nil || fi.Size() == 0 {
		log.Printf("Trim produced empty file %s: err=%v stderr=%s", archivePath, err, stderr.String())
		writeError(w, http.StatusInternalServerError, "trim produced empty file")
		return
	}

	w.Header().Set("Content-Type", "video/mp4")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", outName))
	http.ServeFile(w, r, archivePath)
}
