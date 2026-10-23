package api

import (
	"log"
	"net/http"
	"os"
	"strings"
)

func (a *API) HandleArchive(w http.ResponseWriter, r *http.Request) {
	camera := r.URL.Query().Get("camera")
	if camera == "" {
		writeError(w, http.StatusBadRequest, "camera parameter required")
		return
	}

	cameraDir, err := safeJoin(a.config.ArchiveDir, camera)
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
		writeError(w, http.StatusInternalServerError, "Error reading archive dir")
		return
	}

	writeJSON(w, http.StatusOK, result)
}

func (a *API) HandleArchiveVideo(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/archive/video/"), "/")
	if len(parts) < 5 {
		writeError(w, http.StatusBadRequest, "invalid video path")
		return
	}

	videoPath, err := safeJoin(a.config.ArchiveDir, parts[0], parts[1], parts[2], parts[3], parts[4])
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

func (a *API) HandleArchiveDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if !requireAdminRole(r) {
		writeError(w, http.StatusForbidden, "forbidden")
		return
	}

	camera := r.URL.Query().Get("camera")
	folder := r.URL.Query().Get("folder")
	file := r.URL.Query().Get("file")

	if camera == "" || folder == "" || file == "" {
		writeError(w, http.StatusBadRequest, "missing parameters")
		return
	}

	videoPath, err := safeJoin(a.config.ArchiveDir, camera, folder, file)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid path")
		return
	}
	if _, err := os.Stat(videoPath); os.IsNotExist(err) {
		writeError(w, http.StatusNotFound, "file not found")
		return
	}

	if err := os.Remove(videoPath); err != nil {
		log.Printf("Error deleting archive file %s: %v", videoPath, err)
		writeError(w, http.StatusInternalServerError, "delete error")
		return
	}

	writeStatus(w, "deleted")
}
