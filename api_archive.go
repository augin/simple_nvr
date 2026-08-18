package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

func (a *API) HandleArchive(w http.ResponseWriter, r *http.Request) {
	camera := r.URL.Query().Get("camera")
	if camera == "" {
		http.Error(w, "camera parameter required", http.StatusBadRequest)
		return
	}

	cameraDir := filepath.Join(a.config.ArchiveDir, camera)
	if _, err := os.Stat(cameraDir); os.IsNotExist(err) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{})
		return
	}

	result, err := listMP4Files(cameraDir)
	if err != nil {
		http.Error(w, "Error reading archive dir", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func (a *API) HandleArchiveVideo(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/archive/video/"), "/")
	if len(parts) < 5 {
		http.Error(w, "invalid video path", http.StatusBadRequest)
		return
	}

	camera := parts[0]
	year := parts[1]
	month := parts[2]
	day := parts[3]
	file := parts[4]

	videoPath := filepath.Join(a.config.ArchiveDir, camera, year, month, day, file)

	if _, err := os.Stat(videoPath); os.IsNotExist(err) {
		http.Error(w, "video not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "video/mp4")
	http.ServeFile(w, r, videoPath)
}

func (a *API) HandleArchiveDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	_, role, _ := GetUserFromContext(r)
	if role != "admin" {
		http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
		return
	}

	camera := r.URL.Query().Get("camera")
	folder := r.URL.Query().Get("folder")
	file := r.URL.Query().Get("file")

	if camera == "" || folder == "" || file == "" {
		http.Error(w, "missing parameters", http.StatusBadRequest)
		return
	}

	videoPath := filepath.Join(a.config.ArchiveDir, camera, folder, file)
	if _, err := os.Stat(videoPath); os.IsNotExist(err) {
		http.Error(w, "file not found", http.StatusNotFound)
		return
	}

	if err := os.Remove(videoPath); err != nil {
		log.Printf("Error deleting archive file %s: %v", videoPath, err)
		http.Error(w, "delete error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "deleted"})
}
