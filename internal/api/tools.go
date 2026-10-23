package api

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"simple_nvr/internal/recovery"
)

func (a *API) HandleToolsScanStart(w http.ResponseWriter, r *http.Request) {
	if !requireAdminRole(r) {
		writeError(w, http.StatusForbidden, "forbidden")
		return
	}

	if !a.toolsScan.start() {
		writeStatus(w, "already running")
		return
	}

	go a.runScan()

	writeStatus(w, "started")
}

func (a *API) HandleToolsScanStatus(w http.ResponseWriter, r *http.Request) {
	if !requireAdminRole(r) {
		writeError(w, http.StatusForbidden, "forbidden")
		return
	}

	writeJSON(w, http.StatusOK, a.toolsScan.snapshot())
}

// pathUnderBase reports whether candidate names a file inside base. Unlike a
// plain prefix check this rejects "/baseevil" for base "/base" and resolves
// symlinks before comparing.
func pathUnderBase(base, candidate string) bool {
	realBase, err := filepath.EvalSymlinks(base)
	if err != nil {
		realBase = filepath.Clean(base)
	}
	realCandidate, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		realCandidate = filepath.Clean(candidate)
	}
	rel, err := filepath.Rel(realBase, realCandidate)
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func (a *API) HandleToolsRepair(w http.ResponseWriter, r *http.Request) {
	if !requireAdminRole(r) {
		writeError(w, http.StatusForbidden, "forbidden")
		return
	}

	var req struct {
		Path string `json:"path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request")
		return
	}

	if req.Path == "" || !pathUnderBase(a.config.BaseDir, req.Path) {
		writeError(w, http.StatusBadRequest, "invalid path")
		return
	}

	fi, err := os.Stat(req.Path)
	if os.IsNotExist(err) {
		writeError(w, http.StatusNotFound, "file not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "stat failed")
		return
	}

	if time.Since(fi.ModTime()) < 60*time.Second {
		writeJSON(w, http.StatusOK, map[string]any{
			"status":  "skipped",
			"message": "Файл изменён менее 60 секунд назад, возможно, ещё записывается",
		})
		return
	}

	rel, _ := filepath.Rel(a.config.BaseDir, req.Path)
	camera := strings.Split(rel, string(filepath.Separator))[0]

	log.Printf("repair: attempting ffmpeg recovery for %s (camera=%s)", req.Path, camera)

	if err := recovery.RecoverWithFFmpeg(req.Path, camera, a.go2rtc.BaseURL); err != nil {
		log.Printf("repair failed %s: %v", req.Path, err)
		writeJSON(w, http.StatusOK, map[string]any{
			"status":  "error",
			"message": fmt.Sprintf("Восстановление невозможно: %v", err),
		})
		return
	}

	log.Printf("repair succeeded %s", req.Path)
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "method": "ffmpeg_recover"})
}
