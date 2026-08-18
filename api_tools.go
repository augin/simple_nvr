package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

type brokenFile struct {
	Camera string `json:"camera"`
	Date   string `json:"date"`
	File   string `json:"file"`
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	Error  string `json:"error"`
}

type scanState struct {
	mu          sync.Mutex
	running     bool
	total       int
	checked     int
	brokenCount int
	current     string
	brokenFiles []brokenFile
	seen        map[string]bool
	err         string
}

func (a *API) HandleToolsScanStart(w http.ResponseWriter, r *http.Request) {
	if !requireAdminRole(r) {
		http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	a.toolsScan.mu.Lock()
	if a.toolsScan.running {
		a.toolsScan.mu.Unlock()
		json.NewEncoder(w).Encode(map[string]string{"status": "already running"})
		return
	}
	a.toolsScan.running = true
	a.toolsScan.total = 0
	a.toolsScan.checked = 0
	a.toolsScan.brokenCount = 0
	a.toolsScan.current = ""
	a.toolsScan.brokenFiles = nil
	a.toolsScan.seen = make(map[string]bool)
	a.toolsScan.err = ""
	a.toolsScan.mu.Unlock()

	go a.runScan()

	json.NewEncoder(w).Encode(map[string]string{"status": "started"})
}

func (a *API) HandleToolsScanStatus(w http.ResponseWriter, r *http.Request) {
	if !requireAdminRole(r) {
		http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	a.toolsScan.mu.Lock()
	resp := map[string]any{
		"running": a.toolsScan.running,
		"total":   a.toolsScan.total,
		"checked": a.toolsScan.checked,
		"broken":  a.toolsScan.brokenCount,
		"current": a.toolsScan.current,
		"results": a.toolsScan.brokenFiles,
	}
	if !a.toolsScan.running {
		if a.toolsScan.err != "" {
			resp["error"] = a.toolsScan.err
		}
	}
	a.toolsScan.mu.Unlock()

	json.NewEncoder(w).Encode(resp)
}

func (a *API) runScan() {
	entries, err := os.ReadDir(a.config.BaseDir)
	if err != nil {
		a.toolsScan.mu.Lock()
		a.toolsScan.running = false
		a.toolsScan.err = err.Error()
		a.toolsScan.mu.Unlock()
		return
	}

	var mp4Files []struct {
		camera string
		path   string
		name   string
		size   int64
		date   string
	}

	for _, cameraEntry := range entries {
		if !cameraEntry.IsDir() {
			continue
		}
		camera := cameraEntry.Name()
		cameraDir := filepath.Join(a.config.BaseDir, camera)

		filepath.Walk(cameraDir, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || !strings.HasSuffix(info.Name(), ".mp4") {
				return nil
			}
			rel, _ := filepath.Rel(a.config.BaseDir, path)
			parts := strings.SplitN(rel, string(filepath.Separator), 3)
			date := ""
			if len(parts) >= 3 {
				date = parts[1] + "/" + parts[2]
				date = filepath.Dir(date)
			}
			mp4Files = append(mp4Files, struct {
				camera string
				path   string
				name   string
				size   int64
				date   string
			}{camera, path, info.Name(), info.Size(), date})
			return nil
		})
	}

	a.toolsScan.mu.Lock()
	a.toolsScan.total = len(mp4Files)
	a.toolsScan.mu.Unlock()

	activeRec := a.recorder.ActiveRecordings()

	type scanResult struct {
		camera string
		path   string
		name   string
		size   int64
		date   string
		broken bool
		err    string
	}

	fileCh := make(chan struct {
		camera string
		path   string
		name   string
		size   int64
		date   string
	}, len(mp4Files))
	resultCh := make(chan scanResult, len(mp4Files))

	numWorkers := runtime.NumCPU()
	if numWorkers < 2 {
		numWorkers = 2
	}
	if numWorkers > 8 {
		numWorkers = 8
	}

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for f := range fileCh {
				if activeRec[f.path] {
					resultCh <- scanResult{camera: f.camera, path: f.path, name: f.name, size: f.size, date: f.date, broken: false}
					continue
				}

				a.toolsScan.mu.Lock()
				a.toolsScan.current = f.camera + "/" + f.date + "/" + f.name
				a.toolsScan.mu.Unlock()

				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				cmd := exec.CommandContext(ctx, "ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", f.path)
				var stderr bytes.Buffer
				cmd.Stderr = &stderr
				cmd.Run()
				cancel()

				resultCh <- scanResult{camera: f.camera, path: f.path, name: f.name, size: f.size, date: f.date, broken: stderr.Len() > 0, err: strings.TrimSpace(stderr.String())}
			}
		}()
	}

	for _, f := range mp4Files {
		fileCh <- f
	}
	close(fileCh)

	var collectWg sync.WaitGroup
	collectWg.Add(1)
	go func() {
		defer collectWg.Done()
		for res := range resultCh {
			a.toolsScan.mu.Lock()
			a.toolsScan.checked++
			if res.broken && !a.toolsScan.seen[res.path] {
				a.toolsScan.seen[res.path] = true
				a.toolsScan.brokenCount++
				a.toolsScan.brokenFiles = append(a.toolsScan.brokenFiles, brokenFile{
					Camera: res.camera,
					Date:   res.date,
					File:   res.name,
					Path:   res.path,
					Size:   res.size,
					Error:  res.err,
				})
			}
			a.toolsScan.mu.Unlock()
		}
	}()

	wg.Wait()
	close(resultCh)
	collectWg.Wait()

	a.toolsScan.mu.Lock()
	a.toolsScan.running = false
	a.toolsScan.current = ""
	a.toolsScan.mu.Unlock()

	log.Printf("tools scan: %d files checked, %d broken", a.toolsScan.total, a.toolsScan.brokenCount)
}

func (a *API) HandleToolsRepair(w http.ResponseWriter, r *http.Request) {
	if !requireAdminRole(r) {
		http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	var req struct {
		Path string `json:"path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
		return
	}

	if req.Path == "" || !strings.HasPrefix(req.Path, a.config.BaseDir) {
		http.Error(w, `{"error":"invalid path"}`, http.StatusBadRequest)
		return
	}

	if _, err := os.Stat(req.Path); os.IsNotExist(err) {
		http.Error(w, `{"error":"file not found"}`, http.StatusNotFound)
		return
	}

	fi, err := os.Stat(req.Path)
	if err != nil {
		http.Error(w, `{"error":"stat failed"}`, http.StatusInternalServerError)
		return
	}

	if time.Since(fi.ModTime()) < 60*time.Second {
		json.NewEncoder(w).Encode(map[string]any{"status": "skipped", "message": "Файл изменён менее 60 секунд назад, возможно, ещё записывается"})
		return
	}

	camera := ""
	if strings.HasPrefix(req.Path, a.config.BaseDir) {
		rel := strings.TrimPrefix(req.Path, a.config.BaseDir)
		parts := strings.Split(rel, string(filepath.Separator))
		if len(parts) > 1 {
			camera = parts[1]
		}
	}

	log.Printf("repair: attempting ffmpeg recovery for %s (camera=%s)", req.Path, camera)

	if err := recoverWithFFmpeg(req.Path, camera, a.go2rtcAPIBase()); err != nil {
		log.Printf("repair failed %s: %v", req.Path, err)
		json.NewEncoder(w).Encode(map[string]any{"status": "error", "message": fmt.Sprintf("Восстановление невозможно: %v", err)})
		return
	}

	log.Printf("repair succeeded %s", req.Path)
	json.NewEncoder(w).Encode(map[string]any{"status": "ok", "method": "ffmpeg_recover"})
}
