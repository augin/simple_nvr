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

type API struct {
	config     *NVRConfig
	configPath string
	recorder   *Recorder
	storage    *Storage
	alarm      *AlarmServer
}

func NewAPI(config *NVRConfig, configPath string, recorder *Recorder, storage *Storage, alarm *AlarmServer) *API {
	return &API{
		config:     config,
		configPath: configPath,
		recorder:   recorder,
		storage:    storage,
		alarm:      alarm,
	}
}

func (a *API) HandleCameras(w http.ResponseWriter, r *http.Request) {
	go2cfg, err := loadGo2RTCConfig(a.config.Go2RTCConfigPath)
	if err != nil {
		log.Printf("Warning: could not load go2rtc config %s: %v", a.config.Go2RTCConfigPath, err)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"cameras": []string{}, "error": err.Error()})
		return
	}

	cameras := make([]string, 0, len(go2cfg.Streams))
	for _, name := range go2cfg.StreamOrder {
		cameras = append(cameras, name)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"cameras": cameras})
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

	result := make(map[string][]string)

	entries, err := os.ReadDir(cameraDir)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error reading camera dir: %v", err), http.StatusInternalServerError)
		return
	}

	for _, yearEntry := range entries {
		if !yearEntry.IsDir() {
			continue
		}
		yearDir := filepath.Join(cameraDir, yearEntry.Name())

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

func (a *API) HandleStatus(w http.ResponseWriter, r *http.Request) {
	status := a.recorder.GetStatus()
	status["storage"] = a.storage.GetStorageInfo()

	if procs, ok := status["processes"].([]*StreamInfo); ok && len(procs) > 1 {
		go2cfg, err := loadGo2RTCConfig(a.config.Go2RTCConfigPath)
		if err == nil {
			orderMap := make(map[string]int, len(go2cfg.StreamOrder))
			for i, name := range go2cfg.StreamOrder {
				orderMap[name] = i
			}
			sort.Slice(procs, func(i, j int) bool {
				return orderMap[procs[i].Name] < orderMap[procs[j].Name]
			})
			status["processes"] = procs
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

func (a *API) HandleCamerasStorage(w http.ResponseWriter, r *http.Request) {
	data := a.storage.GetCamerasStorage()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func (a *API) HandleRecordStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if a.recorder.IsRecording() {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "already recording"})
		return
	}

	go a.recorder.StartRecording()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "started"})
}

func (a *API) HandleRecordStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	a.recorder.StopRecording()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "stopped"})
}

func (a *API) HandleGetConfig(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(a.config)
}

func (a *API) HandleSaveConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var cfg NVRConfig
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	a.config.BaseDir = cfg.BaseDir
	a.config.ArchiveDir = cfg.ArchiveDir
	a.config.StreamServer = cfg.StreamServer
	a.config.DefaultCameraLimitGB = cfg.DefaultCameraLimitGB
	a.config.GlobalSizeGB = cfg.GlobalSizeGB
	a.config.Go2RTCConfigPath = cfg.Go2RTCConfigPath
	if cfg.HTTPPort > 0 {
		a.config.HTTPPort = cfg.HTTPPort
	}
	a.config.AlarmEnabled = cfg.AlarmEnabled
	if cfg.AlarmPort > 0 {
		a.config.AlarmPort = cfg.AlarmPort
	}
	a.config.MQTTHost = cfg.MQTTHost
	if cfg.MQTTPort > 0 {
		a.config.MQTTPort = cfg.MQTTPort
	}
	a.config.MQTTUser = cfg.MQTTUser
	a.config.MQTTPass = cfg.MQTTPass
	a.config.AlarmCommand = cfg.AlarmCommand
	if cfg.CameraLimits != nil {
		a.config.CameraLimits = cfg.CameraLimits
	}
	if cfg.CameraDayLimits != nil {
		a.config.CameraDayLimits = cfg.CameraDayLimits
	}

	if err := saveNVRConfig(a.configPath, a.config); err != nil {
		log.Printf("Error saving config: %v", err)
		http.Error(w, "save error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
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

	result := make(map[string][]string)

	entries, err := os.ReadDir(cameraDir)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error reading archive dir: %v", err), http.StatusInternalServerError)
		return
	}

	for _, yearEntry := range entries {
		if !yearEntry.IsDir() {
			continue
		}
		yearDir := filepath.Join(cameraDir, yearEntry.Name())

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

func (a *API) HandleAlarmStatus(w http.ResponseWriter, r *http.Request) {
	status := a.alarm.GetStatus()
	status["alarm_enabled"] = a.config.AlarmEnabled
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

func (a *API) HandleAlarmStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := a.alarm.Start(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	a.config.AlarmEnabled = true
	saveNVRConfig(a.configPath, a.config)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "started"})
}

func (a *API) HandleAlarmStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	a.alarm.Stop()

	a.config.AlarmEnabled = false
	saveNVRConfig(a.configPath, a.config)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "stopped"})
}

func (a *API) HandleAlarmLog(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 {
			limit = n
		}
	}

	log := a.alarm.GetLog(limit)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(log)
}

func (a *API) HandleAlarmsRange(w http.ResponseWriter, r *http.Request) {
	camera := r.URL.Query().Get("camera")
	date := r.URL.Query().Get("date")
	if camera == "" || date == "" {
		http.Error(w, "camera and date required", http.StatusBadRequest)
		return
	}

	filePath := filepath.Join(alarmDir, date+".jsonl")
	events, err := readEventsFile(filePath)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]AlarmEvent{})
		return
	}

	filtered := make([]AlarmEvent, 0)
	for _, e := range events {
		if e.Camera == camera {
			filtered = append(filtered, e)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(filtered)
}

func (a *API) HandleAlarmClear(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	a.alarm.ClearLog()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "cleared"})
}
