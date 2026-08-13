package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
)

type API struct {
	config          *NVRConfig
	configPath      string
	recorder        *Recorder
	storage         *Storage
	alarm           *AlarmServer
	hikvisionAlarm  *HikvisionAlarmServer
	logBuffer       *LogBuffer
	userStore       *UserStore
}

func NewAPI(config *NVRConfig, configPath string, recorder *Recorder, storage *Storage, alarm *AlarmServer, hikvisionAlarm *HikvisionAlarmServer, logBuffer *LogBuffer, userStore *UserStore) *API {
	return &API{
		config:         config,
		configPath:     configPath,
		recorder:       recorder,
		storage:        storage,
		alarm:          alarm,
		hikvisionAlarm: hikvisionAlarm,
		logBuffer:      logBuffer,
		userStore:      userStore,
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

func requireAdminRole(r *http.Request) bool {
	_, role, _ := GetUserFromContext(r)
	return role == "admin"
}

func (a *API) HandleRecordStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if !requireAdminRole(r) {
		http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
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

	if !requireAdminRole(r) {
		http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
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

	if !requireAdminRole(r) {
		http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
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

func (a *API) HandleAlarmStatus(w http.ResponseWriter, r *http.Request) {
	dahuaStatus := a.alarm.GetStatus()
	hikvisionStatus := a.hikvisionAlarm.GetStatus()

	status := map[string]any{
		"dahua":     dahuaStatus,
		"hikvision": hikvisionStatus,
		"event_count": dahuaStatus["event_count"],
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

func (a *API) HandleAlarmStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if !requireAdminRole(r) {
		http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
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

	if !requireAdminRole(r) {
		http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
		return
	}

	a.alarm.Stop()

	a.config.AlarmEnabled = false
	saveNVRConfig(a.configPath, a.config)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "stopped"})
}

func (a *API) HandleHikvisionAlarmStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if !requireAdminRole(r) {
		http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
		return
	}

	if err := a.hikvisionAlarm.Start(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	a.config.HikvisionEnabled = true
	saveNVRConfig(a.configPath, a.config)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "started"})
}

func (a *API) HandleHikvisionAlarmStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if !requireAdminRole(r) {
		http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
		return
	}

	a.hikvisionAlarm.Stop()

	a.config.HikvisionEnabled = false
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

	if !requireAdminRole(r) {
		http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
		return
	}

	a.alarm.ClearLog()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "cleared"})
}

func (a *API) HandleLogs(w http.ResponseWriter, r *http.Request) {
	limit := 500
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 {
			limit = n
		}
	}
	since := r.URL.Query().Get("since")

	logs := a.logBuffer.GetLogs(limit, since)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(logs)
}

func (a *API) HandleLogsClear(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if !requireAdminRole(r) {
		http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
		return
	}

	a.logBuffer.Clear()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "cleared"})
}

func (a *API) HandleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
		return
	}

	user, err := a.userStore.Authenticate(req.Username, req.Password)
	if err != nil {
		http.Error(w, `{"error":"invalid credentials"}`, http.StatusUnauthorized)
		return
	}

	cookie := a.userStore.CreateSessionCookie(req.Username, user.Role)
	http.SetCookie(w, cookie)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"username": req.Username,
		"role":     user.Role,
	})
}

func (a *API) HandleLogout(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name:     "session_token",
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		MaxAge:   -1,
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "logged out"})
}

func (a *API) HandleMe(w http.ResponseWriter, r *http.Request) {
	username, role, ok := GetUserFromContext(r)
	if !ok {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"authorized": false})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"authorized": true,
		"username":   username,
		"role":       role,
	})
}

func (a *API) HandleAuthCheck(w http.ResponseWriter, r *http.Request) {
	if !a.userStore.HasUsers() {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"auth_required": false})
		return
	}

	username, role, ok := GetUserFromContext(r)
	if !ok {
		if cookie, err := r.Cookie("session_token"); err == nil {
			username, role, err = a.userStore.ParseSessionCookie(cookie)
			if err == nil {
				ok = true
			}
		}
	}

	if !ok {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"auth_required": true, "authorized": false})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"auth_required": true,
		"authorized":    true,
		"username":      username,
		"role":          role,
	})
}

func (a *API) HandleGetUsers(w http.ResponseWriter, r *http.Request) {
	users := a.userStore.GetUsers()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(users)
}

func (a *API) HandleAddUser(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if a.userStore.HasUsers() {
		_, role, ok := GetUserFromContext(r)
		if !ok || role != "admin" {
			http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
			return
		}
	}

	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
		Role     string `json:"role"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
		return
	}

	if req.Username == "" || req.Password == "" {
		http.Error(w, `{"error":"username and password required"}`, http.StatusBadRequest)
		return
	}

	if !a.userStore.HasUsers() && req.Role != "admin" {
		req.Role = "admin"
	}

	if req.Role == "" {
		req.Role = "user"
	}

	if err := a.userStore.AddUser(req.Username, req.Password, req.Role); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "created"})
}

func (a *API) HandleDeleteUser(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	username := r.URL.Query().Get("username")
	if username == "" {
		http.Error(w, `{"error":"username required"}`, http.StatusBadRequest)
		return
	}

	currentUser, _, _ := GetUserFromContext(r)
	if currentUser == username {
		http.Error(w, `{"error":"cannot delete yourself"}`, http.StatusBadRequest)
		return
	}

	if err := a.userStore.DeleteUser(username); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "deleted"})
}

func (a *API) HandleChangePassword(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Username    string `json:"username"`
		OldPassword string `json:"old_password"`
		NewPassword string `json:"new_password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
		return
	}

	currentUser, currentRole, _ := GetUserFromContext(r)

	if currentRole != "admin" {
		if currentUser != req.Username {
			http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
			return
		}
		if req.OldPassword == "" {
			http.Error(w, `{"error":"old password required"}`, http.StatusBadRequest)
			return
		}
		if _, err := a.userStore.Authenticate(currentUser, req.OldPassword); err != nil {
			http.Error(w, `{"error":"invalid old password"}`, http.StatusUnauthorized)
			return
		}
	}

	if req.NewPassword == "" {
		http.Error(w, `{"error":"new password required"}`, http.StatusBadRequest)
		return
	}

	if err := a.userStore.ChangePassword(req.Username, req.NewPassword); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "password changed"})
}

func (a *API) HandleGo2RTCStatus(w http.ResponseWriter, r *http.Request) {
	if !requireAdminRole(r) {
		http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
		return
	}

	status := map[string]any{
		"config_path": a.config.Go2RTCConfigPath,
		"running":     false,
		"version":     "",
	}

	out, err := exec.Command("systemctl", "is-active", "go2rtc").Output()
	if err == nil && strings.TrimSpace(string(out)) == "active" {
		status["running"] = true
	}

	resp, err := http.Get("http://localhost:1984/api")
	if err == nil {
		defer resp.Body.Close()
		var go2rtcInfo map[string]any
		if json.NewDecoder(resp.Body).Decode(&go2rtcInfo) == nil {
			if v, ok := go2rtcInfo["version"].(string); ok {
				status["version"] = v
			}
			if rtsp, ok := go2rtcInfo["rtsp"].(map[string]any); ok {
				if listen, ok := rtsp["listen"].(string); ok {
					status["rtsp_listen"] = listen
				}
			}
		}
	}

	ghResp, err := http.Get("https://api.github.com/repos/AlexxIT/go2rtc/releases/latest")
	if err == nil {
		defer ghResp.Body.Close()
		var release struct {
			TagName string `json:"tag_name"`
			Assets  []struct {
				Name               string `json:"name"`
				BrowserDownloadURL string `json:"browser_download_url"`
			} `json:"assets"`
		}
		if json.NewDecoder(ghResp.Body).Decode(&release) == nil {
			latestVersion := strings.TrimPrefix(release.TagName, "v")
			currentVersion := status["version"].(string)
			status["latest_version"] = latestVersion
			status["update_available"] = latestVersion != "" && currentVersion != "" && latestVersion != currentVersion
			if status["update_available"].(bool) {
				arch := runtime.GOARCH
				binaryName := "go2rtc_linux_" + arch
				for _, asset := range release.Assets {
					if asset.Name == binaryName {
						status["update_url"] = asset.BrowserDownloadURL
						break
					}
				}
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

func (a *API) HandleGo2RTCRestart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !requireAdminRole(r) {
		http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
		return
	}

	cmd := exec.Command("systemctl", "restart", "go2rtc")
	if out, err := cmd.CombinedOutput(); err != nil {
		log.Printf("go2rtc restart error: %v %s", err, string(out))
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "restarted"})
}

func (a *API) HandleGo2RTCUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !requireAdminRole(r) {
		http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
		return
	}

	var req struct {
		URL string `json:"url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.URL == "" {
		http.Error(w, `{"error":"url required"}`, http.StatusBadRequest)
		return
	}

	log.Printf("go2rtc update: downloading %s", req.URL)

	httpResp, err := http.Get(req.URL)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"download failed: %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != 200 {
		http.Error(w, fmt.Sprintf(`{"error":"download returned %d"}`, httpResp.StatusCode), http.StatusInternalServerError)
		return
	}

	tmpPath := "/usr/bin/go2rtc.tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"create temp: %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if _, err := io.Copy(f, httpResp.Body); err != nil {
		f.Close()
		os.Remove(tmpPath)
		http.Error(w, fmt.Sprintf(`{"error":"write temp: %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	f.Close()
	os.Chmod(tmpPath, 0755)

	exec.Command("systemctl", "stop", "go2rtc").Run()

	if err := os.Rename(tmpPath, "/usr/bin/go2rtc"); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"replace binary: %s"}`, err.Error()), http.StatusInternalServerError)
		exec.Command("systemctl", "start", "go2rtc").Run()
		return
	}

	exec.Command("systemctl", "start", "go2rtc").Run()

	log.Printf("go2rtc updated successfully")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "updated"})
}

func (a *API) HandleGo2RTCCameras(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		a.handleGo2RTCCamerasGet(w, r)
	case http.MethodPost:
		a.handleGo2RTCCamerasAdd(w, r)
	case http.MethodDelete:
		a.handleGo2RTCCamerasDelete(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (a *API) handleGo2RTCCamerasGet(w http.ResponseWriter, r *http.Request) {
	go2cfg, err := loadGo2RTCConfig(a.config.Go2RTCConfigPath)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	type cameraInfo struct {
		Name    string `json:"name"`
		Type    string `json:"type"`
		URL     string `json:"url"`
		IP      string `json:"ip"`
		LimitGB int    `json:"limit_gb"`
		Channel string `json:"channel,omitempty"`
	}

	cameras := make([]cameraInfo, 0, len(go2cfg.StreamOrder))
	for _, name := range go2cfg.StreamOrder {
		urlStr := ""
		switch v := go2cfg.Streams[name].(type) {
		case string:
			urlStr = v
		}

		camType := "unknown"
		ip := ""
		channel := ""
		if urlStr != "" {
			if strings.HasPrefix(urlStr, "dvrip://") {
				camType = "dvrip"
			} else if strings.HasPrefix(urlStr, "rtsp://") {
				camType = "rtsp"
			} else if strings.HasPrefix(urlStr, "onvif://") {
				camType = "onvif"
			} else if strings.HasPrefix(urlStr, "isapi://") {
				camType = "isapi"
			}

			if idx := strings.Index(urlStr, "@"); idx != -1 {
				parts := urlStr[idx+1:]
				if qIdx := strings.Index(parts, "?"); qIdx != -1 {
					parts = parts[:qIdx]
				}
				if colonIdx := strings.Index(parts, ":"); colonIdx != -1 {
					ip = parts[:colonIdx]
				} else if slashIdx := strings.Index(parts, "/"); slashIdx != -1 {
					ip = parts[:slashIdx]
				} else {
					ip = parts
				}
			}

			if camType == "dvrip" {
				if idx := strings.Index(urlStr, "channel="); idx != -1 {
					channel = urlStr[idx+8:]
					if ampIdx := strings.Index(channel, "&"); ampIdx != -1 {
						channel = channel[:ampIdx]
					}
				}
			}
		}

		limitGB := a.config.DefaultCameraLimitGB
		if l, ok := a.config.CameraLimits[name]; ok {
			limitGB = l
		}

		cameras = append(cameras, cameraInfo{
			Name:    name,
			Type:    camType,
			URL:     urlStr,
			IP:      ip,
			LimitGB: limitGB,
			Channel: channel,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(cameras)
}

func (a *API) handleGo2RTCCamerasAdd(w http.ResponseWriter, r *http.Request) {
	if !requireAdminRole(r) {
		http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
		return
	}

	var req struct {
		Name    string `json:"name"`
		Type    string `json:"type"`
		User    string `json:"user"`
		Pass    string `json:"pass"`
		IP      string `json:"ip"`
		Port    string `json:"port"`
		Channel string `json:"channel"`
		LimitGB int    `json:"limit_gb"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
		return
	}

	if req.Name == "" || req.IP == "" {
		http.Error(w, `{"error":"name and ip required"}`, http.StatusBadRequest)
		return
	}

	var urlStr string
	switch req.Type {
	case "rtsp":
		port := req.Port
		if port == "" {
			port = "554"
		}
		urlStr = fmt.Sprintf("rtsp://%s:%s@%s:%s/stream0", req.User, req.Pass, req.IP, port)
	case "dvrip":
		channel := req.Channel
		if channel == "" {
			channel = "0"
		}
		urlStr = fmt.Sprintf("dvrip://%s:%s@%s?channel=%s&subtype=0", req.User, req.Pass, req.IP, channel)
	case "onvif":
		urlStr = fmt.Sprintf("onvif://%s:%s@%s", req.User, req.Pass, req.IP)
	case "isapi":
		urlStr = fmt.Sprintf("isapi://%s:%s@%s", req.User, req.Pass, req.IP)
	default:
		http.Error(w, `{"error":"unsupported type"}`, http.StatusBadRequest)
		return
	}

	go2cfg, err := loadGo2RTCConfig(a.config.Go2RTCConfigPath)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	go2cfg.Streams[req.Name] = urlStr
	go2cfg.StreamOrder = append(go2cfg.StreamOrder, req.Name)

	if err := saveGo2RTCConfig(a.config.Go2RTCConfigPath, go2cfg.Streams); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"save config: %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	limitGB := req.LimitGB
	if limitGB <= 0 {
		limitGB = a.config.DefaultCameraLimitGB
	}
	a.config.CameraLimits[req.Name] = limitGB
	if err := saveNVRConfig(a.configPath, a.config); err != nil {
		log.Printf("warning: save nvr config: %v", err)
	}

	exec.Command("systemctl", "restart", "go2rtc").Run()

	log.Printf("go2rtc: added camera %s (%s)", req.Name, req.Type)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "added"})
}

func (a *API) handleGo2RTCCamerasDelete(w http.ResponseWriter, r *http.Request) {
	if !requireAdminRole(r) {
		http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
		return
	}

	name := r.URL.Query().Get("name")
	if name == "" {
		http.Error(w, `{"error":"name required"}`, http.StatusBadRequest)
		return
	}

	go2cfg, err := loadGo2RTCConfig(a.config.Go2RTCConfigPath)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	if _, ok := go2cfg.Streams[name]; !ok {
		http.Error(w, `{"error":"camera not found"}`, http.StatusNotFound)
		return
	}

	delete(go2cfg.Streams, name)
	newOrder := make([]string, 0, len(go2cfg.StreamOrder)-1)
	for _, n := range go2cfg.StreamOrder {
		if n != name {
			newOrder = append(newOrder, n)
		}
	}
	go2cfg.StreamOrder = newOrder

	if err := saveGo2RTCConfig(a.config.Go2RTCConfigPath, go2cfg.Streams); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"save config: %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	exec.Command("systemctl", "restart", "go2rtc").Run()

	log.Printf("go2rtc: deleted camera %s", name)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "deleted"})
}
