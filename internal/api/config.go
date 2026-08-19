package api

import (
	"encoding/json"
	"log"
	"net/http"
	"sort"

	"simple_nvr/internal/config"
	"simple_nvr/internal/recorder"
)

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

	var cfg config.NVRConfig
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
	a.config.KioskEnabled = cfg.KioskEnabled
	if cfg.KioskPort > 0 {
		a.config.KioskPort = cfg.KioskPort
	}

	if err := config.SaveNVRConfig(a.configPath, a.config); err != nil {
		log.Printf("Error saving config: %v", err)
		http.Error(w, "save error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (a *API) HandleStatus(w http.ResponseWriter, r *http.Request) {
	status := a.recorder.GetStatus()
	status["storage"] = a.storage.GetStorageInfo()

	go2cfg, err := config.LoadGo2RTCConfig(a.config.Go2RTCConfigPath)
	if err == nil {
		status["totalStreams"] = len(go2cfg.StreamOrder)

		if procs, ok := status["processes"].([]*recorder.StreamInfo); ok && len(procs) > 1 {
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
