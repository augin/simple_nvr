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
	writeJSON(w, http.StatusOK, a.config)
}

func (a *API) HandleSaveConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if !requireAdminRole(r) {
		writeError(w, http.StatusForbidden, "forbidden")
		return
	}

	var cfg config.NVRConfig
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}

	applyConfig(&cfg, a.config)

	if err := config.SaveNVRConfig(a.configPath, a.config); err != nil {
		log.Printf("Error saving config: %v", err)
		writeError(w, http.StatusInternalServerError, "save error")
		return
	}

	writeStatus(w, "ok")
}

// saveConfigQuietly persists the config, logging (but not failing) on error;
// used by state toggles where the operation already succeeded.
func saveConfigQuietly(path string, cfg *config.NVRConfig) {
	if err := config.SaveNVRConfig(path, cfg); err != nil {
		log.Printf("warning: save nvr config: %v", err)
	}
}

// applyConfig copies the editable settings from src into dst. Zero values
// never clobber live port numbers (a partially filled form must not stop the
// server); maps are only replaced when the request carries them.
func applyConfig(src, dst *config.NVRConfig) {
	dst.BaseDir = src.BaseDir
	dst.ArchiveDir = src.ArchiveDir
	dst.StreamServer = src.StreamServer
	dst.DefaultCameraLimitGB = src.DefaultCameraLimitGB
	dst.GlobalSizeGB = src.GlobalSizeGB
	dst.Go2RTCConfigPath = src.Go2RTCConfigPath
	if src.HTTPPort > 0 {
		dst.HTTPPort = src.HTTPPort
	}
	dst.AlarmEnabled = src.AlarmEnabled
	if src.AlarmPort > 0 {
		dst.AlarmPort = src.AlarmPort
	}
	dst.MQTTHost = src.MQTTHost
	if src.MQTTPort > 0 {
		dst.MQTTPort = src.MQTTPort
	}
	dst.MQTTUser = src.MQTTUser
	dst.MQTTPass = src.MQTTPass
	dst.AlarmCommand = src.AlarmCommand
	if src.CameraLimits != nil {
		dst.CameraLimits = src.CameraLimits
	}
	if src.CameraDayLimits != nil {
		dst.CameraDayLimits = src.CameraDayLimits
	}
	dst.KioskEnabled = src.KioskEnabled
	if src.KioskPort > 0 {
		dst.KioskPort = src.KioskPort
	}
	// Hikvision settings are managed exclusively through the alarm
	// start/stop endpoints and are deliberately left untouched here.
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

	writeJSON(w, http.StatusOK, status)
}

func (a *API) HandleCamerasStorage(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, a.storage.GetCamerasStorage())
}

func (a *API) HandleRecordStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if !requireAdminRole(r) {
		writeError(w, http.StatusForbidden, "forbidden")
		return
	}

	if a.recorder.IsRecording() {
		writeStatus(w, "already recording")
		return
	}

	go a.recorder.StartRecording()

	writeStatus(w, "started")
}

func (a *API) HandleRecordStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if !requireAdminRole(r) {
		writeError(w, http.StatusForbidden, "forbidden")
		return
	}

	a.recorder.StopRecording()

	writeStatus(w, "stopped")
}
