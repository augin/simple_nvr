package api

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"strconv"

	"simple_nvr/internal/alarm"
	"simple_nvr/internal/config"
)

func (a *API) HandleAlarmStatus(w http.ResponseWriter, r *http.Request) {
	dahuaStatus := a.alarm.GetStatus()
	hikvisionStatus := a.hikvisionAlarm.GetStatus()

	status := map[string]any{
		"dahua":       dahuaStatus,
		"hikvision":   hikvisionStatus,
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
	config.SaveNVRConfig(a.configPath, a.config)

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
	config.SaveNVRConfig(a.configPath, a.config)

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
	config.SaveNVRConfig(a.configPath, a.config)

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
	config.SaveNVRConfig(a.configPath, a.config)

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

	filePath := filepath.Join(alarm.AlarmDir, date+".jsonl")
	events, err := alarm.ReadEventsFile(filePath)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]alarm.AlarmEvent{})
		return
	}

	filtered := make([]alarm.AlarmEvent, 0)
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
