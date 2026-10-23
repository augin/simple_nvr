package api

import (
	"net/http"
	"path/filepath"
	"strconv"
	"time"

	"simple_nvr/internal/alarm"
)

// alarmDateLayout is the on-disk journal file layout (YYYY-MM-DD.jsonl).
const alarmDateLayout = "2006-01-02"

func (a *API) HandleAlarmStatus(w http.ResponseWriter, r *http.Request) {
	dahuaStatus := a.alarm.GetStatus()
	hikvisionStatus := a.hikvisionAlarm.GetStatus()

	writeJSON(w, http.StatusOK, map[string]any{
		"dahua":       dahuaStatus,
		"hikvision":   hikvisionStatus,
		"event_count": dahuaStatus["event_count"],
	})
}

func (a *API) HandleAlarmStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if !requireAdminRole(r) {
		writeError(w, http.StatusForbidden, "forbidden")
		return
	}

	if err := a.alarm.Start(); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	a.config.AlarmEnabled = true
	saveConfigQuietly(a.configPath, a.config)

	writeStatus(w, "started")
}

func (a *API) HandleAlarmStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if !requireAdminRole(r) {
		writeError(w, http.StatusForbidden, "forbidden")
		return
	}

	a.alarm.Stop()

	a.config.AlarmEnabled = false
	saveConfigQuietly(a.configPath, a.config)

	writeStatus(w, "stopped")
}

func (a *API) HandleHikvisionAlarmStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if !requireAdminRole(r) {
		writeError(w, http.StatusForbidden, "forbidden")
		return
	}

	if err := a.hikvisionAlarm.Start(); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	a.config.HikvisionEnabled = true
	saveConfigQuietly(a.configPath, a.config)

	writeStatus(w, "started")
}

func (a *API) HandleHikvisionAlarmStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if !requireAdminRole(r) {
		writeError(w, http.StatusForbidden, "forbidden")
		return
	}

	a.hikvisionAlarm.Stop()

	a.config.HikvisionEnabled = false
	saveConfigQuietly(a.configPath, a.config)

	writeStatus(w, "stopped")
}

func (a *API) HandleAlarmLog(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 {
			limit = n
		}
	}

	writeJSON(w, http.StatusOK, a.alarm.GetLog(limit))
}

func (a *API) HandleAlarmsRange(w http.ResponseWriter, r *http.Request) {
	camera := r.URL.Query().Get("camera")
	date := r.URL.Query().Get("date")
	if camera == "" || date == "" {
		writeError(w, http.StatusBadRequest, "camera and date required")
		return
	}

	// The date becomes a file name; anything but a plain YYYY-MM-DD would
	// let a caller walk out of the journal directory.
	if _, err := time.Parse(alarmDateLayout, date); err != nil {
		writeError(w, http.StatusBadRequest, "invalid date")
		return
	}

	filePath := filepath.Join(a.alarm.EventDir(), date+".jsonl")
	events, err := alarm.ReadEventsFile(filePath)
	if err != nil {
		writeJSON(w, http.StatusOK, []alarm.AlarmEvent{})
		return
	}

	filtered := make([]alarm.AlarmEvent, 0)
	for _, e := range events {
		if e.Camera == camera {
			filtered = append(filtered, e)
		}
	}

	writeJSON(w, http.StatusOK, filtered)
}

func (a *API) HandleAlarmClear(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if !requireAdminRole(r) {
		writeError(w, http.StatusForbidden, "forbidden")
		return
	}

	a.alarm.ClearLog()

	writeStatus(w, "cleared")
}
