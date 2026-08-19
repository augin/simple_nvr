package api

import (
	"encoding/json"
	"net/http"
	"strconv"
)

func (a *API) HandleLogs(w http.ResponseWriter, r *http.Request) {
	limit := 500
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 {
			limit = n
		}
	}
	since := r.URL.Query().Get("since")
	level := r.URL.Query().Get("level")

	logs := a.logBuffer.GetLogs(limit, since, level)
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
