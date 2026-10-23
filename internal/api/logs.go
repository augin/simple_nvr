package api

import (
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

	writeJSON(w, http.StatusOK, a.logBuffer.GetLogs(limit, since, level))
}

func (a *API) HandleLogsClear(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if !requireAdminRole(r) {
		writeError(w, http.StatusForbidden, "forbidden")
		return
	}

	a.logBuffer.Clear()

	writeStatus(w, "cleared")
}
