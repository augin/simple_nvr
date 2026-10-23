// Package api implements the HTTP API of the NVR: cameras, recordings,
// archive, config, users, alarms, logs and maintenance tools.
package api

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"simple_nvr/internal/alarm"
	"simple_nvr/internal/auth"
	"simple_nvr/internal/config"
	"simple_nvr/internal/go2rtc"
	"simple_nvr/internal/logs"
	"simple_nvr/internal/recorder"
	"simple_nvr/internal/storage"
)

// StreamController is the part of the recorder the API depends on. It exists
// so handler tests do not have to run real ffmpeg processes.
type StreamController interface {
	StartRecording(durations ...int)
	StopRecording()
	IsRecording() bool
	GetStatus() map[string]any
	ActiveRecordings() map[string]bool
	StartRecordingStream(name string)
	StopRecordingStream(name string)
}

// Options carries everything an API instance needs.
type Options struct {
	Config         *config.NVRConfig
	ConfigPath     string
	Recorder       StreamController
	Storage        *storage.Storage
	Alarm          *alarm.AlarmServer
	HikvisionAlarm *alarm.HikvisionAlarmServer
	LogBuffer      *logs.LogBuffer
	UserStore      *auth.UserStore
	Version        string
}

type API struct {
	config         *config.NVRConfig
	configPath     string
	recorder       StreamController
	storage        *storage.Storage
	alarm          *alarm.AlarmServer
	hikvisionAlarm *alarm.HikvisionAlarmServer
	logBuffer      *logs.LogBuffer
	userStore      *auth.UserStore
	go2rtc         *go2rtc.Client
	version        string
	toolsScan      scanState
}

func NewAPI(opts Options) *API {
	return &API{
		config:         opts.Config,
		configPath:     opts.ConfigPath,
		recorder:       opts.Recorder,
		storage:        opts.Storage,
		alarm:          opts.Alarm,
		hikvisionAlarm: opts.HikvisionAlarm,
		logBuffer:      opts.LogBuffer,
		userStore:      opts.UserStore,
		go2rtc:         go2rtc.NewClient(opts.Config.StreamServer),
		version:        opts.Version,
	}
}

// RegisterRoutes mounts every /api endpoint on mux.
func (a *API) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/cameras", a.HandleCameras)
	mux.HandleFunc("/api/files", a.HandleFiles)
	mux.HandleFunc("/api/video/", a.HandleVideo)
	mux.HandleFunc("/api/download", a.HandleVideoDownload)
	mux.HandleFunc("/api/archive/video/", a.HandleArchiveVideo)
	mux.HandleFunc("/api/archive/delete", a.HandleArchiveDelete)
	mux.HandleFunc("/api/archive", a.HandleArchive)
	mux.HandleFunc("/api/status", a.HandleStatus)
	mux.HandleFunc("/api/storage/cameras", a.HandleCamerasStorage)
	mux.HandleFunc("/api/record/start", a.HandleRecordStart)
	mux.HandleFunc("/api/record/stop", a.HandleRecordStop)
	mux.HandleFunc("/api/config", a.handleConfig)
	mux.HandleFunc("/api/version", a.HandleVersion)

	mux.HandleFunc("/api/alarm/status", a.HandleAlarmStatus)
	mux.HandleFunc("/api/alarm/start", a.HandleAlarmStart)
	mux.HandleFunc("/api/alarm/stop", a.HandleAlarmStop)
	mux.HandleFunc("/api/alarm/log", a.HandleAlarmLog)
	mux.HandleFunc("/api/alarm/clear", a.HandleAlarmClear)
	mux.HandleFunc("/api/alarms/range", a.HandleAlarmsRange)

	mux.HandleFunc("/api/hikvision/start", a.HandleHikvisionAlarmStart)
	mux.HandleFunc("/api/hikvision/stop", a.HandleHikvisionAlarmStop)

	mux.HandleFunc("/api/go2rtc/status", a.HandleGo2RTCStatus)
	mux.HandleFunc("/api/go2rtc/restart", a.HandleGo2RTCRestart)
	mux.HandleFunc("/api/go2rtc/update", a.HandleGo2RTCUpdate)
	mux.HandleFunc("/api/go2rtc/install", a.HandleGo2RTCInstall)
	mux.HandleFunc("/api/go2rtc/cameras", a.HandleGo2RTCCameras)
	mux.HandleFunc("/api/go2rtc/reorder", a.HandleGo2RTCReorder)

	mux.HandleFunc("/api/logs", a.HandleLogs)
	mux.HandleFunc("/api/logs/clear", a.HandleLogsClear)

	mux.HandleFunc("/api/tools/scan", a.HandleToolsScanStart)
	mux.HandleFunc("/api/tools/scan/status", a.HandleToolsScanStatus)
	mux.HandleFunc("/api/tools/repair", a.HandleToolsRepair)

	mux.HandleFunc("/api/auth/login", a.HandleLogin)
	mux.HandleFunc("/api/auth/logout", a.HandleLogout)
	mux.HandleFunc("/api/auth/me", a.HandleMe)
	mux.HandleFunc("/api/auth/check", a.HandleAuthCheck)
	mux.HandleFunc("/api/users", a.handleUsers)
	mux.HandleFunc("/api/users/change-password", a.HandleChangePassword)
}

func (a *API) handleConfig(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		a.HandleGetConfig(w, r)
	case http.MethodPost:
		a.HandleSaveConfig(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (a *API) handleUsers(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		auth.RequireAdmin(a.HandleGetUsers)(w, r)
	case http.MethodPost:
		a.HandleAddUser(w, r)
	case http.MethodDelete:
		auth.RequireAdmin(a.HandleDeleteUser)(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// HandleVersion reports the build version.
func (a *API) HandleVersion(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"version": a.version})
}

// writeJSON sends v as a JSON body with the given status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// writeError sends a machine-readable error object.
func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// writeStatus sends a {"status": ...} acknowledgement.
func writeStatus(w http.ResponseWriter, msg string) {
	writeJSON(w, http.StatusOK, map[string]string{"status": msg})
}

// safeJoin joins elements under base and rejects any path that would escape
// it. Elements may contain "/" separated segments (e.g. "2024/01/15"), but no
// segment may be empty, "." or "..".
func safeJoin(base string, elems ...string) (string, error) {
	var parts []string
	for _, elem := range elems {
		for _, seg := range strings.Split(filepath.ToSlash(elem), "/") {
			if seg == "" || seg == "." || seg == ".." ||
				strings.ContainsRune(seg, osPathSeparator) || strings.ContainsRune(seg, 0) {
				return "", fmt.Errorf("invalid path element %q", elem)
			}
			parts = append(parts, seg)
		}
	}
	joined := filepath.Join(append([]string{base}, parts...)...)
	// Defence in depth: the cleaned result must stay under base.
	if rel, err := filepath.Rel(filepath.Clean(base), joined); err != nil ||
		rel == ".." || strings.HasPrefix(rel, ".."+string(osPathSeparator)) {
		return "", fmt.Errorf("path %q escapes base %q", joined, base)
	}
	return joined, nil
}

// osPathSeparator is a local alias to keep safeJoin readable.
const osPathSeparator = filepath.Separator

func requireAdminRole(r *http.Request) bool {
	_, role, _ := auth.GetUserFromContext(r)
	return role == auth.RoleAdmin
}

func isRTSPReachable(rtspURL string) bool {
	u, err := url.Parse(rtspURL)
	if err != nil {
		return false
	}
	host := u.Hostname()
	if host == "" {
		host = u.Host
	}
	port := u.Port()
	if port == "" {
		port = go2rtc.DefaultRTSPPort
	}
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, port), 2*time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// ensure recorder and storage satisfy the wiring expected at startup.
var _ StreamController = (*recorder.Recorder)(nil)
