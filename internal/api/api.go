package api

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"

	"simple_nvr/internal/alarm"
	"simple_nvr/internal/auth"
	"simple_nvr/internal/config"
	"simple_nvr/internal/logs"
	"simple_nvr/internal/recorder"
	"simple_nvr/internal/storage"
)

type API struct {
	config         *config.NVRConfig
	configPath     string
	recorder       *recorder.Recorder
	storage        *storage.Storage
	alarm          *alarm.AlarmServer
	hikvisionAlarm *alarm.HikvisionAlarmServer
	logBuffer      *logs.LogBuffer
	userStore      *auth.UserStore
	toolsScan      scanState
}

func NewAPI(cfg *config.NVRConfig, configPath string, rec *recorder.Recorder, stor *storage.Storage, alarmSrv *alarm.AlarmServer, hikvisionAlarm *alarm.HikvisionAlarmServer, logBuf *logs.LogBuffer, userSt *auth.UserStore) *API {
	return &API{
		config:         cfg,
		configPath:     configPath,
		recorder:       rec,
		storage:        stor,
		alarm:          alarmSrv,
		hikvisionAlarm: hikvisionAlarm,
		logBuffer:      logBuf,
		userStore:      userSt,
	}
}

func (a *API) go2rtcAPIBase() string {
	if a.config.StreamServer != "" {
		if u, err := url.Parse(a.config.StreamServer); err == nil && u.Hostname() != "" {
			return "http://" + u.Hostname() + ":1984"
		}
	}
	return "http://localhost:1984"
}

func (a *API) restartGo2RTC() error {
	resp, err := http.Post(a.go2rtcAPIBase()+"/api/restart", "", nil)
	if err != nil {
		return fmt.Errorf("go2rtc restart failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("go2rtc restart returned status %d", resp.StatusCode)
	}
	return nil
}

func requireAdminRole(r *http.Request) bool {
	_, role, _ := auth.GetUserFromContext(r)
	return role == "admin"
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
		port = "554"
	}
	conn, err := net.DialTimeout("tcp", host+":"+port, 2*time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}
