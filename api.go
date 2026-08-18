package main

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"
)

type API struct {
	config         *NVRConfig
	configPath     string
	recorder       *Recorder
	storage        *Storage
	alarm          *AlarmServer
	hikvisionAlarm *HikvisionAlarmServer
	logBuffer      *LogBuffer
	userStore      *UserStore
	toolsScan      scanState
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
	_, role, _ := GetUserFromContext(r)
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
