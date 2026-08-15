package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

var version = "2.10.26"

func findStaticDir() string {
	exe, err := os.Executable()
	if err != nil {
		return "."
	}
	dir := filepath.Dir(exe)
	if _, err := os.Stat(filepath.Join(dir, "templates", "index.html")); err == nil {
		return dir
	}
	if _, err := os.Stat(filepath.Join(dir, "..", "templates", "index.html")); err == nil {
		return filepath.Join(dir, "..")
	}
	return "."
}

func findConfig() string {
	exe, err := os.Executable()
	if err != nil {
		return "/etc/simple-nvr/nvr.yaml"
	}
	dir := filepath.Dir(exe)
	if _, err := os.Stat(filepath.Join(dir, "nvr.yaml")); err == nil {
		return filepath.Join(dir, "nvr.yaml")
	}
	if _, err := os.Stat(filepath.Join(dir, "..", "nvr.yaml")); err == nil {
		return filepath.Join(dir, "..", "nvr.yaml")
	}
	return "/etc/simple-nvr/nvr.yaml"
}

func main() {
	configPath := flag.String("config", "", "path to config file")
	staticDir := flag.String("static-dir", "", "path to static files directory")
	flag.Parse()

	if *staticDir == "" {
		*staticDir = findStaticDir()
	}
	if *configPath == "" {
		*configPath = findConfig()
	}

	config, err := loadNVRConfig(*configPath)
	if err != nil {
		log.Fatalf("Error loading config: %v", err)
	}

	log.Printf("Simple NVR starting...")
	log.Printf("Config: %s", *configPath)
	log.Printf("Static: %s", *staticDir)
	log.Printf("Base dir: %s", config.BaseDir)
	log.Printf("Stream server: %s", config.StreamServer)
	log.Printf("Default camera limit: %d GB, Global limit: %d GB", config.DefaultCameraLimitGB, config.GlobalSizeGB)

	var ipMap map[string]string
	go2cfg, err := loadGo2RTCConfig(config.Go2RTCConfigPath)
	if err != nil {
		log.Printf("Warning: could not load go2rtc config: %v", err)
	} else {
		ipMap = go2cfg.IPMap
		log.Printf("IP camera map: %d entries", len(ipMap))
	}

	recorder := NewRecorder(config)
	storage := NewStorage(config)
	alarm := NewAlarmServer(config, ipMap)
	hikvisionAlarm := NewHikvisionAlarmServer(config, alarm, ipMap)
	logBuffer := NewLogBuffer(1000)
	RedirectLogOutput(logBuffer)
	userStore := NewUserStore(config.UsersFile)
	api := NewAPI(config, *configPath, recorder, storage, alarm, hikvisionAlarm, logBuffer, userStore)

	alarm.LoadRecentEvents(7)

	go startScheduler(recorder)

	if config.AlarmEnabled {
		if err := alarm.Start(); err != nil {
			log.Printf("Warning: failed to start alarm server: %v", err)
		}
	}

	if config.HikvisionEnabled {
		if err := hikvisionAlarm.Start(); err != nil {
			log.Printf("Warning: failed to start hikvision alarm server: %v", err)
		}
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		log.Printf("Received %v, shutting down...", sig)
		alarm.Stop()
		hikvisionAlarm.Stop()
		recorder.StopRecording()
		os.Exit(0)
	}()

	templatePath := filepath.Join(*staticDir, "templates", "index.html")
	staticPath := filepath.Join(*staticDir, "static")

	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		data, err := os.ReadFile(templatePath)
		if err != nil {
			http.Error(w, "template error", http.StatusInternalServerError)
			return
		}
		html := strings.ReplaceAll(string(data), "{{VERSION}}", version)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(html))
	})

	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir(staticPath))))

	mux.HandleFunc("/favicon.ico", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, filepath.Join(staticPath, "favicon.ico"))
	})

	mux.HandleFunc("/api/cameras", api.HandleCameras)
	mux.HandleFunc("/api/files", api.HandleFiles)
	mux.HandleFunc("/api/video/", api.HandleVideo)
	mux.HandleFunc("/api/download", api.HandleVideoDownload)
	mux.HandleFunc("/api/archive/video/", api.HandleArchiveVideo)
	mux.HandleFunc("/api/archive/delete", api.HandleArchiveDelete)
	mux.HandleFunc("/api/archive", api.HandleArchive)
	mux.HandleFunc("/api/status", api.HandleStatus)
	mux.HandleFunc("/api/storage/cameras", api.HandleCamerasStorage)
	mux.HandleFunc("/api/record/start", api.HandleRecordStart)
	mux.HandleFunc("/api/record/stop", api.HandleRecordStop)
	mux.HandleFunc("/api/config", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			api.HandleGetConfig(w, r)
		case http.MethodPost:
			api.HandleSaveConfig(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/alarm/status", api.HandleAlarmStatus)
	mux.HandleFunc("/api/alarm/start", api.HandleAlarmStart)
	mux.HandleFunc("/api/alarm/stop", api.HandleAlarmStop)
	mux.HandleFunc("/api/alarm/log", api.HandleAlarmLog)
	mux.HandleFunc("/api/alarm/clear", api.HandleAlarmClear)
	mux.HandleFunc("/api/alarms/range", api.HandleAlarmsRange)

	mux.HandleFunc("/api/hikvision/start", api.HandleHikvisionAlarmStart)
	mux.HandleFunc("/api/hikvision/stop", api.HandleHikvisionAlarmStop)

	mux.HandleFunc("/api/go2rtc/status", api.HandleGo2RTCStatus)
	mux.HandleFunc("/api/go2rtc/restart", api.HandleGo2RTCRestart)
	mux.HandleFunc("/api/go2rtc/update", api.HandleGo2RTCUpdate)
	mux.HandleFunc("/api/go2rtc/install", api.HandleGo2RTCInstall)
	mux.HandleFunc("/api/go2rtc/cameras", api.HandleGo2RTCCameras)
	mux.HandleFunc("/api/go2rtc/reorder", api.HandleGo2RTCReorder)

	mux.HandleFunc("/api/logs", api.HandleLogs)
	mux.HandleFunc("/api/logs/clear", api.HandleLogsClear)

	mux.HandleFunc("/api/auth/login", api.HandleLogin)
	mux.HandleFunc("/api/auth/logout", api.HandleLogout)
	mux.HandleFunc("/api/auth/me", api.HandleMe)
	mux.HandleFunc("/api/auth/check", api.HandleAuthCheck)
	mux.HandleFunc("/api/users", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			RequireAdmin(api.HandleGetUsers)(w, r)
		case http.MethodPost:
			api.HandleAddUser(w, r)
		case http.MethodDelete:
			RequireAdmin(api.HandleDeleteUser)(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/api/users/change-password", api.HandleChangePassword)

	mux.HandleFunc("/api/version", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"version": version})
	})

	addr := fmt.Sprintf(":%d", config.HTTPPort)
	log.Printf("Server starting on %s", addr)
	log.Printf("Users file: %s", config.UsersFile)

	if config.KioskEnabled {
		kiosk := NewKioskServer(config)
		kiosk.Start()
		log.Printf("Kiosk mode enabled on port %d", config.KioskPort)
	}

	log.Fatal(http.ListenAndServe(addr, userStore.RequireAuth(mux)))
}

func startScheduler(recorder *Recorder) {
	now := time.Now()
	minute := now.Minute()
	second := now.Second()
	nextInterval := ((minute / 10) + 1) * 10
	remaining := (nextInterval - minute) * 60 - second

	log.Printf("First recording now: %ds until :%02d:00 (+7s overlap)", remaining, nextInterval%60)
	recorder.StartRecording(remaining + 7)

	for {
		now := time.Now()
		nextMinute := ((now.Minute() / 10) + 1) * 10
		var nextTick time.Time
		if nextMinute >= 60 {
			nextTick = time.Date(now.Year(), now.Month(), now.Day(), now.Hour()+1, 0, 0, 0, now.Location())
		} else {
			nextTick = time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), nextMinute, 0, 0, now.Location())
		}
		log.Printf("Next recording cycle at %s (in %v)", nextTick.Format("15:04:05"), nextTick.Sub(now).Round(time.Second))
		time.Sleep(nextTick.Sub(now))

		storage := NewStorage(recorder.config)
		storage.CleanCameraFolders()
		recorder.StartRecording(607)
	}
}
