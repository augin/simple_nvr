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

	"simple_nvr/internal/alarm"
	"simple_nvr/internal/api"
	"simple_nvr/internal/auth"
	"simple_nvr/internal/config"
	"simple_nvr/internal/kiosk"
	"simple_nvr/internal/logs"
	"simple_nvr/internal/recorder"
	"simple_nvr/internal/storage"
)

var version = "2.13.10"

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

	cfg, err := config.LoadNVRConfig(*configPath)
	if err != nil {
		log.Fatalf("Error loading config: %v", err)
	}

	log.Printf("Simple NVR starting...")
	log.Printf("Config: %s", *configPath)
	log.Printf("Static: %s", *staticDir)
	log.Printf("Base dir: %s", cfg.BaseDir)
	log.Printf("Stream server: %s", cfg.StreamServer)
	log.Printf("Default camera limit: %d GB, Global limit: %d GB", cfg.DefaultCameraLimitGB, cfg.GlobalSizeGB)

	var ipMap map[string]string
	go2cfg, err := config.LoadGo2RTCConfig(cfg.Go2RTCConfigPath)
	if err != nil {
		log.Printf("Warning: could not load go2rtc config: %v", err)
	} else {
		ipMap = go2cfg.IPMap
		log.Printf("IP camera map: %d entries", len(ipMap))
	}

	rec := recorder.NewRecorder(cfg)
	stor := storage.NewStorage(cfg)
	alarmSrv := alarm.NewAlarmServer(cfg, ipMap)
	hikvisionAlarm := alarm.NewHikvisionAlarmServer(cfg, alarmSrv, ipMap)
	logBuffer := logs.NewLogBuffer(1000)
	logs.RedirectLogOutput(logBuffer)
	userStore := auth.NewUserStore(cfg.UsersFile)
	apiSrv := api.NewAPI(cfg, *configPath, rec, stor, alarmSrv, hikvisionAlarm, logBuffer, userStore)

	alarmSrv.LoadRecentEvents(7)

	go startScheduler(rec)

	if cfg.AlarmEnabled {
		if err := alarmSrv.Start(); err != nil {
			log.Printf("Warning: failed to start alarm server: %v", err)
		}
	}

	if cfg.HikvisionEnabled {
		if err := hikvisionAlarm.Start(); err != nil {
			log.Printf("Warning: failed to start hikvision alarm server: %v", err)
		}
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		log.Printf("Received %v, shutting down...", sig)
		alarmSrv.Stop()
		hikvisionAlarm.Stop()
		rec.StopRecording()
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

	mux.HandleFunc("/api/cameras", apiSrv.HandleCameras)
	mux.HandleFunc("/api/files", apiSrv.HandleFiles)
	mux.HandleFunc("/api/video/", apiSrv.HandleVideo)
	mux.HandleFunc("/api/download", apiSrv.HandleVideoDownload)
	mux.HandleFunc("/api/archive/video/", apiSrv.HandleArchiveVideo)
	mux.HandleFunc("/api/archive/delete", apiSrv.HandleArchiveDelete)
	mux.HandleFunc("/api/archive", apiSrv.HandleArchive)
	mux.HandleFunc("/api/status", apiSrv.HandleStatus)
	mux.HandleFunc("/api/storage/cameras", apiSrv.HandleCamerasStorage)
	mux.HandleFunc("/api/record/start", apiSrv.HandleRecordStart)
	mux.HandleFunc("/api/record/stop", apiSrv.HandleRecordStop)
	mux.HandleFunc("/api/config", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			apiSrv.HandleGetConfig(w, r)
		case http.MethodPost:
			apiSrv.HandleSaveConfig(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/alarm/status", apiSrv.HandleAlarmStatus)
	mux.HandleFunc("/api/alarm/start", apiSrv.HandleAlarmStart)
	mux.HandleFunc("/api/alarm/stop", apiSrv.HandleAlarmStop)
	mux.HandleFunc("/api/alarm/log", apiSrv.HandleAlarmLog)
	mux.HandleFunc("/api/alarm/clear", apiSrv.HandleAlarmClear)
	mux.HandleFunc("/api/alarms/range", apiSrv.HandleAlarmsRange)

	mux.HandleFunc("/api/hikvision/start", apiSrv.HandleHikvisionAlarmStart)
	mux.HandleFunc("/api/hikvision/stop", apiSrv.HandleHikvisionAlarmStop)

	mux.HandleFunc("/api/go2rtc/status", apiSrv.HandleGo2RTCStatus)
	mux.HandleFunc("/api/go2rtc/restart", apiSrv.HandleGo2RTCRestart)
	mux.HandleFunc("/api/go2rtc/update", apiSrv.HandleGo2RTCUpdate)
	mux.HandleFunc("/api/go2rtc/install", apiSrv.HandleGo2RTCInstall)
	mux.HandleFunc("/api/go2rtc/cameras", apiSrv.HandleGo2RTCCameras)
	mux.HandleFunc("/api/go2rtc/reorder", apiSrv.HandleGo2RTCReorder)

	mux.HandleFunc("/api/logs", apiSrv.HandleLogs)
	mux.HandleFunc("/api/logs/clear", apiSrv.HandleLogsClear)

	mux.HandleFunc("/api/tools/scan", apiSrv.HandleToolsScanStart)
	mux.HandleFunc("/api/tools/scan/status", apiSrv.HandleToolsScanStatus)
	mux.HandleFunc("/api/tools/repair", apiSrv.HandleToolsRepair)

	mux.HandleFunc("/api/auth/login", apiSrv.HandleLogin)
	mux.HandleFunc("/api/auth/logout", apiSrv.HandleLogout)
	mux.HandleFunc("/api/auth/me", apiSrv.HandleMe)
	mux.HandleFunc("/api/auth/check", apiSrv.HandleAuthCheck)
	mux.HandleFunc("/api/users", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			auth.RequireAdmin(apiSrv.HandleGetUsers)(w, r)
		case http.MethodPost:
			apiSrv.HandleAddUser(w, r)
		case http.MethodDelete:
			auth.RequireAdmin(apiSrv.HandleDeleteUser)(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/api/users/change-password", apiSrv.HandleChangePassword)

	mux.HandleFunc("/api/version", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"version": version})
	})

	addr := fmt.Sprintf(":%d", cfg.HTTPPort)
	log.Printf("Server starting on %s", addr)
	log.Printf("Users file: %s", cfg.UsersFile)

	if cfg.KioskEnabled {
		k := kiosk.NewKioskServer(cfg, version)
		k.Start()
		log.Printf("Kiosk mode enabled on port %d", cfg.KioskPort)
	}

	log.Fatal(http.ListenAndServe(addr, userStore.RequireAuth(mux)))
}

func startScheduler(rec *recorder.Recorder) {
	now := time.Now()
	minute := now.Minute()
	second := now.Second()
	nextInterval := ((minute / 10) + 1) * 10
	remaining := (nextInterval - minute) * 60 - second

	log.Printf("First recording now: %ds until :%02d:00 (+7s overlap)", remaining, nextInterval%60)
	rec.StartRecording(remaining + 7)

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

		go func() {
			s := storage.NewStorage(rec.Config())
			s.CleanCameraFolders()
		}()
		rec.StartRecordingScheduled(nextTick, 607)
	}
}
