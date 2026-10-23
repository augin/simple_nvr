package main

import (
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

var version = "2.14.0"

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

// registerUI mounts the index page, static assets and favicon on mux.
func registerUI(mux *http.ServeMux, staticDir string) {
	templatePath := filepath.Join(staticDir, "templates", "index.html")
	staticPath := filepath.Join(staticDir, "static")

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
	if err := userStore.Err(); err != nil {
		// A corrupt users file must not silently disable authentication.
		log.Fatalf("Users file: %v", err)
	}

	apiSrv := api.NewAPI(api.Options{
		Config:         cfg,
		ConfigPath:     *configPath,
		Recorder:       rec,
		Storage:        stor,
		Alarm:          alarmSrv,
		HikvisionAlarm: hikvisionAlarm,
		LogBuffer:      logBuffer,
		UserStore:      userStore,
		Version:        version,
	})

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
		log.Printf("Shutdown complete")
		os.Exit(0)
	}()

	mux := http.NewServeMux()
	registerUI(mux, *staticDir)
	apiSrv.RegisterRoutes(mux)

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

// nextCycleTick returns the next 10-minute mark at or after now.
func nextCycleTick(now time.Time) time.Time {
	nextMinute := ((now.Minute() / 10) + 1) * 10
	if nextMinute >= 60 {
		return time.Date(now.Year(), now.Month(), now.Day(), now.Hour()+1, 0, 0, 0, now.Location())
	}
	return time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), nextMinute, 0, 0, now.Location())
}

func startScheduler(rec *recorder.Recorder) {
	now := time.Now()
	nextTick := nextCycleTick(now)
	// Whole seconds until the tick, matching the historic formula.
	remaining := int(nextTick.Sub(now).Round(time.Second).Seconds())

	log.Printf("First recording now: %ds until %s (+7s overlap)", remaining, nextTick.Format("15:04:00"))
	rec.StartRecording(remaining + 7)

	for {
		now := time.Now()
		nextTick := nextCycleTick(now)
		log.Printf("Next recording cycle at %s (in %v)", nextTick.Format("15:04:05"), nextTick.Sub(now).Round(time.Second))
		time.Sleep(nextTick.Sub(now))

		go func() {
			s := storage.NewStorage(rec.Config())
			s.CleanCameraFolders()
		}()
		rec.StartRecordingScheduled(nextTick, 607)
	}
}
