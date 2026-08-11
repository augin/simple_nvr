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
	"syscall"
	"time"
)

var version = "dev"

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
	api := NewAPI(config, *configPath, recorder, storage, alarm)

	go startScheduler(recorder)

	if config.AlarmEnabled {
		if err := alarm.Start(); err != nil {
			log.Printf("Warning: failed to start alarm server: %v", err)
		}
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		log.Printf("Received %v, shutting down...", sig)
		alarm.Stop()
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
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		http.ServeFile(w, r, templatePath)
	})

	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir(staticPath))))

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

	mux.HandleFunc("/api/version", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"version": version})
	})

	addr := fmt.Sprintf(":%d", config.HTTPPort)
	log.Printf("Server starting on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func startScheduler(recorder *Recorder) {
	now := time.Now()
	minute := now.Minute()
	second := now.Second()
	nextInterval := ((minute / 10) + 1) * 10
	remaining := (nextInterval - minute) * 60 - second

	log.Printf("First recording now: %ds until :%02d:00 (+7s overlap)", remaining+7, nextInterval%60)
	recorder.StartRecording(remaining + 7)
	time.Sleep(time.Duration(remaining) * time.Second)

	for {
		storage := NewStorage(recorder.config)
		storage.CleanCameraFolders()
		recorder.StartRecording(607)
		time.Sleep(10 * time.Minute)
	}
}
