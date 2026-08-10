package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	configPath := flag.String("config", "nvr.yaml", "path to config file")
	flag.Parse()

	config, err := loadNVRConfig(*configPath)
	if err != nil {
		log.Fatalf("Error loading config: %v", err)
	}

	log.Printf("Simple NVR starting...")
	log.Printf("Base dir: %s", config.BaseDir)
	log.Printf("Stream server: %s", config.StreamServer)
	log.Printf("Target size: %d GB", config.TargetSizeGB)

	recorder := NewRecorder(config)
	storage := NewStorage(config)
	api := NewAPI(config, recorder, storage)

	go startScheduler(recorder)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		log.Printf("Received %v, shutting down...", sig)
		recorder.StopRecording()
		os.Exit(0)
	}()

	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		http.ServeFile(w, r, "templates/index.html")
	})

	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("static"))))

	mux.HandleFunc("/api/cameras", api.HandleCameras)
	mux.HandleFunc("/api/files", api.HandleFiles)
	mux.HandleFunc("/api/video/", api.HandleVideo)
	mux.HandleFunc("/api/download", api.HandleVideoDownload)
	mux.HandleFunc("/api/archive/video/", api.HandleArchiveVideo)
	mux.HandleFunc("/api/archive/delete", api.HandleArchiveDelete)
	mux.HandleFunc("/api/archive", api.HandleArchive)
	mux.HandleFunc("/api/status", api.HandleStatus)
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
		recorder.StartRecording(607)
		time.Sleep(10 * time.Minute)
	}
}
