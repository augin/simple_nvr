package main

import (
	"log"
	"net/http"
	"net/http/httputil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type KioskServer struct {
	config   *NVRConfig
	mainAddr string
}

func NewKioskServer(config *NVRConfig) *KioskServer {
	return &KioskServer{
		config:   config,
		mainAddr: "http://127.0.0.1:" + strconv.Itoa(config.HTTPPort),
	}
}

func (ks *KioskServer) Start() {
	if !ks.config.KioskEnabled {
		return
	}

	staticPath := filepath.Join("/usr/share/simple-nvr", "static")
	templatePath := filepath.Join("/usr/share/simple-nvr", "templates", "index.html")

	proxy := &httputil.ReverseProxy{
		Director: func(r *http.Request) {
			r.URL.Scheme = "http"
			r.URL.Host = "127.0.0.1:" + strconv.Itoa(ks.config.HTTPPort)
			r.Header.Set("X-Kiosk-Proxy", "1")
		},
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		raw, err := readFile(templatePath)
		if err != nil {
			http.Error(w, "template error", 500)
			return
		}
		injected := strings.Replace(raw,
			"<head>",
			"<head>\n<script>window.__kioskMode=true;window.__kioskRole='user';</script>", 1)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(injected))
	})

	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir(staticPath))))

	mux.HandleFunc("/favicon.ico", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, filepath.Join(staticPath, "favicon.ico"))
	})

	mux.HandleFunc("/api/", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path

		if path == "/api/auth/check" {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"auth_required":false}`))
			return
		}
		if path == "/api/version" {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"version":"kiosk"}`))
			return
		}

		proxy.ServeHTTP(w, r)
	})

	addr := ":" + strconv.Itoa(ks.config.KioskPort)
	log.Printf("Kiosk server starting on %s", addr)
	go func() {
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Printf("Kiosk server error: %v", err)
		}
	}()
}

func readFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
