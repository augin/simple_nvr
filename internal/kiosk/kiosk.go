// Package kiosk serves a stripped-down, auth-less mirror of the UI for a
// local display (kiosk tablet): static assets and the index page are served
// directly, every other path is proxied to the main HTTP server.
package kiosk

import (
	"log"
	"net/http"
	"net/http/httputil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"simple_nvr/internal/config"
)

// DefaultAssetRoot is where the deb package installs static files.
const DefaultAssetRoot = "/usr/share/simple-nvr"

type KioskServer struct {
	config   *config.NVRConfig
	mainAddr string
	version  string

	// Asset paths; overridable in tests.
	staticPath   string
	templatePath string
}

func NewKioskServer(cfg *config.NVRConfig, ver string) *KioskServer {
	return &KioskServer{
		config:       cfg,
		mainAddr:     "http://127.0.0.1:" + strconv.Itoa(cfg.HTTPPort),
		version:      ver,
		staticPath:   filepath.Join(DefaultAssetRoot, "static"),
		templatePath: filepath.Join(DefaultAssetRoot, "templates", "index.html"),
	}
}

// Handler builds the kiosk mux: static files, the injected index page, a
// permissive auth check and a reverse proxy to the main server.
func (ks *KioskServer) Handler() http.Handler {
	proxy := &httputil.ReverseProxy{
		Director: func(r *http.Request) {
			r.URL.Scheme = "http"
			r.URL.Host = strings.TrimPrefix(ks.mainAddr, "http://")
			r.Header.Set("X-Kiosk-Proxy", "1")
		},
	}

	mux := http.NewServeMux()

	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir(ks.staticPath))))

	mux.HandleFunc("/favicon.ico", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, filepath.Join(ks.staticPath, "favicon.ico"))
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/":
			raw, err := os.ReadFile(ks.templatePath)
			if err != nil {
				http.Error(w, "template error", http.StatusInternalServerError)
				return
			}
			page := string(raw)
			injected := strings.Replace(page,
				"<head>",
				"<head>\n<script>window.__kioskMode=true;window.__kioskRole='user';</script>", 1)
			injected = strings.ReplaceAll(injected, "{{VERSION}}", ks.version)
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.Write([]byte(injected))
		case "/api/auth/check":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"auth_required":false}`))
		default:
			proxy.ServeHTTP(w, r)
		}
	})

	return mux
}

func (ks *KioskServer) Start() {
	if !ks.config.KioskEnabled {
		return
	}

	addr := ":" + strconv.Itoa(ks.config.KioskPort)
	log.Printf("Kiosk server starting on %s", addr)
	go func() {
		if err := http.ListenAndServe(addr, ks.Handler()); err != nil {
			log.Printf("Kiosk server error: %v", err)
		}
	}()
}
