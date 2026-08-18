package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strings"
)

func (a *API) HandleGo2RTCStatus(w http.ResponseWriter, r *http.Request) {
	if !requireAdminRole(r) {
		http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
		return
	}

	binaryExists := false
	if _, err := os.Stat("/usr/bin/go2rtc"); err == nil {
		binaryExists = true
	}

	status := map[string]any{
		"config_path":    a.config.Go2RTCConfigPath,
		"running":        false,
		"version":        "",
		"install_needed": !binaryExists,
	}

	if binaryExists {
		out, err := exec.Command("systemctl", "is-active", "go2rtc").Output()
		if err == nil && strings.TrimSpace(string(out)) == "active" {
			status["running"] = true
		}
	}

	if binaryExists {
		resp, err := http.Get(a.go2rtcAPIBase() + "/api")
		if err == nil {
			defer resp.Body.Close()
			var go2rtcInfo map[string]any
			if json.NewDecoder(resp.Body).Decode(&go2rtcInfo) == nil {
				if v, ok := go2rtcInfo["version"].(string); ok {
					status["version"] = v
				}
				if rtsp, ok := go2rtcInfo["rtsp"].(map[string]any); ok {
					if listen, ok := rtsp["listen"].(string); ok {
						status["rtsp_listen"] = listen
					}
				}
			}
		}
	}

	if !binaryExists && a.config.StreamServer != "" {
		if isRTSPReachable(a.config.StreamServer) {
			status["running"] = true
		}
	}

	ghResp, err := http.Get("https://api.github.com/repos/AlexxIT/go2rtc/releases/latest")
	if err == nil {
		defer ghResp.Body.Close()
		var release struct {
			TagName string `json:"tag_name"`
			Assets  []struct {
				Name               string `json:"name"`
				BrowserDownloadURL string `json:"browser_download_url"`
			} `json:"assets"`
		}
		if json.NewDecoder(ghResp.Body).Decode(&release) == nil {
			latestVersion := strings.TrimPrefix(release.TagName, "v")
			currentVersion := status["version"].(string)
			status["latest_version"] = latestVersion

			arch := runtime.GOARCH
			binaryName := "go2rtc_linux_" + arch
			for _, asset := range release.Assets {
				if asset.Name == binaryName {
					if !binaryExists {
						status["install_url"] = asset.BrowserDownloadURL
					}
					status["update_url"] = asset.BrowserDownloadURL
					break
				}
			}

			status["update_available"] = latestVersion != "" && currentVersion != "" && latestVersion != currentVersion
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

func (a *API) HandleGo2RTCInstall(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !requireAdminRole(r) {
		http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
		return
	}

	var req struct {
		URL string `json:"url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.URL == "" {
		http.Error(w, `{"error":"url required"}`, http.StatusBadRequest)
		return
	}

	log.Printf("go2rtc install: downloading %s", req.URL)

	httpResp, err := http.Get(req.URL)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"download failed: %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != 200 {
		http.Error(w, fmt.Sprintf(`{"error":"download returned %d"}`, httpResp.StatusCode), http.StatusInternalServerError)
		return
	}

	if err := os.MkdirAll("/etc/go2rtc", 0755); err != nil {
		log.Printf("go2rtc install: mkdir /etc/go2rtc: %v", err)
	}

	configPath := a.config.Go2RTCConfigPath
	if configPath == "" {
		configPath = "/etc/go2rtc/go2rtc.yaml"
	}
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		if err := os.WriteFile(configPath, []byte("streams: {}\n"), 0644); err != nil {
			log.Printf("go2rtc install: create config: %v", err)
		} else {
			log.Printf("go2rtc install: created empty config %s", configPath)
		}
	}

	tmpPath := "/usr/bin/go2rtc.tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"create temp: %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if _, err := io.Copy(f, httpResp.Body); err != nil {
		f.Close()
		os.Remove(tmpPath)
		http.Error(w, fmt.Sprintf(`{"error":"write temp: %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	f.Close()
	os.Chmod(tmpPath, 0755)

	if err := os.Rename(tmpPath, "/usr/bin/go2rtc"); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"install binary: %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	systemdUnit := `[Unit]
Description=go2rtc
After=network.target

[Service]
ExecStart=/usr/bin/go2rtc -config ` + configPath + `
Restart=on-failure

[Install]
WantedBy=multi-user.target
`
	if err := os.WriteFile("/etc/systemd/system/go2rtc.service", []byte(systemdUnit), 0644); err != nil {
		log.Printf("go2rtc install: write systemd unit: %v", err)
	}

	exec.Command("systemctl", "daemon-reload").Run()
	exec.Command("systemctl", "enable", "go2rtc").Run()
	exec.Command("systemctl", "start", "go2rtc").Run()

	log.Printf("go2rtc installed successfully")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "installed"})
}

func (a *API) HandleGo2RTCRestart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !requireAdminRole(r) {
		http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
		return
	}

	if err := a.restartGo2RTC(); err != nil {
		log.Printf("go2rtc restart error: %v", err)
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "restarted"})
}

func (a *API) HandleGo2RTCUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !requireAdminRole(r) {
		http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
		return
	}

	var req struct {
		URL string `json:"url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.URL == "" {
		http.Error(w, `{"error":"url required"}`, http.StatusBadRequest)
		return
	}

	log.Printf("go2rtc update: downloading %s", req.URL)

	httpResp, err := http.Get(req.URL)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"download failed: %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != 200 {
		http.Error(w, fmt.Sprintf(`{"error":"download returned %d"}`, httpResp.StatusCode), http.StatusInternalServerError)
		return
	}

	tmpPath := "/usr/bin/go2rtc.tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"create temp: %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if _, err := io.Copy(f, httpResp.Body); err != nil {
		f.Close()
		os.Remove(tmpPath)
		http.Error(w, fmt.Sprintf(`{"error":"write temp: %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	f.Close()
	os.Chmod(tmpPath, 0755)

	exec.Command("systemctl", "stop", "go2rtc").Run()

	if err := os.Rename(tmpPath, "/usr/bin/go2rtc"); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"replace binary: %s"}`, err.Error()), http.StatusInternalServerError)
		exec.Command("systemctl", "start", "go2rtc").Run()
		return
	}

	exec.Command("systemctl", "start", "go2rtc").Run()

	log.Printf("go2rtc updated successfully")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "updated"})
}
