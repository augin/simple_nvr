package api

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

// Paths the self-update machinery writes to. Variables so tests can point
// them into a temp dir.
var (
	go2rtcBinaryPath = "/usr/bin/go2rtc"
	systemdUnitPath  = "/etc/systemd/system/go2rtc.service"
)

// githubLatestRelease fetches the newest go2rtc release metadata. Variable so
// tests do not hit the network.
var githubLatestRelease = func() (version, downloadURL string, ok bool) {
	resp, err := http.Get("https://api.github.com/repos/AlexxIT/go2rtc/releases/latest")
	if err != nil {
		return "", "", false
	}
	defer resp.Body.Close()

	var release struct {
		TagName string `json:"tag_name"`
		Assets  []struct {
			Name               string `json:"name"`
			BrowserDownloadURL string `json:"browser_download_url"`
		} `json:"assets"`
	}
	if json.NewDecoder(resp.Body).Decode(&release) != nil {
		return "", "", false
	}

	binaryName := "go2rtc_linux_" + runtime.GOARCH
	for _, asset := range release.Assets {
		if asset.Name == binaryName {
			return strings.TrimPrefix(release.TagName, "v"), asset.BrowserDownloadURL, true
		}
	}
	return strings.TrimPrefix(release.TagName, "v"), "", true
}

func (a *API) HandleGo2RTCStatus(w http.ResponseWriter, r *http.Request) {
	if !requireAdminRole(r) {
		writeError(w, http.StatusForbidden, "forbidden")
		return
	}

	binaryExists := false
	if _, err := os.Stat(go2rtcBinaryPath); err == nil {
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

		if info, err := a.go2rtc.Info(); err == nil {
			if v, ok := info["version"].(string); ok {
				status["version"] = v
			}
			if rtsp, ok := info["rtsp"].(map[string]any); ok {
				if listen, ok := rtsp["listen"].(string); ok {
					status["rtsp_listen"] = listen
				}
			}
		}
	}

	if !binaryExists && a.config.StreamServer != "" {
		if isRTSPReachable(a.config.StreamServer) {
			status["running"] = true
		}
	}

	if latestVersion, downloadURL, ok := githubLatestRelease(); ok {
		status["latest_version"] = latestVersion
		if downloadURL != "" {
			if !binaryExists {
				status["install_url"] = downloadURL
			}
			status["update_url"] = downloadURL
		}
		currentVersion := status["version"].(string)
		status["update_available"] = latestVersion != "" && currentVersion != "" && latestVersion != currentVersion
	}

	writeJSON(w, http.StatusOK, status)
}

// downloadTo fetches url and writes it to dest with mode 0755 via a temp file.
func downloadTo(url, dest string) error {
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("download failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download returned %d", resp.StatusCode)
	}

	tmpPath := dest + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("create temp: %w", err)
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("write temp: %w", err)
	}
	f.Close()
	os.Chmod(tmpPath, 0755)

	if err := os.Rename(tmpPath, dest); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("install binary: %w", err)
	}
	return nil
}

// ensureGo2RTCCreate creates an empty go2rtc config when none exists.
func (a *API) ensureGo2RTCCreate() string {
	configPath := a.config.Go2RTCConfigPath
	if configPath == "" {
		configPath = "/etc/go2rtc/go2rtc.yaml"
	}
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		if err := os.MkdirAll(filepath.Dir(configPath), 0755); err != nil {
			log.Printf("go2rtc install: mkdir: %v", err)
		}
		if err := os.WriteFile(configPath, []byte("streams: {}\n"), 0644); err != nil {
			log.Printf("go2rtc install: create config: %v", err)
		} else {
			log.Printf("go2rtc install: created empty config %s", configPath)
		}
	}
	return configPath
}

func (a *API) HandleGo2RTCInstall(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !requireAdminRole(r) {
		writeError(w, http.StatusForbidden, "forbidden")
		return
	}

	var req struct {
		URL string `json:"url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.URL == "" {
		writeError(w, http.StatusBadRequest, "url required")
		return
	}

	log.Printf("go2rtc install: downloading %s", req.URL)

	configPath := a.ensureGo2RTCCreate()

	if err := downloadTo(req.URL, go2rtcBinaryPath); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	systemdUnit := `[Unit]
Description=go2rtc
After=network.target

[Service]
ExecStart=` + go2rtcBinaryPath + ` -config ` + configPath + `
Restart=on-failure

[Install]
WantedBy=multi-user.target
`
	if err := os.WriteFile(systemdUnitPath, []byte(systemdUnit), 0644); err != nil {
		log.Printf("go2rtc install: write systemd unit: %v", err)
	}

	exec.Command("systemctl", "daemon-reload").Run()
	exec.Command("systemctl", "enable", "go2rtc").Run()
	exec.Command("systemctl", "start", "go2rtc").Run()

	log.Printf("go2rtc installed successfully")
	writeStatus(w, "installed")
}

func (a *API) HandleGo2RTCRestart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !requireAdminRole(r) {
		writeError(w, http.StatusForbidden, "forbidden")
		return
	}

	if err := a.go2rtc.Restart(); err != nil {
		log.Printf("go2rtc restart error: %v", err)
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeStatus(w, "restarted")
}

func (a *API) HandleGo2RTCUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !requireAdminRole(r) {
		writeError(w, http.StatusForbidden, "forbidden")
		return
	}

	var req struct {
		URL string `json:"url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.URL == "" {
		writeError(w, http.StatusBadRequest, "url required")
		return
	}

	log.Printf("go2rtc update: downloading %s", req.URL)

	exec.Command("systemctl", "stop", "go2rtc").Run()

	if err := downloadTo(req.URL, go2rtcBinaryPath); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		exec.Command("systemctl", "start", "go2rtc").Run()
		return
	}

	exec.Command("systemctl", "start", "go2rtc").Run()

	log.Printf("go2rtc updated successfully")
	writeStatus(w, "updated")
}
