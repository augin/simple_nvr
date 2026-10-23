package api

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"simple_nvr/internal/config"
	"simple_nvr/internal/go2rtc"
)

func (a *API) HandleCameras(w http.ResponseWriter, r *http.Request) {
	go2cfg, err := config.LoadGo2RTCConfig(a.config.Go2RTCConfigPath)
	if err != nil {
		log.Printf("Warning: could not load go2rtc config %s: %v", a.config.Go2RTCConfigPath, err)
		writeJSON(w, http.StatusOK, map[string]any{"cameras": []string{}, "error": err.Error()})
		return
	}

	cameras := make([]string, 0, len(go2cfg.Streams))
	for _, name := range go2cfg.StreamOrder {
		cameras = append(cameras, name)
	}

	writeJSON(w, http.StatusOK, map[string]any{"cameras": cameras})
}

func (a *API) HandleGo2RTCCameras(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		a.handleGo2RTCCamerasGet(w, r)
	case http.MethodPost:
		a.handleGo2RTCCamerasAdd(w, r)
	case http.MethodDelete:
		a.handleGo2RTCCamerasDelete(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// cameraInfo describes one go2rtc stream for the UI.
func cameraInfoOf(name string, stream any, limitGB int) go2rtc.Camera {
	urlStr, _ := stream.(string)

	// Credentials are always present in URLs written by this app, so the IP
	// is only reported for authenticated URLs (historic UI behaviour).
	ip := ""
	if urlStr != "" && strings.Contains(urlStr, "@") {
		ip = go2rtc.CameraIP(urlStr)
	}

	return go2rtc.Camera{
		Name:    name,
		Type:    go2rtc.CameraType(urlStr),
		URL:     urlStr,
		IP:      ip,
		LimitGB: limitGB,
		Channel: go2rtc.CameraChannel(urlStr),
	}
}

func (a *API) handleGo2RTCCamerasGet(w http.ResponseWriter, r *http.Request) {
	go2cfg, err := config.LoadGo2RTCConfig(a.config.Go2RTCConfigPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	cameras := make([]go2rtc.Camera, 0, len(go2cfg.StreamOrder))
	for _, name := range go2cfg.StreamOrder {
		limitGB := a.config.DefaultCameraLimitGB
		if l, ok := a.config.CameraLimits[name]; ok {
			limitGB = l
		}
		cameras = append(cameras, cameraInfoOf(name, go2cfg.Streams[name], limitGB))
	}

	writeJSON(w, http.StatusOK, cameras)
}

type addCameraRequest struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	User    string `json:"user"`
	Pass    string `json:"pass"`
	IP      string `json:"ip"`
	Port    string `json:"port"`
	Channel string `json:"channel"`
	LimitGB int    `json:"limit_gb"`
}

func (a *API) handleGo2RTCCamerasAdd(w http.ResponseWriter, r *http.Request) {
	if !requireAdminRole(r) {
		writeError(w, http.StatusForbidden, "forbidden")
		return
	}

	var req addCameraRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request")
		return
	}

	if req.Name == "" || req.IP == "" {
		writeError(w, http.StatusBadRequest, "name and ip required")
		return
	}

	urlStr, err := go2rtc.BuildURL(req.Type, req.User, req.Pass, req.IP, req.Port, req.Channel)
	if err != nil {
		writeError(w, http.StatusBadRequest, "unsupported type")
		return
	}

	go2cfg, err := config.LoadGo2RTCConfig(a.config.Go2RTCConfigPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	go2cfg.Streams[req.Name] = urlStr
	go2cfg.StreamOrder = append(go2cfg.StreamOrder, req.Name)

	if err := config.SaveGo2RTCConfig(a.config.Go2RTCConfigPath, go2cfg.Streams, go2cfg.StreamOrder); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("save config: %s", err.Error()))
		return
	}

	limitGB := req.LimitGB
	if limitGB <= 0 {
		limitGB = a.config.DefaultCameraLimitGB
	}
	a.config.CameraLimits[req.Name] = limitGB
	saveConfigQuietly(a.configPath, a.config)

	if err := a.go2rtc.Restart(); err != nil {
		log.Printf("go2rtc restart error after adding camera %s: %v", req.Name, err)
	}

	a.recorder.StartRecordingStream(req.Name)

	log.Printf("go2rtc: added camera %s (%s)", req.Name, req.Type)
	writeStatus(w, "added")
}

func (a *API) handleGo2RTCCamerasDelete(w http.ResponseWriter, r *http.Request) {
	if !requireAdminRole(r) {
		writeError(w, http.StatusForbidden, "forbidden")
		return
	}

	name := r.URL.Query().Get("name")
	if name == "" {
		writeError(w, http.StatusBadRequest, "name required")
		return
	}

	go2cfg, err := config.LoadGo2RTCConfig(a.config.Go2RTCConfigPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if _, ok := go2cfg.Streams[name]; !ok {
		writeError(w, http.StatusNotFound, "camera not found")
		return
	}

	delete(go2cfg.Streams, name)
	newOrder := make([]string, 0, len(go2cfg.StreamOrder)-1)
	for _, n := range go2cfg.StreamOrder {
		if n != name {
			newOrder = append(newOrder, n)
		}
	}
	go2cfg.StreamOrder = newOrder

	if err := config.SaveGo2RTCConfig(a.config.Go2RTCConfigPath, go2cfg.Streams, go2cfg.StreamOrder); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("save config: %s", err.Error()))
		return
	}

	if err := a.go2rtc.Restart(); err != nil {
		log.Printf("go2rtc restart error after deleting camera %s: %v", name, err)
	}

	a.recorder.StopRecordingStream(name)

	log.Printf("go2rtc: deleted camera %s", name)
	writeStatus(w, "deleted")
}

func (a *API) HandleGo2RTCReorder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !requireAdminRole(r) {
		writeError(w, http.StatusForbidden, "forbidden")
		return
	}

	var req struct {
		From int `json:"from"`
		To   int `json:"to"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request")
		return
	}

	go2cfg, err := config.LoadGo2RTCConfig(a.config.Go2RTCConfigPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	order := go2cfg.StreamOrder
	if req.From < 0 || req.From >= len(order) || req.To < 0 || req.To >= len(order) {
		writeError(w, http.StatusBadRequest, "invalid position")
		return
	}

	name := order[req.From]
	newOrder := make([]string, 0, len(order))
	for i, n := range order {
		if i == req.From {
			continue
		}
		newOrder = append(newOrder, n)
	}
	newOrder = append(newOrder[:req.To], append([]string{name}, newOrder[req.To:]...)...)
	go2cfg.StreamOrder = newOrder

	if err := config.SaveGo2RTCConfig(a.config.Go2RTCConfigPath, go2cfg.Streams, go2cfg.StreamOrder); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("save config: %s", err.Error()))
		return
	}

	log.Printf("go2rtc: reordered camera %s from %d to %d", name, req.From, req.To)
	writeStatus(w, "reordered")
}
