package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
)

func (a *API) HandleCameras(w http.ResponseWriter, r *http.Request) {
	go2cfg, err := loadGo2RTCConfig(a.config.Go2RTCConfigPath)
	if err != nil {
		log.Printf("Warning: could not load go2rtc config %s: %v", a.config.Go2RTCConfigPath, err)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"cameras": []string{}, "error": err.Error()})
		return
	}

	cameras := make([]string, 0, len(go2cfg.Streams))
	for _, name := range go2cfg.StreamOrder {
		cameras = append(cameras, name)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"cameras": cameras})
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
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (a *API) handleGo2RTCCamerasGet(w http.ResponseWriter, r *http.Request) {
	go2cfg, err := loadGo2RTCConfig(a.config.Go2RTCConfigPath)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	type cameraInfo struct {
		Name    string `json:"name"`
		Type    string `json:"type"`
		URL     string `json:"url"`
		IP      string `json:"ip"`
		LimitGB int    `json:"limit_gb"`
		Channel string `json:"channel,omitempty"`
	}

	cameras := make([]cameraInfo, 0, len(go2cfg.StreamOrder))
	for _, name := range go2cfg.StreamOrder {
		urlStr := ""
		switch v := go2cfg.Streams[name].(type) {
		case string:
			urlStr = v
		}

		camType := "unknown"
		ip := ""
		channel := ""
		if urlStr != "" {
			if strings.HasPrefix(urlStr, "dvrip://") {
				camType = "dvrip"
			} else if strings.HasPrefix(urlStr, "rtsp://") {
				camType = "rtsp"
			} else if strings.HasPrefix(urlStr, "onvif://") {
				camType = "onvif"
			} else if strings.HasPrefix(urlStr, "isapi://") {
				camType = "isapi"
			}

			if idx := strings.Index(urlStr, "@"); idx != -1 {
				parts := urlStr[idx+1:]
				if qIdx := strings.Index(parts, "?"); qIdx != -1 {
					parts = parts[:qIdx]
				}
				if colonIdx := strings.Index(parts, ":"); colonIdx != -1 {
					ip = parts[:colonIdx]
				} else if slashIdx := strings.Index(parts, "/"); slashIdx != -1 {
					ip = parts[:slashIdx]
				} else {
					ip = parts
				}
			}

			if camType == "dvrip" {
				if idx := strings.Index(urlStr, "channel="); idx != -1 {
					channel = urlStr[idx+8:]
					if ampIdx := strings.Index(channel, "&"); ampIdx != -1 {
						channel = channel[:ampIdx]
					}
				}
			}
		}

		limitGB := a.config.DefaultCameraLimitGB
		if l, ok := a.config.CameraLimits[name]; ok {
			limitGB = l
		}

		cameras = append(cameras, cameraInfo{
			Name:    name,
			Type:    camType,
			URL:     urlStr,
			IP:      ip,
			LimitGB: limitGB,
			Channel: channel,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(cameras)
}

func (a *API) handleGo2RTCCamerasAdd(w http.ResponseWriter, r *http.Request) {
	if !requireAdminRole(r) {
		http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
		return
	}

	var req struct {
		Name    string `json:"name"`
		Type    string `json:"type"`
		User    string `json:"user"`
		Pass    string `json:"pass"`
		IP      string `json:"ip"`
		Port    string `json:"port"`
		Channel string `json:"channel"`
		LimitGB int    `json:"limit_gb"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
		return
	}

	if req.Name == "" || req.IP == "" {
		http.Error(w, `{"error":"name and ip required"}`, http.StatusBadRequest)
		return
	}

	var urlStr string
	switch req.Type {
	case "rtsp":
		port := req.Port
		if port == "" {
			port = "554"
		}
		urlStr = fmt.Sprintf("rtsp://%s:%s@%s:%s/stream0", req.User, req.Pass, req.IP, port)
	case "dvrip":
		channel := req.Channel
		if channel == "" {
			channel = "0"
		}
		urlStr = fmt.Sprintf("dvrip://%s:%s@%s?channel=%s&subtype=0", req.User, req.Pass, req.IP, channel)
	case "onvif":
		urlStr = fmt.Sprintf("onvif://%s:%s@%s", req.User, req.Pass, req.IP)
	case "isapi":
		urlStr = fmt.Sprintf("isapi://%s:%s@%s", req.User, req.Pass, req.IP)
	default:
		http.Error(w, `{"error":"unsupported type"}`, http.StatusBadRequest)
		return
	}

	go2cfg, err := loadGo2RTCConfig(a.config.Go2RTCConfigPath)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	go2cfg.Streams[req.Name] = urlStr
	go2cfg.StreamOrder = append(go2cfg.StreamOrder, req.Name)

	if err := saveGo2RTCConfig(a.config.Go2RTCConfigPath, go2cfg.Streams, go2cfg.StreamOrder); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"save config: %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	limitGB := req.LimitGB
	if limitGB <= 0 {
		limitGB = a.config.DefaultCameraLimitGB
	}
	a.config.CameraLimits[req.Name] = limitGB
	if err := saveNVRConfig(a.configPath, a.config); err != nil {
		log.Printf("warning: save nvr config: %v", err)
	}

	if err := a.restartGo2RTC(); err != nil {
		log.Printf("go2rtc restart error after adding camera %s: %v", req.Name, err)
	}

	a.recorder.StartRecordingStream(req.Name)

	log.Printf("go2rtc: added camera %s (%s)", req.Name, req.Type)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "added"})
}

func (a *API) handleGo2RTCCamerasDelete(w http.ResponseWriter, r *http.Request) {
	if !requireAdminRole(r) {
		http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
		return
	}

	name := r.URL.Query().Get("name")
	if name == "" {
		http.Error(w, `{"error":"name required"}`, http.StatusBadRequest)
		return
	}

	go2cfg, err := loadGo2RTCConfig(a.config.Go2RTCConfigPath)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	if _, ok := go2cfg.Streams[name]; !ok {
		http.Error(w, `{"error":"camera not found"}`, http.StatusNotFound)
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

	if err := saveGo2RTCConfig(a.config.Go2RTCConfigPath, go2cfg.Streams, go2cfg.StreamOrder); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"save config: %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	if err := a.restartGo2RTC(); err != nil {
		log.Printf("go2rtc restart error after deleting camera %s: %v", name, err)
	}

	a.recorder.StopRecordingStream(name)

	log.Printf("go2rtc: deleted camera %s", name)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "deleted"})
}

func (a *API) HandleGo2RTCReorder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !requireAdminRole(r) {
		http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
		return
	}

	var req struct {
		From int `json:"from"`
		To   int `json:"to"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
		return
	}

	go2cfg, err := loadGo2RTCConfig(a.config.Go2RTCConfigPath)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	order := go2cfg.StreamOrder
	if req.From < 0 || req.From >= len(order) || req.To < 0 || req.To >= len(order) {
		http.Error(w, `{"error":"invalid position"}`, http.StatusBadRequest)
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
	insertAt := req.To
	if req.From < req.To {
		insertAt = req.To
	}
	newOrder = append(newOrder[:insertAt], append([]string{name}, newOrder[insertAt:]...)...)
	go2cfg.StreamOrder = newOrder

	if err := saveGo2RTCConfig(a.config.Go2RTCConfigPath, go2cfg.Streams, go2cfg.StreamOrder); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"save config: %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	log.Printf("go2rtc: reordered camera %s from %d to %d", name, req.From, req.To)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "reordered"})
}
