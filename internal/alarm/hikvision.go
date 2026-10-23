package alarm

import (
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"simple_nvr/internal/config"
)

const maxHikvisionBody = 1024 * 1024

// HikvisionEvent is the ISAPI EventNotificationAlert XML payload.
type HikvisionEvent struct {
	XMLName     xml.Name `xml:"EventNotificationAlert"`
	IPAddress   string   `xml:"ipAddress"`
	MACAddress  string   `xml:"macAddress"`
	ChannelID   string   `xml:"channelID"`
	DateTime    string   `xml:"dateTime"`
	EventType   string   `xml:"eventType"`
	EventState  string   `xml:"eventState"`
	Describe    string   `xml:"eventDescription"`
	ChannelName string   `xml:"channelName"`
}

// hikvisionTypes maps ISAPI event types to the common alarm vocabulary.
var hikvisionTypes = map[string]string{
	"VMD":                  "Motion",
	"motionDetection":      "Motion",
	"linedetection":        "LineCross",
	"lineDetection":        "LineCross",
	"fielddetection":       "Intrusion",
	"fieldDetection":       "Intrusion",
	"regionEntrance":       "RegionEntrance",
	"regionExiting":        "RegionExiting",
	"faceDetection":        "Face",
	"faceCapture":          "FaceSnap",
	"peopleDetection":      "PeopleGather",
	"group":                "PeopleGather",
	"vehicleDetection":     "Vehicle",
	"ANPR":                 "Vehicle",
	"Shelteralarm":         "Tamper",
	"tamperDetection":      "Tamper",
	"defocusDetection":     "Defocus",
	"defocus":              "Defocus",
	"scenechangedetection": "SceneChange",
	"videoLoss":            "VideoLoss",
	"audioDetection":       "Audio",
	"audioexception":       "Audio",
	"diskfull":             "DiskFull",
	"diskerror":            "DiskError",
	"hdBadBlock":           "HDBadBlock",
	"fireDetection":        "Fire",
	"smokeDetection":       "Smoke",
	"PIR":                  "PIR",
	"loitering":            "Loitering",
	"unattendedBaggage":    "UnattendedBaggage",
	"attendedBaggage":      "ObjectRemoval",
	"parking":              "Parking",
	"rapidMove":            "FastMove",
}

// mapHikvisionType translates an ISPI event type to the internal name.
func mapHikvisionType(hikType string) string {
	if mapped, ok := hikvisionTypes[hikType]; ok {
		return mapped
	}
	return hikType
}

// HikvisionAlarmServer accepts Hikvision ISAPI HTTP notifications.
type HikvisionAlarmServer struct {
	config *config.NVRConfig
	alarm  *AlarmServer
	ipMap  map[string]string

	mu       sync.Mutex
	running  bool
	listener net.Listener
	server   *http.Server
}

// NewHikvisionAlarmServer creates a stopped server.
func NewHikvisionAlarmServer(cfg *config.NVRConfig, alarmServer *AlarmServer, ipMap map[string]string) *HikvisionAlarmServer {
	return &HikvisionAlarmServer{
		config: cfg,
		alarm:  alarmServer,
		ipMap:  ipMap,
	}
}

// Start binds the HTTP port and serves notifications in the background.
func (s *HikvisionAlarmServer) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return fmt.Errorf("hikvision alarm server already running")
	}

	addr := fmt.Sprintf(":%d", s.config.HikvisionAlarmPort)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", addr, err)
	}

	s.listener = listener
	s.server = &http.Server{Handler: s.Handler()}
	s.running = true
	server := s.server

	log.Printf("Hikvision alarm server started on %s", addr)
	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Printf("Hikvision alarm server error: %v", err)
		}
	}()
	return nil
}

// Stop closes the HTTP server; calling it twice is a no-op.
func (s *HikvisionAlarmServer) Stop() {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}
	s.running = false
	server := s.server
	s.server = nil
	s.mu.Unlock()

	if server != nil {
		server.Close()
	}
	log.Printf("Hikvision alarm server stopped")
}

// Addr returns the bound address (useful when started on port 0).
func (s *HikvisionAlarmServer) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// GetStatus reports server state for the UI.
func (s *HikvisionAlarmServer) GetStatus() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return map[string]any{
		"running": s.running,
		"port":    s.config.HikvisionAlarmPort,
	}
}

// Handler builds the HTTP routes of the notification endpoint.
func (s *HikvisionAlarmServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/hikvision/alarm", s.handleAlarm)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(io.LimitReader(r.Body, maxHikvisionBody))
		log.Printf("Hikvision unknown path: %s %s from %s body=%s",
			r.Method, r.URL.Path, r.RemoteAddr, string(body))
		http.NotFound(w, r)
	})
	return mux
}

func (s *HikvisionAlarmServer) handleAlarm(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		log.Printf("Hikvision alarm: unexpected %s from %s", r.Method, r.RemoteAddr)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxHikvisionBody))
	if err != nil {
		log.Printf("Hikvision alarm read error from %s: %v", r.RemoteAddr, err)
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}

	var hEvent HikvisionEvent
	if err := xml.Unmarshal(body, &hEvent); err != nil {
		log.Printf("Hikvision alarm parse error from %s: %v body=%s", r.RemoteAddr, err, string(body))
		http.Error(w, "xml parse error", http.StatusBadRequest)
		return
	}

	event := buildHikvisionEvent(hEvent, s.ipMap, time.Now())
	s.alarm.Publish(event)

	cameraLog := event.Camera
	if cameraLog == "" {
		cameraLog = event.Address
	}
	log.Printf("Hikvision alarm: camera=%s type=%s event=%s status=%s",
		cameraLog, event.Type, event.Event, event.Status)

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// buildHikvisionEvent converts an XML notification into an AlarmEvent,
// preferring the configured stream name over the device channel name.
func buildHikvisionEvent(hEvent HikvisionEvent, ipMap map[string]string, now time.Time) AlarmEvent {
	status := "Start"
	if hEvent.EventState == "inactive" {
		status = "Stop"
	}

	event := AlarmEvent{
		Time:    now,
		Camera:  hEvent.ChannelName,
		Type:    mapHikvisionType(hEvent.EventType),
		Event:   hEvent.EventType,
		Status:  status,
		Descrip: hEvent.Describe,
		Address: hEvent.IPAddress,
	}

	if ipMap != nil && event.Address != "" {
		if name, ok := ipMap[event.Address]; ok {
			event.Camera = name
		}
	}
	return event
}
