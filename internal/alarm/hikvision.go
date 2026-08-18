package alarm

import (
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"simple_nvr/internal/config"
)

type HikvisionEvent struct {
	XMLName    xml.Name `xml:"EventNotificationAlert"`
	IPAddress  string   `xml:"ipAddress"`
	MACAddress string   `xml:"macAddress"`
	ChannelID  string   `xml:"channelID"`
	DateTime   string   `xml:"dateTime"`
	EventType  string   `xml:"eventType"`
	EventState string   `xml:"eventState"`
	Describe   string   `xml:"eventDescription"`
	ChannelName string  `xml:"channelName"`
}

type HikvisionAlarmServer struct {
	config   *config.NVRConfig
	alarm    *AlarmServer
	ipMap    map[string]string
	running  bool
	server   *http.Server
	stopCh   chan struct{}
}

func NewHikvisionAlarmServer(cfg *config.NVRConfig, alarmServer *AlarmServer, ipMap map[string]string) *HikvisionAlarmServer {
	return &HikvisionAlarmServer{
		config: cfg,
		alarm:  alarmServer,
		ipMap:  ipMap,
		stopCh: make(chan struct{}),
	}
}

func (s *HikvisionAlarmServer) Start() error {
	s.running = true
	s.stopCh = make(chan struct{})

	mux := http.NewServeMux()
	mux.HandleFunc("/hikvision/alarm", s.handleAlarm)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1024*1024))
		log.Printf("Hikvision unknown path: %s %s from %s body=%s", r.Method, r.URL.Path, r.RemoteAddr, string(body))
		http.NotFound(w, r)
	})

	addr := fmt.Sprintf(":%d", s.config.HikvisionAlarmPort)
	s.server = &http.Server{Addr: addr, Handler: mux}

	go func() {
		log.Printf("Hikvision alarm server started on %s", addr)
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Hikvision alarm server error: %v", err)
		}
	}()

	return nil
}

func (s *HikvisionAlarmServer) Stop() {
	if !s.running {
		return
	}
	s.running = false
	close(s.stopCh)
	if s.server != nil {
		s.server.Close()
	}
	log.Printf("Hikvision alarm server stopped")
}

func (s *HikvisionAlarmServer) GetStatus() map[string]any {
	return map[string]any{
		"running": s.running,
		"port":    s.config.HikvisionAlarmPort,
	}
}

func (s *HikvisionAlarmServer) handleAlarm(w http.ResponseWriter, r *http.Request) {
	log.Printf("Hikvision alarm: %s %s from %s headers=%v", r.Method, r.URL.Path, r.RemoteAddr, r.Header)

	if r.Method != http.MethodPost {
		log.Printf("Hikvision alarm: unexpected %s from %s", r.Method, r.RemoteAddr)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1024*1024))
	if err != nil {
		log.Printf("Hikvision alarm read error from %s: %v", r.RemoteAddr, err)
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}

	log.Printf("Hikvision alarm body from %s (%d bytes): %s", r.RemoteAddr, len(body), string(body))

	var hEvent HikvisionEvent
	if err := xml.Unmarshal(body, &hEvent); err != nil {
		log.Printf("Hikvision alarm parse error from %s: %v body=%s", r.RemoteAddr, err, string(body))
		http.Error(w, "xml parse error", http.StatusBadRequest)
		return
	}

	status := "Start"
	if hEvent.EventState == "inactive" {
		status = "Stop"
	}

	eventType := mapHikvisionType(hEvent.EventType)

	event := AlarmEvent{
		Time:    time.Now(),
		Camera:  hEvent.ChannelName,
		Type:    eventType,
		Event:   hEvent.EventType,
		Status:  status,
		Descrip: hEvent.Describe,
		Address: hEvent.IPAddress,
	}

	if s.ipMap != nil && event.Address != "" {
		if name, ok := s.ipMap[event.Address]; ok {
			event.Camera = name
		}
	}

	s.alarm.AddLog(event)
	s.alarm.SaveEvent(event)

	cameraLog := event.Camera
	if cameraLog == "" {
		cameraLog = event.Address
	}
	log.Printf("Hikvision alarm: camera=%s type=%s event=%s status=%s",
		cameraLog, event.Type, event.Event, event.Status)

	s.alarm.PublishMQTT(event)

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func mapHikvisionType(hikType string) string {
	switch hikType {
	case "VMD", "motionDetection":
		return "Motion"
	case "linedetection", "lineDetection":
		return "LineCross"
	case "fielddetection", "fieldDetection":
		return "Intrusion"
	case "regionEntrance":
		return "RegionEntrance"
	case "regionExiting":
		return "RegionExiting"
	case "faceDetection":
		return "Face"
	case "faceCapture":
		return "FaceSnap"
	case "peopleDetection", "group":
		return "PeopleGather"
	case "vehicleDetection", "ANPR":
		return "Vehicle"
	case "Shelteralarm", "tamperDetection":
		return "Tamper"
	case "defocusDetection", "defocus":
		return "Defocus"
	case "scenechangedetection":
		return "SceneChange"
	case "videoLoss":
		return "VideoLoss"
	case "audioDetection", "audioexception":
		return "Audio"
	case "diskfull":
		return "DiskFull"
	case "diskerror":
		return "DiskError"
	case "hdBadBlock":
		return "HDBadBlock"
	case "fireDetection":
		return "Fire"
	case "smokeDetection":
		return "Smoke"
	case "PIR":
		return "PIR"
	case "loitering":
		return "Loitering"
	case "unattendedBaggage":
		return "UnattendedBaggage"
	case "attendedBaggage":
		return "ObjectRemoval"
	case "parking":
		return "Parking"
	case "rapidMove":
		return "FastMove"
	default:
		return hikType
	}
}
