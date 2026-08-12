package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type AlarmEvent struct {
	Time     time.Time `json:"time"`
	Camera   string    `json:"camera"`
	SerialID string    `json:"serial_id"`
	Type     string    `json:"type"`
	Event    string    `json:"event"`
	Channel  int       `json:"channel"`
	Status   string    `json:"status"`
	Descrip  string    `json:"descrip"`
	Address  string    `json:"address"`
}

type AlarmServer struct {
	config   *NVRConfig
	ipMap    map[string]string
	mu       sync.Mutex
	log      []AlarmEvent
	running  bool
	listener net.Listener
	mqtt     mqtt.Client
	stopCh   chan struct{}
}

const (
	maxAlarmLog = 500
	alarmDir    = "/var/lib/simple-nvr/alarms"
	retentionDays = 30
)

func NewAlarmServer(config *NVRConfig, ipMap map[string]string) *AlarmServer {
	return &AlarmServer{
		config: config,
		ipMap:  ipMap,
		log:    make([]AlarmEvent, 0, maxAlarmLog),
		stopCh: make(chan struct{}),
	}
}

func (s *AlarmServer) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return fmt.Errorf("alarm server already running")
	}

	if err := os.MkdirAll(alarmDir, 0755); err != nil {
		return fmt.Errorf("failed to create alarm dir: %v", err)
	}

	addr := fmt.Sprintf(":%d", s.config.AlarmPort)
	var err error
	s.listener, err = net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", addr, err)
	}

	s.running = true
	s.stopCh = make(chan struct{})

	if s.config.MQTTHost != "" {
		s.connectMQTT()
	}

	go s.acceptLoop()
	go s.cleanupLoop()
	log.Printf("Alarm server started on %s", addr)
	return nil
}

func (s *AlarmServer) Stop() {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}
	s.running = false
	close(s.stopCh)
	s.listener.Close()
	if s.mqtt != nil && s.mqtt.IsConnected() {
		s.mqtt.Disconnect(250)
		s.mqtt = nil
	}
	s.mu.Unlock()
	log.Printf("Alarm server stopped")
}

func (s *AlarmServer) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			s.mu.Lock()
			running := s.running
			s.mu.Unlock()
			if !running {
				return
			}
			log.Printf("Alarm accept error: %v", err)
			continue
		}
		go s.handleClient(conn)
	}
}

func (s *AlarmServer) handleClient(conn net.Conn) {
	defer conn.Close()

	addr := conn.RemoteAddr().String()
	log.Printf("Alarm client connected: %s", addr)

	for {
		select {
		case <-s.stopCh:
			return
		default:
		}

		conn.SetReadDeadline(time.Now().Add(30 * time.Second))

		header := make([]byte, 20)
		if _, err := io.ReadFull(conn, header); err != nil {
			if err != io.EOF {
				log.Printf("Alarm read header error from %s: %v", addr, err)
			}
			return
		}

		contentLen := binary.LittleEndian.Uint32(header[16:20])
		if contentLen > 1024*1024 {
			log.Printf("Alarm payload too large from %s: %d bytes", addr, contentLen)
			return
		}

		payload := make([]byte, contentLen)
		if _, err := io.ReadFull(conn, payload); err != nil {
			log.Printf("Alarm read payload error from %s: %v", addr, err)
			return
		}

		var rawData map[string]any
		if err := json.Unmarshal(payload, &rawData); err != nil {
			log.Printf("Alarm JSON parse error from %s: %v", addr, err)
			continue
		}

		event := AlarmEvent{
			Time:     time.Now(),
			SerialID: getString(rawData, "SerialID"),
			Type:     getString(rawData, "Type"),
			Event:    getString(rawData, "Event"),
			Channel:  getInt(rawData, "Channel"),
			Status:   getString(rawData, "Status"),
			Descrip:  getString(rawData, "Descrip"),
			Address:  decodeAddress(rawData["Address"]),
		}

		if s.ipMap != nil {
			event.Camera = s.ipMap[event.Address]
		}

		s.addLog(event)
		s.saveEvent(event)

		cameraLog := event.Camera
		if cameraLog == "" {
			cameraLog = event.Address
		}
		log.Printf("Alarm event: camera=%s serial=%s type=%s event=%s status=%s",
			cameraLog, event.SerialID, event.Type, event.Event, event.Status)

		s.publishMQTT(event)

		if s.config.AlarmCommand != "" && event.Status == "Start" {
			go s.runCommand(event)
		}
	}
}

func (s *AlarmServer) addLog(event AlarmEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.log = append(s.log, event)
	if len(s.log) > maxAlarmLog {
		s.log = s.log[len(s.log)-maxAlarmLog:]
	}
}

func (s *AlarmServer) saveEvent(event AlarmEvent) {
	fileName := event.Time.Format("2006-01-02") + ".jsonl"
	filePath := filepath.Join(alarmDir, fileName)

	data, err := json.Marshal(event)
	if err != nil {
		log.Printf("Alarm marshal error: %v", err)
		return
	}

	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("Alarm file open error: %v", err)
		return
	}
	defer f.Close()

	f.Write(data)
	f.WriteString("\n")
}

func (s *AlarmServer) loadRecentEvents(days int) {
	now := time.Now()
	for i := days - 1; i >= 0; i-- {
		date := now.AddDate(0, 0, -i)
		fileName := date.Format("2006-01-02") + ".jsonl"
		filePath := filepath.Join(alarmDir, fileName)

		events, err := readEventsFile(filePath)
		if err != nil {
			continue
		}

		for _, e := range events {
			s.log = append(s.log, e)
		}
	}

	if len(s.log) > maxAlarmLog {
		s.log = s.log[len(s.log)-maxAlarmLog:]
	}
}

func readEventsFile(path string) ([]AlarmEvent, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var events []AlarmEvent
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var event AlarmEvent
		if err := json.Unmarshal(line, &event); err != nil {
			continue
		}
		events = append(events, event)
	}

	return events, scanner.Err()
}

func (s *AlarmServer) cleanupOldEvents() {
	threshold := time.Now().AddDate(0, 0, -retentionDays)
	thresholdStr := threshold.Format("2006-01-02")

	entries, err := os.ReadDir(alarmDir)
	if err != nil {
		return
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".jsonl") {
			continue
		}
		dateStr := strings.TrimSuffix(entry.Name(), ".jsonl")
		if dateStr < thresholdStr {
			os.Remove(filepath.Join(alarmDir, entry.Name()))
			log.Printf("Alarm: cleaned up old log %s", entry.Name())
		}
	}
}

func (s *AlarmServer) cleanupLoop() {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.cleanupOldEvents()
		}
	}
}

func (s *AlarmServer) GetStatus() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()

	mqttConnected := false
	if s.mqtt != nil {
		mqttConnected = s.mqtt.IsConnected()
	}

	return map[string]any{
		"running":     s.running,
		"port":        s.config.AlarmPort,
		"mqtt":        mqttConnected,
		"mqtt_host":   s.config.MQTTHost,
		"event_count": len(s.log),
		"enabled":     s.config.AlarmEnabled,
	}
}

func (s *AlarmServer) GetLog(limit int) []AlarmEvent {
	s.mu.Lock()
	logCopy := make([]AlarmEvent, len(s.log))
	copy(logCopy, s.log)
	s.mu.Unlock()

	n := len(logCopy)
	if limit > 0 && limit < n {
		n = limit
	}

	result := make([]AlarmEvent, n)
	copy(result, logCopy[len(logCopy)-n:])

	sort.Slice(result, func(i, j int) bool {
		return result[i].Time.After(result[j].Time)
	})

	return result
}

func (s *AlarmServer) ClearLog() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log = s.log[:0]
}

func (s *AlarmServer) LoadRecentEvents(days int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loadRecentEvents(days)
}

func (s *AlarmServer) connectMQTT() {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", s.config.MQTTHost, s.config.MQTTPort))
	opts.SetClientID("simple-nvr-alarm")
	opts.SetAutoReconnect(true)

	if s.config.MQTTUser != "" {
		opts.SetUsername(s.config.MQTTUser)
		opts.SetPassword(s.config.MQTTPass)
	}

	opts.SetOnConnectHandler(func(c mqtt.Client) {
		log.Printf("MQTT connected to %s:%d", s.config.MQTTHost, s.config.MQTTPort)
	})
	opts.SetConnectionLostHandler(func(c mqtt.Client, err error) {
		log.Printf("MQTT connection lost: %v", err)
	})

	s.mqtt = mqtt.NewClient(opts)
	if token := s.mqtt.Connect(); token.Wait() && token.Error() != nil {
		log.Printf("MQTT connect error: %v", token.Error())
	}
}

func (s *AlarmServer) publishMQTT(event AlarmEvent) {
	if s.mqtt == nil || !s.mqtt.IsConnected() {
		return
	}

	topic := "dvr-alarm-server"
	serial := event.SerialID

	data, _ := json.Marshal(event)

	s.mqtt.Publish(topic+"/events", 0, false, data)
	s.mqtt.Publish(fmt.Sprintf("%s/devices/%s/events", topic, serial), 0, false, data)

	if event.Camera != "" {
		s.mqtt.Publish(fmt.Sprintf("%s/devices/%s/camera", topic, serial), 0, false, event.Camera)
	}
	if event.Type != "" {
		s.mqtt.Publish(fmt.Sprintf("%s/devices/%s/type", topic, serial), 0, false, event.Type)
	}
	if event.Status != "" {
		s.mqtt.Publish(fmt.Sprintf("%s/devices/%s/status", topic, serial), 0, false, event.Status)
	}
	if event.Event != "" {
		s.mqtt.Publish(fmt.Sprintf("%s/devices/%s/event", topic, serial), 0, false, event.Event)
	}
	if event.Descrip != "" {
		s.mqtt.Publish(fmt.Sprintf("%s/devices/%s/descrip", topic, serial), 0, false, event.Descrip)
	}
	if event.Address != "" {
		s.mqtt.Publish(fmt.Sprintf("%s/devices/%s/address", topic, serial), 0, false, event.Address)
	}
}

func (s *AlarmServer) runCommand(event AlarmEvent) {
	log.Printf("Running alarm command: %s", s.config.AlarmCommand)
	cmd := exec.Command("sh", "-c", s.config.AlarmCommand)
	out, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("Alarm command error: %v output: %s", err, string(out))
	} else {
		log.Printf("Alarm command output: %s", string(out))
	}
}

func getString(m map[string]any, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func getInt(m map[string]any, key string) int {
	if v, ok := m[key]; ok {
		switch n := v.(type) {
		case float64:
			return int(n)
		case int:
			return n
		}
	}
	return 0
}

func decodeAddress(raw any) string {
	if raw == nil {
		return ""
	}
	var hexStr string
	switch v := raw.(type) {
	case string:
		hexStr = v
	case float64:
		hexStr = fmt.Sprintf("%08X", uint32(v))
	default:
		return ""
	}
	if hexStr == "" {
		return ""
	}
	cleaned := strings.TrimPrefix(strings.TrimPrefix(hexStr, "0X"), "0x")
	addr, err := strconv.ParseUint(cleaned, 16, 32)
	if err != nil {
		return ""
	}
	return net.IPv4(byte(addr), byte(addr>>8), byte(addr>>16), byte(addr>>24)).String()
}

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
	config   *NVRConfig
	alarm    *AlarmServer
	ipMap    map[string]string
	running  bool
	server   *http.Server
	stopCh   chan struct{}
}

func NewHikvisionAlarmServer(config *NVRConfig, alarm *AlarmServer, ipMap map[string]string) *HikvisionAlarmServer {
	return &HikvisionAlarmServer{
		config: config,
		alarm:  alarm,
		ipMap:  ipMap,
		stopCh: make(chan struct{}),
	}
}

func (s *HikvisionAlarmServer) Start() error {
	s.running = true
	s.stopCh = make(chan struct{})

	mux := http.NewServeMux()
	mux.HandleFunc("/hikvision/alarm", s.handleAlarm)

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
	if r.Method != http.MethodPost {
		log.Printf("Hikvision alarm: unexpected %s from %s", r.Method, r.RemoteAddr)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1024*1024))
	if err != nil {
		log.Printf("Hikvision alarm read error: %v", err)
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}

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

	s.alarm.addLog(event)
	s.alarm.saveEvent(event)

	cameraLog := event.Camera
	if cameraLog == "" {
		cameraLog = event.Address
	}
	log.Printf("Hikvision alarm: camera=%s type=%s event=%s status=%s",
		cameraLog, event.Type, event.Event, event.Status)

	s.alarm.publishMQTT(event)

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
