package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
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

	if _, err := f.Write(data); err != nil {
		log.Printf("Alarm file write error: %v", err)
		return
	}
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
			if err := os.Remove(filepath.Join(alarmDir, entry.Name())); err != nil {
				log.Printf("Alarm cleanup error removing %s: %v", entry.Name(), err)
			} else {
				log.Printf("Alarm: cleaned up old log %s", entry.Name())
			}
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
