package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os/exec"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type AlarmEvent struct {
	Time     time.Time `json:"time"`
	SerialID string    `json:"serial_id"`
	Type     string    `json:"type"`
	Event    string    `json:"event"`
	Channel  int       `json:"channel"`
	Status   string    `json:"status"`
	Descrip  string    `json:"descrip"`
	Address  string    `json:"address"`
	Raw      any       `json:"raw"`
}

type AlarmServer struct {
	config   *NVRConfig
	mu       sync.Mutex
	log      []AlarmEvent
	running  bool
	listener net.Listener
	mqtt     mqtt.Client
	conn     chan net.Conn
	stopCh   chan struct{}
}

const maxAlarmLog = 500

func NewAlarmServer(config *NVRConfig) *AlarmServer {
	return &AlarmServer{
		config: config,
		log:    make([]AlarmEvent, 0, maxAlarmLog),
		conn:   make(chan net.Conn, 16),
		stopCh: make(chan struct{}),
	}
}

func (s *AlarmServer) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return fmt.Errorf("alarm server already running")
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
			Address:  decodeAddress(getString(rawData, "Address")),
			Raw:      rawData,
		}

		s.addLog(event)
		log.Printf("Alarm event: serial=%s type=%s event=%s status=%s",
			event.SerialID, event.Type, event.Event, event.Status)

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
	}
}

func (s *AlarmServer) GetLog(limit int) []AlarmEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	n := len(s.log)
	if limit > 0 && limit < n {
		n = limit
	}

	result := make([]AlarmEvent, n)
	for i := 0; i < n; i++ {
		result[i] = s.log[len(s.log)-n+i]
	}

	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}

	return result
}

func (s *AlarmServer) ClearLog() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log = s.log[:0]
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

	data, _ := json.Marshal(event.Raw)

	s.mqtt.Publish(topic+"/events", 0, false, data)
	s.mqtt.Publish(fmt.Sprintf("%s/devices/%s/events", topic, serial), 0, false, data)

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

func decodeAddress(hexStr string) string {
	if hexStr == "" {
		return ""
	}
	var addr uint32
	fmt.Sscanf(hexStr, "%x", &addr)
	ip := net.IPv4(byte(addr), byte(addr>>8), byte(addr>>16), byte(addr>>24))
	return ip.String()
}
