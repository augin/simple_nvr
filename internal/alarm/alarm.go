// Package alarm implements the alarm event pipeline: a TCP server for
// Dahua/XM devices, an HTTP endpoint for Hikvision ISAPI notifications, an
// in-memory event log, daily journals on disk and optional MQTT publishing.
package alarm

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"simple_nvr/internal/config"
)

// AlarmServer accepts events from Dahua/XM devices over TCP.
type AlarmServer struct {
	config *config.NVRConfig
	ipMap  map[string]string

	mu       sync.Mutex
	log      []AlarmEvent
	running  bool
	listener net.Listener
	mqtt     mqtt.Client
	stopCh   chan struct{}

	// dir stores daily journals; defaults to AlarmDir (see SetEventDir).
	dir string
	// fileMu serialises appends to the journal files.
	fileMu sync.Mutex
}

// NewAlarmServer creates a stopped server; call Start to begin listening.
func NewAlarmServer(cfg *config.NVRConfig, ipMap map[string]string) *AlarmServer {
	return &AlarmServer{
		config: cfg,
		ipMap:  ipMap,
		log:    make([]AlarmEvent, 0, MaxAlarmLog),
		stopCh: make(chan struct{}),
		dir:    AlarmDir,
	}
}

// SetEventDir overrides the journal directory (used by tests and packagers).
// It must be called before Start.
func (s *AlarmServer) SetEventDir(dir string) {
	s.fileMu.Lock()
	defer s.fileMu.Unlock()
	s.dir = dir
}

// EventDir returns the directory with daily event journals.
func (s *AlarmServer) EventDir() string {
	s.fileMu.Lock()
	defer s.fileMu.Unlock()
	return s.dir
}

// Start begins listening on alarm_port; it fails when already running or when
// the port cannot be bound.
func (s *AlarmServer) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return fmt.Errorf("alarm server already running")
	}
	if err := os.MkdirAll(s.EventDir(), permissionsDir); err != nil {
		return fmt.Errorf("failed to create alarm dir: %v", err)
	}

	addr := fmt.Sprintf(":%d", s.config.AlarmPort)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", addr, err)
	}

	s.listener = listener
	s.stopCh = make(chan struct{})
	s.running = true

	if s.config.MQTTHost != "" {
		s.connectMQTT()
	}

	go s.acceptLoop(listener, s.stopCh)
	go s.cleanupLoop(s.stopCh)

	log.Printf("Alarm server started on %s", addr)
	return nil
}

// Stop shuts the server down; calling it on a stopped server is a no-op.
func (s *AlarmServer) Stop() {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}
	s.running = false
	close(s.stopCh)
	listener := s.listener
	client := s.mqtt
	s.mqtt = nil
	s.mu.Unlock()

	if listener != nil {
		listener.Close()
	}
	if client != nil && client.IsConnected() {
		client.Disconnect(250)
	}
	log.Printf("Alarm server stopped")
}

func (s *AlarmServer) acceptLoop(listener net.Listener, stopCh <-chan struct{}) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-stopCh:
				return
			default:
			}
			log.Printf("Alarm accept error: %v", err)
			continue
		}
		go s.handleClient(conn, stopCh)
	}
}

// handleClient reads frames from a single device connection until it closes
// or the server stops.
func (s *AlarmServer) handleClient(conn net.Conn, stopCh <-chan struct{}) {
	defer conn.Close()

	addr := conn.RemoteAddr().String()
	log.Printf("Alarm client connected: %s", addr)

	for {
		select {
		case <-stopCh:
			return
		default:
		}

		conn.SetReadDeadline(time.Now().Add(30 * time.Second))

		payload, err := readDahuaFrame(conn)
		if err != nil {
			if err != io.EOF && !isTimeout(err) {
				log.Printf("Alarm read error from %s: %v", addr, err)
			}
			return
		}

		event, err := parseDahuaPayload(payload, s.ipMap)
		if err != nil {
			log.Printf("Alarm parse error from %s: %v", addr, err)
			continue
		}

		s.recordEvent(event)
	}
}

// recordEvent stores, journals, publishes and logs a single device event.
func (s *AlarmServer) recordEvent(event AlarmEvent) {
	event.Time = time.Now()
	s.Publish(event)

	cameraLog := event.Camera
	if cameraLog == "" {
		cameraLog = event.Address
	}
	log.Printf("Alarm event: camera=%s serial=%s type=%s event=%s status=%s",
		cameraLog, event.SerialID, event.Type, event.Event, event.Status)

	if s.config.AlarmCommand != "" && event.Status == "Start" {
		go s.runCommand(event)
	}
}

func isTimeout(err error) bool {
	ne, ok := err.(net.Error)
	return ok && ne.Timeout()
}

// GetStatus reports server state for the UI.
func (s *AlarmServer) GetStatus() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()

	mqttConnected := s.mqtt != nil && s.mqtt.IsConnected()

	return map[string]any{
		"running":     s.running,
		"port":        s.config.AlarmPort,
		"mqtt":        mqttConnected,
		"mqtt_host":   s.config.MQTTHost,
		"event_count": len(s.log),
		"enabled":     s.config.AlarmEnabled,
	}
}

// connectMQTT connects the MQTT client (if configured) with auto-reconnect.
func (s *AlarmServer) connectMQTT() {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", s.config.MQTTHost, s.config.MQTTPort))
	opts.SetClientID("simple-nvr-alarm")
	opts.SetAutoReconnect(true)

	if s.config.MQTTUser != "" {
		opts.SetUsername(s.config.MQTTUser)
		opts.SetPassword(s.config.MQTTPass)
	}

	opts.SetOnConnectHandler(func(mqtt.Client) {
		log.Printf("MQTT connected to %s:%d", s.config.MQTTHost, s.config.MQTTPort)
	})
	opts.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		log.Printf("MQTT connection lost: %v", err)
	})

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Printf("MQTT connect error: %v", token.Error())
	}

	s.mu.Lock()
	s.mqtt = client
	s.mu.Unlock()
}

// PublishMQTT publishes an event to the shared and per-device topics.
func (s *AlarmServer) PublishMQTT(event AlarmEvent) {
	s.mu.Lock()
	client := s.mqtt
	s.mu.Unlock()

	if client == nil || !client.IsConnected() {
		return
	}

	const topic = "dvr-alarm-server"
	data, err := json.Marshal(event)
	if err != nil {
		log.Printf("Alarm marshal error: %v", err)
		return
	}

	client.Publish(topic+"/events", 0, false, data)
	client.Publish(fmt.Sprintf("%s/devices/%s/events", topic, event.SerialID), 0, false, data)

	fields := map[string]string{
		"camera":  event.Camera,
		"type":    event.Type,
		"status":  event.Status,
		"event":   event.Event,
		"descrip": event.Descrip,
		"address": event.Address,
	}
	for name, value := range fields {
		if value != "" {
			client.Publish(fmt.Sprintf("%s/devices/%s/%s", topic, event.SerialID, name), 0, false, value)
		}
	}
}

// runCommand executes the configured alarm_command for "Start" events.
func (s *AlarmServer) runCommand(event AlarmEvent) {
	log.Printf("Running alarm command: %s", s.config.AlarmCommand)
	out, err := exec.Command("sh", "-c", s.config.AlarmCommand).CombinedOutput()
	if err != nil {
		log.Printf("Alarm command error: %v output: %s", err, string(out))
		return
	}
	log.Printf("Alarm command output: %s", string(out))
}
