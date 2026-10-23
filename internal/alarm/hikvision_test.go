package alarm

import (
	"encoding/xml"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"simple_nvr/internal/config"
)

const sampleHikvisionXML = `<EventNotificationAlert>
  <ipAddress>192.168.1.20</ipAddress>
  <macAddress>aa:bb:cc:dd:ee:ff</macAddress>
  <channelID>1</channelID>
  <dateTime>2026-10-23T10:00:00</dateTime>
  <eventType>motionDetection</eventType>
  <eventState>active</eventState>
  <eventDescription>Motion detection</eventDescription>
  <channelName>Front Door</channelName>
</EventNotificationAlert>`

func TestMapHikvisionType(t *testing.T) {
	cases := map[string]string{
		"VMD":              "Motion",
		"motionDetection":  "Motion",
		"linedetection":    "LineCross",
		"fielddetection":   "Intrusion",
		"faceDetection":    "Face",
		"peopleDetection":  "PeopleGather",
		"vehicleDetection": "Vehicle",
		"videoLoss":        "VideoLoss",
		"unknownType":      "unknownType",
		"":                 "",
	}
	for in, want := range cases {
		if got := mapHikvisionType(in); got != want {
			t.Errorf("mapHikvisionType(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestBuildHikvisionEvent(t *testing.T) {
	var hEvent HikvisionEvent
	if err := xml.Unmarshal([]byte(sampleHikvisionXML), &hEvent); err != nil {
		t.Fatalf("xml: %v", err)
	}
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)

	event := buildHikvisionEvent(hEvent, map[string]string{"192.168.1.20": "front"}, now)
	if event.Time != now {
		t.Errorf("Time = %v", event.Time)
	}
	if event.Camera != "front" {
		t.Errorf("Camera = %q, want ipMap override", event.Camera)
	}
	if event.Type != "Motion" || event.Event != "motionDetection" || event.Status != "Start" {
		t.Errorf("event = %+v", event)
	}
	if event.Address != "192.168.1.20" || event.Descrip != "Motion detection" {
		t.Errorf("event = %+v", event)
	}

	// without a mapping the channel name is used
	event = buildHikvisionEvent(hEvent, nil, now)
	if event.Camera != "Front Door" {
		t.Errorf("Camera = %q, want channel name", event.Camera)
	}

	// inactive state means the alarm stopped
	hEvent.EventState = "inactive"
	event = buildHikvisionEvent(hEvent, nil, now)
	if event.Status != "Stop" {
		t.Errorf("Status = %q, want Stop", event.Status)
	}
}

func newHikvisionPair(t *testing.T) (*HikvisionAlarmServer, *AlarmServer) {
	t.Helper()
	cfg := &config.NVRConfig{AlarmPort: 0, HikvisionAlarmPort: 0}
	alarmSrv := newTestServer(t, 0)
	hik := NewHikvisionAlarmServer(cfg, alarmSrv, map[string]string{"192.168.1.20": "front"})
	t.Cleanup(hik.Stop)
	return hik, alarmSrv
}

func TestHikvisionHandleAlarm(t *testing.T) {
	hik, alarmSrv := newHikvisionPair(t)

	req := httptest.NewRequest(http.MethodPost, "/hikvision/alarm", strings.NewReader(sampleHikvisionXML))
	rec := httptest.NewRecorder()
	hik.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK || rec.Body.String() != "OK" {
		t.Fatalf("status = %d body = %q", rec.Code, rec.Body.String())
	}

	events := alarmSrv.GetLog(10)
	if len(events) != 1 {
		t.Fatalf("events = %d", len(events))
	}
	if events[0].Camera != "front" || events[0].Status != "Start" || events[0].Type != "Motion" {
		t.Errorf("event = %+v", events[0])
	}
}

func TestHikvisionHandleAlarmErrors(t *testing.T) {
	hik, alarmSrv := newHikvisionPair(t)
	handler := hik.Handler()

	// wrong method
	req := httptest.NewRequest(http.MethodGet, "/hikvision/alarm", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET status = %d", rec.Code)
	}

	// broken xml
	req = httptest.NewRequest(http.MethodPost, "/hikvision/alarm", strings.NewReader("<EventNotificationAlert>"))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("broken xml status = %d", rec.Code)
	}

	// unknown path
	req = httptest.NewRequest(http.MethodPost, "/other", strings.NewReader("payload"))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("unknown path status = %d", rec.Code)
	}

	if got := alarmSrv.GetLog(10); len(got) != 0 {
		t.Errorf("failed requests must not produce events: %+v", got)
	}
}

func TestHikvisionStartStopIdempotency(t *testing.T) {
	hik, _ := newHikvisionPair(t)

	if err := hik.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := hik.Start(); err == nil {
		t.Error("second Start must fail")
	}
	if hik.Addr() == "" {
		t.Error("Addr must be set after Start")
	}
	status := hik.GetStatus()
	if status["running"] != true {
		t.Errorf("status = %v", status)
	}

	hik.Stop()
	if hik.GetStatus()["running"] != false {
		t.Error("still running after Stop")
	}
	hik.Stop() // double stop must not panic

	// endpoint is gone after stop
	if _, err := net.DialTimeout("tcp", hik.Addr(), 100*time.Millisecond); err == nil {
		t.Error("port still accepting connections after Stop")
	}
}

func TestHikvisionStartFailsWhenPortBusy(t *testing.T) {
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()

	hik, _ := newHikvisionPair(t)
	hik.config.HikvisionAlarmPort = busy.Addr().(*net.TCPAddr).Port
	if err := hik.Start(); err == nil {
		t.Fatal("expected error when port is busy")
	}
	if hik.GetStatus()["running"] != false {
		t.Error("status must stay stopped after failed start")
	}
}

func TestHikvisionServesOverHTTP(t *testing.T) {
	hik, alarmSrv := newHikvisionPair(t)
	if err := hik.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	resp, err := http.Post("http://"+hik.Addr()+"/hikvision/alarm",
		"application/xml", strings.NewReader(sampleHikvisionXML))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(alarmSrv.GetLog(10)) == 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Error("event not received")
}
