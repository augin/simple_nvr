package alarm

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"simple_nvr/internal/config"
)

// newTestServer builds a stopped server writing journals into a temp dir.
func newTestServer(t *testing.T, port int) *AlarmServer {
	t.Helper()
	cfg := &config.NVRConfig{AlarmPort: port, HikvisionAlarmPort: 0}
	s := NewAlarmServer(cfg, nil)
	s.SetEventDir(t.TempDir())
	t.Cleanup(s.Stop)
	return s
}

func testEvent(camera, status string, ts time.Time) AlarmEvent {
	return AlarmEvent{
		Time:     ts,
		Camera:   camera,
		SerialID: "dev",
		Type:     "Alarm",
		Event:    "Motion",
		Status:   status,
	}
}

func TestAddLogCapsAtMaxAlarmLog(t *testing.T) {
	s := newTestServer(t, 0)
	now := time.Now()
	for i := 0; i < MaxAlarmLog+50; i++ {
		s.AddLog(testEvent(fmt.Sprintf("cam%d", i), "Start", now.Add(time.Duration(i)*time.Second)))
	}

	s.mu.Lock()
	n := len(s.log)
	first := s.log[0]
	last := s.log[len(s.log)-1]
	s.mu.Unlock()

	if n != MaxAlarmLog {
		t.Errorf("log size = %d, want %d", n, MaxAlarmLog)
	}
	if first.Camera != "cam50" {
		t.Errorf("oldest kept = %q, want cam50", first.Camera)
	}
	if last.Camera != fmt.Sprintf("cam%d", MaxAlarmLog+49) {
		t.Errorf("newest kept = %q", last.Camera)
	}
}

func TestGetLogOrderAndLimit(t *testing.T) {
	s := newTestServer(t, 0)
	base := time.Date(2026, 10, 23, 10, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		s.AddLog(testEvent(fmt.Sprintf("cam%d", i), "Start", base.Add(time.Duration(i)*time.Minute)))
	}

	all := s.GetLog(0)
	if len(all) != 5 {
		t.Fatalf("len = %d", len(all))
	}
	if all[0].Camera != "cam4" || all[4].Camera != "cam0" {
		t.Errorf("expected newest first, got %q..%q", all[0].Camera, all[4].Camera)
	}

	limited := s.GetLog(2)
	if len(limited) != 2 || limited[0].Camera != "cam4" || limited[1].Camera != "cam3" {
		t.Errorf("GetLog(2) = %+v", limited)
	}

	if got := s.GetLog(-1); len(got) != 5 {
		t.Errorf("negative limit returned %d entries", len(got))
	}
	if got := s.GetLog(100); len(got) != 5 {
		t.Errorf("limit above size returned %d entries", len(got))
	}
}

func TestClearLog(t *testing.T) {
	s := newTestServer(t, 0)
	s.AddLog(testEvent("cam", "Start", time.Now()))
	s.ClearLog()
	if got := s.GetLog(0); len(got) != 0 {
		t.Errorf("GetLog after clear = %d entries", len(got))
	}
}

func TestSaveEventAndLoadRecentEvents(t *testing.T) {
	s := newTestServer(t, 0)
	dir := s.EventDir()

	s.SaveEvent(testEvent("cam1", "Start", time.Now()))
	s.SaveEvent(testEvent("cam2", "Stop", time.Now()))

	journal := filepath.Join(dir, time.Now().Format("2006-01-02")+eventsFileSuffix)
	events, err := ReadEventsFile(journal)
	if err != nil {
		t.Fatalf("journal read: %v", err)
	}
	if len(events) != 2 || events[0].Camera != "cam1" || events[1].Camera != "cam2" {
		t.Errorf("journal = %+v", events)
	}

	s.ClearLog()
	s.LoadRecentEvents(1)
	got := s.GetLog(0)
	if len(got) != 2 {
		t.Fatalf("loaded %d events, want 2", len(got))
	}
	if got[0].Camera != "cam2" {
		t.Errorf("load order: first = %q", got[0].Camera)
	}
}

func TestLoadRecentEventsMissingJournals(t *testing.T) {
	s := newTestServer(t, 0)
	s.LoadRecentEvents(3) // no files at all: must not fail
	if got := s.GetLog(0); len(got) != 0 {
		t.Errorf("events = %d", len(got))
	}
}

func TestCleanupOldEvents(t *testing.T) {
	s := newTestServer(t, 0)
	dir := s.EventDir()

	oldName := time.Now().AddDate(0, 0, -(RetentionDays+5)).Format("2006-01-02") + eventsFileSuffix
	recentName := time.Now().Format("2006-01-02") + eventsFileSuffix
	for _, name := range []string{oldName, recentName} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("{}\n"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	// unrelated files must survive
	other := filepath.Join(dir, "notes.txt")
	if err := os.WriteFile(other, []byte("keep"), 0644); err != nil {
		t.Fatal(err)
	}

	s.cleanupOldEvents()

	if _, err := os.Stat(filepath.Join(dir, oldName)); err == nil {
		t.Error("old journal must be removed")
	}
	if _, err := os.Stat(filepath.Join(dir, recentName)); err != nil {
		t.Error("recent journal must survive")
	}
	if _, err := os.Stat(other); err != nil {
		t.Error("non-journal file must survive")
	}
}

func TestStartStopIdempotency(t *testing.T) {
	s := newTestServer(t, 0) // port 0: kernel picks a free port

	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := s.Start(); err == nil {
		t.Error("second Start must fail")
	}

	status := s.GetStatus()
	if status["running"] != true || status["port"] != 0 {
		t.Errorf("status = %v", status)
	}

	s.Stop()
	if s.GetStatus()["running"] != false {
		t.Error("still running after Stop")
	}
	s.Stop() // must not panic on double stop

	// can start again after stop
	if err := s.Start(); err != nil {
		t.Errorf("restart: %v", err)
	}
}

func TestStartFailsWhenPortBusy(t *testing.T) {
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()

	port := busy.Addr().(*net.TCPAddr).Port
	s := newTestServer(t, port)
	if err := s.Start(); err == nil {
		t.Fatal("expected error when port is busy")
	}
	if s.GetStatus()["running"] != false {
		t.Error("status must stay stopped after failed start")
	}
}

func TestTCPServerReceivesEvent(t *testing.T) {
	cfg := &config.NVRConfig{AlarmPort: 0}
	s := NewAlarmServer(cfg, map[string]string{"192.168.1.10": "yard"})
	s.SetEventDir(t.TempDir())
	defer s.Stop()

	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	s.mu.Lock()
	addr := s.listener.Addr().String()
	s.mu.Unlock()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	payload := []byte(`{"SerialID":"dev-1","Type":"Alarm","Event":"Motion","Channel":1,"Status":"Start","Address":"0A01A8C0"}`)
	if _, err := conn.Write(buildDahuaFrame(payload)); err != nil {
		t.Fatalf("write: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	var events []AlarmEvent
	for time.Now().Before(deadline) {
		events = s.GetLog(10)
		if len(events) == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if len(events) != 1 {
		t.Fatalf("received %d events, want 1", len(events))
	}
	e := events[0]
	if e.Camera != "yard" || e.Address != "192.168.1.10" || e.Status != "Start" || e.SerialID != "dev-1" {
		t.Errorf("event = %+v", e)
	}
	if e.Time.IsZero() {
		t.Error("event time not set")
	}

	// journal written
	journal := filepath.Join(s.EventDir(), time.Now().Format("2006-01-02")+eventsFileSuffix)
	if _, err := os.Stat(journal); err != nil {
		t.Errorf("journal missing: %v", err)
	}
}

func TestTCPServerRejectsOversizedFrameAndStaysAlive(t *testing.T) {
	s := newTestServer(t, 0)
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	s.mu.Lock()
	addr := s.listener.Addr().String()
	s.mu.Unlock()

	// oversized header closes this connection...
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	header := make([]byte, dahuaFrameSize)
	binary.LittleEndian.PutUint32(header[16:20], maxPayloadSize+1)
	if _, err := conn.Write(header); err != nil {
		t.Fatalf("write: %v", err)
	}
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 1)
	if n, _ := conn.Read(buf); n > 0 {
		t.Error("server must not reply to oversized frame")
	}
	conn.Close()

	// ...but the server keeps accepting new connections
	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("second dial failed, server died: %v", err)
	}
	defer conn2.Close()
	if _, err := conn2.Write(buildDahuaFrame([]byte(`{"SerialID":"x"}`))); err != nil {
		t.Fatalf("write: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(s.GetLog(10)) == 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Error("second event was not received")
}

func TestPublishMQTTWithoutClientIsSafe(t *testing.T) {
	s := newTestServer(t, 0)
	s.PublishMQTT(testEvent("cam", "Start", time.Now())) // must not panic
}

func TestEventDirIsolation(t *testing.T) {
	s := newTestServer(t, 0)
	if s.EventDir() == AlarmDir {
		t.Fatal("test server must not write to the default alarm dir")
	}
}
