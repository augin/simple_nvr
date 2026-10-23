package alarm

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestGetString(t *testing.T) {
	m := map[string]any{"s": "value", "n": float64(3), "nil": nil}
	if got := getString(m, "s"); got != "value" {
		t.Errorf("getString = %q", got)
	}
	if got := getString(m, "n"); got != "" {
		t.Errorf("non-string must be empty, got %q", got)
	}
	if got := getString(m, "missing"); got != "" {
		t.Errorf("missing = %q", got)
	}
}

func TestGetInt(t *testing.T) {
	m := map[string]any{"f": float64(7), "i": 42, "s": "text", "nil": nil}
	if got := getInt(m, "f"); got != 7 {
		t.Errorf("float = %d", got)
	}
	if got := getInt(m, "i"); got != 42 {
		t.Errorf("int = %d", got)
	}
	if got := getInt(m, "s"); got != 0 {
		t.Errorf("string = %d", got)
	}
	if got := getInt(m, "missing"); got != 0 {
		t.Errorf("missing = %d", got)
	}
}

func TestDecodeAddress(t *testing.T) {
	cases := []struct {
		in   any
		want string
	}{
		{"0A01A8C0", "192.168.1.10"},
		{"0x0A01A8C0", "192.168.1.10"},
		{"0X0A01A8C0", "192.168.1.10"},
		{float64(0x0A01A8C0), "192.168.1.10"},
		{"", ""},
		{"not-hex", ""},
		{nil, ""},
		{true, ""},
	}
	for _, tc := range cases {
		if got := decodeAddress(tc.in); got != tc.want {
			t.Errorf("decodeAddress(%v) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestReadDahuaFrame(t *testing.T) {
	payload := []byte(`{"SerialID":"abc"}`)
	frame := buildDahuaFrame(payload)

	got, err := readDahuaFrame(bytes.NewReader(frame))
	if err != nil {
		t.Fatalf("readDahuaFrame: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("payload = %q", got)
	}
}

func TestReadDahuaFrameErrors(t *testing.T) {
	// empty stream
	if _, err := readDahuaFrame(bytes.NewReader(nil)); err != io.EOF {
		t.Errorf("empty: err = %v, want EOF", err)
	}

	// truncated payload
	short := buildDahuaFrame([]byte("0123456789"))
	if _, err := readDahuaFrame(bytes.NewReader(short[:len(short)-3])); err == nil {
		t.Error("truncated payload must fail")
	}

	// oversized frame header
	header := make([]byte, dahuaFrameSize)
	binary.LittleEndian.PutUint32(header[16:20], maxPayloadSize+1)
	if _, err := readDahuaFrame(bytes.NewReader(header)); err == nil {
		t.Error("oversized frame must fail")
	}

	// truncated header
	if _, err := readDahuaFrame(bytes.NewReader(header[:10])); err == nil {
		t.Error("truncated header must fail")
	}
}

func TestParseDahuaPayload(t *testing.T) {
	payload := []byte(`{
		"SerialID": "dev-1",
		"Type": "Alarm",
		"Event": "MotionDetection",
		"Channel": 2,
		"Status": "Start",
		"Descrip": "motion in yard",
		"Address": "0A01A8C0"
	}`)

	event, err := parseDahuaPayload(payload, map[string]string{"192.168.1.10": "yard"})
	if err != nil {
		t.Fatalf("parseDahuaPayload: %v", err)
	}
	if event.SerialID != "dev-1" || event.Type != "Alarm" || event.Event != "MotionDetection" {
		t.Errorf("event = %+v", event)
	}
	if event.Channel != 2 || event.Status != "Start" || event.Descrip != "motion in yard" {
		t.Errorf("event = %+v", event)
	}
	if event.Address != "192.168.1.10" || event.Camera != "yard" {
		t.Errorf("address/camera = %q/%q", event.Address, event.Camera)
	}

	// unknown address keeps camera empty
	event, err = parseDahuaPayload(payload, nil)
	if err != nil {
		t.Fatalf("parseDahuaPayload: %v", err)
	}
	if event.Camera != "" {
		t.Errorf("camera = %q, want empty without ipMap", event.Camera)
	}

	if _, err := parseDahuaPayload([]byte("not json"), nil); err == nil {
		t.Error("invalid json must fail")
	}
}

func TestReadEventsFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "2026-10-23.jsonl")
	content := `{"time":"2026-10-23T10:00:00Z","camera":"cam1","event":"Motion"}
not json

{"time":"2026-10-23T11:00:00Z","camera":"cam2","event":"Line"}
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	events, err := ReadEventsFile(path)
	if err != nil {
		t.Fatalf("ReadEventsFile: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("events = %d, want 2", len(events))
	}
	if events[0].Camera != "cam1" || events[1].Camera != "cam2" {
		t.Errorf("events = %+v", events)
	}

	if _, err := ReadEventsFile(filepath.Join(dir, "absent.jsonl")); err == nil {
		t.Error("missing file must fail")
	}
}
