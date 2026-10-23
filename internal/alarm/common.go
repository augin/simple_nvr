package alarm

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
)

const (
	// MaxAlarmLog is the maximum number of events kept in memory.
	MaxAlarmLog = 500
	// AlarmDir is the default directory for daily event journals.
	AlarmDir = "/var/lib/simple-nvr/alarms"
	// RetentionDays is how long daily journals are kept on disk.
	RetentionDays = 30

	// dahuaFrameSize is the fixed header size of the Dahua/XM alarm protocol.
	dahuaFrameSize = 20
	// maxPayloadSize rejects absurd frames to bound memory usage.
	maxPayloadSize = 1024 * 1024

	permissionsDir   = 0755
	permissionsFile  = 0644
	eventsFileSuffix = ".jsonl"
)

// getString reads a string value from a decoded JSON object.
func getString(m map[string]any, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

// getInt reads a numeric value from a decoded JSON object (JSON numbers are
// float64, integers are accepted too).
func getInt(m map[string]any, key string) int {
	switch n := m[key].(type) {
	case float64:
		return int(n)
	case int:
		return n
	}
	return 0
}

// decodeAddress converts the device address field into a dotted IPv4 string.
// The device sends either a hex string ("0A01A8C0") or a plain number; the
// octets are little endian (first byte is the last IP octet).
func decodeAddress(raw any) string {
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

// readDahuaFrame reads one length-prefixed JSON payload from the stream.
// Frame layout: <20 byte header, length as LE uint32 at offset 16><payload>.
func readDahuaFrame(r io.Reader) ([]byte, error) {
	header := make([]byte, dahuaFrameSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}

	contentLen := binary.LittleEndian.Uint32(header[16:20])
	if contentLen > maxPayloadSize {
		return nil, fmt.Errorf("payload too large: %d bytes", contentLen)
	}

	payload := make([]byte, contentLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

// buildDahuaFrame assembles a frame for tests/clients.
func buildDahuaFrame(payload []byte) []byte {
	frame := make([]byte, dahuaFrameSize+len(payload))
	binary.LittleEndian.PutUint32(frame[16:20], uint32(len(payload)))
	copy(frame[dahuaFrameSize:], payload)
	return frame
}

// parseDahuaPayload converts a JSON payload into an AlarmEvent, resolving the
// camera name through ipMap (address -> stream name).
func parseDahuaPayload(payload []byte, ipMap map[string]string) (AlarmEvent, error) {
	var raw map[string]any
	if err := json.Unmarshal(payload, &raw); err != nil {
		return AlarmEvent{}, fmt.Errorf("invalid json: %w", err)
	}

	event := AlarmEvent{
		SerialID: getString(raw, "SerialID"),
		Type:     getString(raw, "Type"),
		Event:    getString(raw, "Event"),
		Channel:  getInt(raw, "Channel"),
		Status:   getString(raw, "Status"),
		Descrip:  getString(raw, "Descrip"),
		Address:  decodeAddress(raw["Address"]),
	}
	if ipMap != nil {
		event.Camera = ipMap[event.Address]
	}
	return event, nil
}

// ReadEventsFile loads a daily journal, skipping unparsable lines.
func ReadEventsFile(path string) ([]AlarmEvent, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var events []AlarmEvent
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), maxPayloadSize)

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
