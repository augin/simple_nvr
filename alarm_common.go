package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

const (
	maxAlarmLog   = 500
	alarmDir      = "/var/lib/simple-nvr/alarms"
	retentionDays = 30
)

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
