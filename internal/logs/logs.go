package logs

import (
	"io"
	"log"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

var goLogPrefix = regexp.MustCompile(`^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} `)

type LogEntry struct {
	Time    time.Time `json:"time"`
	Level   string    `json:"level"`
	Message string    `json:"message"`
}

type LogBuffer struct {
	mu      sync.Mutex
	entries []LogEntry
	cap     int
	head    int
	count   int
}

func NewLogBuffer(capacity int) *LogBuffer {
	return &LogBuffer{
		entries: make([]LogEntry, capacity),
		cap:     capacity,
	}
}

func (b *LogBuffer) Write(p []byte) (n int, err error) {
	msg := strings.TrimSpace(string(p))
	if msg == "" {
		return len(p), nil
	}

	msg = goLogPrefix.ReplaceAllString(msg, "")

	level := "INFO"
	upper := strings.ToUpper(msg)
	for _, l := range []string{"FATAL", "ERROR", "WARNING", "WARN", "DEBUG"} {
		if strings.HasPrefix(upper, l) {
			level = l
			if level == "WARNING" {
				level = "WARN"
			}
			break
		}
	}

	entry := LogEntry{
		Time:    time.Now(),
		Level:   level,
		Message: msg,
	}

	b.mu.Lock()
	b.entries[b.head] = entry
	b.head = (b.head + 1) % b.cap
	if b.count < b.cap {
		b.count++
	}
	b.mu.Unlock()

	return len(p), nil
}

func (b *LogBuffer) GetLogs(limit int, since string) []LogEntry {
	b.mu.Lock()
	defer b.mu.Unlock()

	var sinceTime time.Time
	if since != "" {
		sinceTime, _ = time.Parse(time.RFC3339, since)
	}

	result := make([]LogEntry, 0, limit)

	start := (b.head - b.count + b.cap) % b.cap
	for i := 0; i < b.count; i++ {
		idx := (start + i) % b.cap
		entry := b.entries[idx]

		if !sinceTime.IsZero() && entry.Time.Before(sinceTime) {
			continue
		}

		result = append(result, entry)
	}

	n := len(result)
	if limit > 0 && limit < n {
		result = result[n-limit:]
	}

	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}

	return result
}

func (b *LogBuffer) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.head = 0
	b.count = 0
}

func (b *LogBuffer) Count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.count
}

func RedirectLogOutput(buf *LogBuffer) {
	multi := io.MultiWriter(buf, os.Stderr)
	log.SetOutput(multi)
}
