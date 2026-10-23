// Package logs keeps a bounded in-memory buffer of application log lines for
// the web UI. It implements io.Writer so it can be plugged into the std logger.
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

// Log levels, ordered by severity in levelOrder.
const (
	LevelDebug = "DEBUG"
	LevelInfo  = "INFO"
	LevelWarn  = "WARN"
	LevelError = "ERROR"
	LevelFatal = "FATAL"
)

// goLogPrefix matches the standard logger timestamp ("2006/01/02 15:04:05 ").
var goLogPrefix = regexp.MustCompile(`^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} `)

// levelOrder assigns severity so a filter can select everything above it.
var levelOrder = map[string]int{
	LevelDebug: 0,
	LevelInfo:  1,
	LevelWarn:  2,
	LevelError: 3,
	LevelFatal: 4,
}

// LogEntry is a single buffered log line.
type LogEntry struct {
	Time    time.Time `json:"time"`
	Level   string    `json:"level"`
	Message string    `json:"message"`
}

// LogBuffer is a thread-safe ring buffer of log entries.
type LogBuffer struct {
	mu       sync.RWMutex
	entries  []LogEntry
	capacity int
	head     int // next slot to write
	count    int // number of stored entries
}

// NewLogBuffer creates a buffer holding at most capacity entries.
func NewLogBuffer(capacity int) *LogBuffer {
	if capacity < 1 {
		capacity = 1
	}
	return &LogBuffer{
		entries:  make([]LogEntry, capacity),
		capacity: capacity,
	}
}

// Write stores one log line; it never fails and always consumes the input.
func (b *LogBuffer) Write(p []byte) (int, error) {
	msg := strings.TrimSpace(string(p))
	if msg == "" {
		return len(p), nil
	}

	b.Add(LogEntry{
		Time:    time.Now(),
		Level:   DetectLevel(msg),
		Message: goLogPrefix.ReplaceAllString(msg, ""),
	})
	return len(p), nil
}

// Add appends an entry, dropping the oldest one when the buffer is full.
func (b *LogBuffer) Add(entry LogEntry) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.entries[b.head] = entry
	b.head = (b.head + 1) % b.capacity
	if b.count < b.capacity {
		b.count++
	}
}

// DetectLevel guesses the level from the message prefix.
func DetectLevel(msg string) string {
	upper := strings.ToUpper(msg)
	for _, level := range []string{LevelFatal, LevelError, "WARNING", LevelWarn, LevelDebug} {
		if strings.HasPrefix(upper, level) {
			if level == "WARNING" {
				return LevelWarn
			}
			return level
		}
	}
	return LevelInfo
}

// levelIncluded reports whether an entry passes the minimum-level filter.
// Unknown filters keep everything (same as no filter).
func levelIncluded(entryLevel, filter string) bool {
	if filter == "" || filter == "all" {
		return true
	}
	minLevel, ok := levelOrder[strings.ToUpper(filter)]
	if !ok {
		return true
	}
	entry, ok := levelOrder[entryLevel]
	if !ok {
		return true
	}
	return entry >= minLevel
}

// GetLogs returns matching entries newest first: the tail of the buffer after
// filtering by "since" (RFC3339) and level, limited to the last "limit" items.
func (b *LogBuffer) GetLogs(limit int, since, level string) []LogEntry {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var sinceTime time.Time
	if since != "" {
		sinceTime, _ = time.Parse(time.RFC3339, since)
	}

	result := make([]LogEntry, 0, b.count)

	start := (b.head - b.count + b.capacity) % b.capacity
	for i := 0; i < b.count; i++ {
		entry := b.entries[(start+i)%b.capacity]

		if !sinceTime.IsZero() && entry.Time.Before(sinceTime) {
			continue
		}
		if !levelIncluded(entry.Level, level) {
			continue
		}
		result = append(result, entry)
	}

	// Keep only the newest matches, then present them newest first.
	if limit > 0 && limit < len(result) {
		result = result[len(result)-limit:]
	}
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}
	return result
}

// Clear empties the buffer.
func (b *LogBuffer) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.head = 0
	b.count = 0
}

// Count reports how many entries are stored.
func (b *LogBuffer) Count() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.count
}

// RedirectLogOutput sends the std logger both to the buffer and to stderr.
func RedirectLogOutput(buf *LogBuffer) {
	log.SetOutput(io.MultiWriter(buf, os.Stderr))
}
