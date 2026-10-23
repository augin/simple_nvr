package alarm

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// AlarmEvent is a single alarm notification from any source.
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

// Publish stores the event in memory, journals it to disk and pushes it to
// MQTT. It is the common path for every alarm source.
func (s *AlarmServer) Publish(event AlarmEvent) {
	s.AddLog(event)
	s.SaveEvent(event)
	s.PublishMQTT(event)
}

// AddLog appends an event to the in-memory log, dropping the oldest entry
// when MaxAlarmLog is exceeded.
func (s *AlarmServer) AddLog(event AlarmEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.log = append(s.log, event)
	if len(s.log) > MaxAlarmLog {
		s.log = s.log[len(s.log)-MaxAlarmLog:]
	}
}

// SaveEvent appends an event to the journal of its day (thread safe).
func (s *AlarmServer) SaveEvent(event AlarmEvent) {
	data, err := json.Marshal(event)
	if err != nil {
		log.Printf("Alarm marshal error: %v", err)
		return
	}

	s.fileMu.Lock()
	defer s.fileMu.Unlock()

	filePath := filepath.Join(s.dir, event.Time.Format("2006-01-02")+eventsFileSuffix)
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, permissionsFile)
	if err != nil {
		log.Printf("Alarm file open error: %v", err)
		return
	}
	defer f.Close()

	if _, err := f.Write(append(data, '\n')); err != nil {
		log.Printf("Alarm file write error: %v", err)
	}
}

// GetLog returns the most recent events, newest first.
func (s *AlarmServer) GetLog(limit int) []AlarmEvent {
	s.mu.Lock()
	events := make([]AlarmEvent, len(s.log))
	copy(events, s.log)
	s.mu.Unlock()

	n := len(events)
	if limit > 0 && limit < n {
		n = limit
	}

	result := make([]AlarmEvent, n)
	copy(result, events[len(events)-n:])

	sort.Slice(result, func(i, j int) bool {
		return result[i].Time.After(result[j].Time)
	})
	return result
}

// ClearLog empties the in-memory event log.
func (s *AlarmServer) ClearLog() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log = s.log[:0]
}

// LoadRecentEvents preloads journals of the last "days" days into memory.
func (s *AlarmServer) LoadRecentEvents(days int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loadRecentEvents(days)
}

func (s *AlarmServer) loadRecentEvents(days int) {
	s.fileMu.Lock()
	dir := s.dir
	s.fileMu.Unlock()

	now := time.Now()
	for i := days - 1; i >= 0; i-- {
		filePath := filepath.Join(dir, now.AddDate(0, 0, -i).Format("2006-01-02")+eventsFileSuffix)
		events, err := ReadEventsFile(filePath)
		if err != nil {
			continue
		}
		s.log = append(s.log, events...)
	}

	if len(s.log) > MaxAlarmLog {
		s.log = s.log[len(s.log)-MaxAlarmLog:]
	}
}

// cleanupOldEvents removes journals older than RetentionDays.
func (s *AlarmServer) cleanupOldEvents() {
	threshold := time.Now().AddDate(0, 0, -RetentionDays).Format("2006-01-02")

	s.fileMu.Lock()
	dir := s.dir
	s.fileMu.Unlock()

	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), eventsFileSuffix) {
			continue
		}
		dateStr := strings.TrimSuffix(entry.Name(), eventsFileSuffix)
		if dateStr >= threshold {
			continue
		}
		if err := os.Remove(filepath.Join(dir, entry.Name())); err != nil {
			log.Printf("Alarm cleanup error removing %s: %v", entry.Name(), err)
		} else {
			log.Printf("Alarm: cleaned up old log %s", entry.Name())
		}
	}
}

// cleanupLoop removes expired journals once an hour until stopCh is closed.
func (s *AlarmServer) cleanupLoop(stopCh <-chan struct{}) {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			s.cleanupOldEvents()
		}
	}
}
