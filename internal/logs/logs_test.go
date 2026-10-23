package logs

import (
	"log"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDetectLevel(t *testing.T) {
	cases := map[string]string{
		"ERROR: disk full":        LevelError,
		"WARNING: slow stream":    LevelWarn,
		"WARN retrying":           LevelWarn,
		"Fatal: cannot start":     LevelFatal,
		"debug detail":            LevelDebug,
		"plain message":           LevelInfo,
		"error: lowercase prefix": LevelError,
		"no error here":           LevelInfo,
		"2026/01/02 03:04:05 x":   LevelInfo,
	}
	for msg, want := range cases {
		if got := DetectLevel(msg); got != want {
			t.Errorf("DetectLevel(%q) = %q, want %q", msg, got, want)
		}
	}
}

func TestWriteStripsTimestampAndCounts(t *testing.T) {
	b := NewLogBuffer(10)

	n, err := b.Write([]byte("2026/01/02 03:04:05 starting up\n"))
	if err != nil || n != len("2026/01/02 03:04:05 starting up\n") {
		t.Fatalf("Write = (%d, %v)", n, err)
	}
	if b.Count() != 1 {
		t.Fatalf("Count = %d", b.Count())
	}

	entry := b.GetLogs(10, "", "")[0]
	if entry.Message != "starting up" {
		t.Errorf("Message = %q", entry.Message)
	}
	if entry.Level != LevelInfo {
		t.Errorf("Level = %q", entry.Level)
	}
	if entry.Time.IsZero() {
		t.Error("Time not set")
	}

	// whitespace-only writes are ignored
	if _, err := b.Write([]byte("   \n")); err != nil {
		t.Fatal(err)
	}
	if b.Count() != 1 {
		t.Errorf("Count after blank write = %d", b.Count())
	}
}

func TestRingBufferKeepsNewest(t *testing.T) {
	b := NewLogBuffer(3)
	for i := 0; i < 5; i++ {
		b.Write([]byte(strings.Repeat("x", i+1)))
	}

	if b.Count() != 3 {
		t.Fatalf("Count = %d, want 3", b.Count())
	}
	got := b.GetLogs(10, "", "")
	if len(got) != 3 {
		t.Fatalf("len(GetLogs) = %d", len(got))
	}
	// newest first
	wantOrder := []string{"xxxxx", "xxxx", "xxx"}
	for i, w := range wantOrder {
		if got[i].Message != w {
			t.Errorf("entry[%d] = %q, want %q", i, got[i].Message, w)
		}
	}
}

func TestGetLogsLimit(t *testing.T) {
	b := NewLogBuffer(100)
	for i := 0; i < 10; i++ {
		b.Write([]byte{byte('a' + i)})
	}

	got := b.GetLogs(4, "", "")
	if len(got) != 4 {
		t.Fatalf("len = %d, want 4", len(got))
	}
	if got[0].Message != "j" || got[3].Message != "g" {
		t.Errorf("unexpected slice: %v", messages(got))
	}
	if unlimited := b.GetLogs(0, "", ""); len(unlimited) != 10 {
		t.Errorf("limit 0 should return everything, got %d", len(unlimited))
	}
}

func TestGetLogsLevelFilter(t *testing.T) {
	b := NewLogBuffer(100)
	write := func(msg string) { b.Write([]byte(msg)) }
	write("INFO one")
	write("WARN two")
	write("ERROR three")
	write("DEBUG zero")

	if got := b.GetLogs(10, "", LevelWarn); len(got) != 2 {
		t.Errorf("level=WARN got %d entries: %v", len(got), messages(got))
	}
	if got := b.GetLogs(10, "", LevelInfo); len(got) != 3 {
		t.Errorf("level=INFO got %d entries: %v", len(got), messages(got))
	}
	if got := b.GetLogs(10, "", LevelDebug); len(got) != 4 {
		t.Errorf("level=DEBUG got %d entries", len(got))
	}
	if got := b.GetLogs(10, "", "nonsense"); len(got) != 4 {
		t.Errorf("unknown filter must keep everything, got %d", len(got))
	}
	if got := b.GetLogs(10, "", "all"); len(got) != 4 {
		t.Errorf("filter=all got %d", len(got))
	}
}

func TestGetLogsSinceFilter(t *testing.T) {
	b := NewLogBuffer(100)
	old := LogEntry{Time: time.Now().Add(-time.Hour), Level: LevelInfo, Message: "old"}
	fresh := LogEntry{Time: time.Now(), Level: LevelInfo, Message: "fresh"}
	b.Add(old)
	b.Add(fresh)

	since := time.Now().Add(-time.Minute).Format(time.RFC3339)
	got := b.GetLogs(10, since, "")
	if len(got) != 1 || got[0].Message != "fresh" {
		t.Errorf("since filter returned %v", messages(got))
	}

	// invalid since timestamp is ignored
	if got := b.GetLogs(10, "not-a-time", ""); len(got) != 2 {
		t.Errorf("invalid since should keep everything, got %d", len(got))
	}
}

func TestClear(t *testing.T) {
	b := NewLogBuffer(5)
	b.Write([]byte("hello"))
	b.Clear()
	if b.Count() != 0 {
		t.Fatalf("Count = %d", b.Count())
	}
	if got := b.GetLogs(10, "", ""); len(got) != 0 {
		t.Errorf("GetLogs after clear = %v", messages(got))
	}
	// buffer stays usable after clear
	b.Write([]byte("again"))
	if b.Count() != 1 {
		t.Errorf("Count = %d", b.Count())
	}
}

func TestNewLogBufferRejectsNonPositiveCapacity(t *testing.T) {
	b := NewLogBuffer(0)
	if _, err := b.Write([]byte("x")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if b.Count() != 1 {
		t.Errorf("Count = %d", b.Count())
	}
}

func TestConcurrentAccess(t *testing.T) {
	b := NewLogBuffer(64)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				b.Write([]byte("concurrent line"))
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				b.GetLogs(10, "", LevelInfo)
				b.Count()
			}
		}()
	}
	wg.Wait()

	if b.Count() > 64 {
		t.Errorf("Count = %d exceeds capacity", b.Count())
	}
}

func TestRedirectLogOutput(t *testing.T) {
	b := NewLogBuffer(10)
	prev := log.Writer()
	defer log.SetOutput(prev)

	RedirectLogOutput(b) // also writes to stderr, which is fine for a test
	log.Print("from std logger")

	if b.Count() != 1 {
		t.Fatalf("buffer did not receive std logger output: %d", b.Count())
	}
	if msg := b.GetLogs(1, "", "")[0].Message; !strings.Contains(msg, "from std logger") {
		t.Errorf("Message = %q", msg)
	}
}

func messages(entries []LogEntry) []string {
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Message)
	}
	return out
}
