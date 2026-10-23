package api

import (
	"bytes"
	"context"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

type brokenFile struct {
	Camera string `json:"camera"`
	Date   string `json:"date"`
	File   string `json:"file"`
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	Error  string `json:"error"`
}

// scanState tracks one background scan. Methods are the only writers so the
// mutex discipline stays in one place.
type scanState struct {
	mu          sync.Mutex
	running     bool
	total       int
	checked     int
	brokenCount int
	current     string
	brokenFiles []brokenFile
	seen        map[string]bool
	err         string
}

// start flips the scan to running; it reports false when one is already active.
func (s *scanState) start() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		return false
	}
	s.running = true
	s.total = 0
	s.checked = 0
	s.brokenCount = 0
	s.current = ""
	s.brokenFiles = nil
	s.seen = make(map[string]bool)
	s.err = ""
	return true
}

func (s *scanState) finish(err string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.running = false
	s.current = ""
	if err != "" {
		s.err = err
	}
}

func (s *scanState) setTotal(n int) {
	s.mu.Lock()
	s.total = n
	s.mu.Unlock()
}

// counters reports the final totals for logging.
func (s *scanState) counters() (total, broken int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.checked, s.brokenCount
}

func (s *scanState) setCurrent(name string) {
	s.mu.Lock()
	s.current = name
	s.mu.Unlock()
}

// record folds one finished file into the totals. A path is only reported
// broken once even if re-queued.
func (s *scanState) record(res scanResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checked++
	if res.broken {
		if s.seen == nil {
			s.seen = make(map[string]bool)
		}
		if !s.seen[res.path] {
			s.seen[res.path] = true
			s.brokenCount++
			s.brokenFiles = append(s.brokenFiles, brokenFile{
				Camera: res.camera,
				Date:   res.date,
				File:   res.name,
				Path:   res.path,
				Size:   res.size,
				Error:  res.err,
			})
		}
	}
}

func (s *scanState) snapshot() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	resp := map[string]any{
		"running": s.running,
		"total":   s.total,
		"checked": s.checked,
		"broken":  s.brokenCount,
		"current": s.current,
		"results": s.brokenFiles,
	}
	if !s.running && s.err != "" {
		resp["error"] = s.err
	}
	return resp
}

// scanFile is one discovered MP4 waiting to be checked.
type scanFile struct {
	camera string
	path   string
	name   string
	size   int64
	date   string
}

type scanResult struct {
	camera string
	path   string
	name   string
	size   int64
	date   string
	broken bool
	err    string
}

// relDate extracts the "year/month" folder from a path relative to base.
func relDate(base, path string) string {
	rel, err := filepath.Rel(base, path)
	if err != nil {
		return ""
	}
	parts := strings.SplitN(rel, string(filepath.Separator), 3)
	if len(parts) < 3 {
		return ""
	}
	return filepath.Dir(parts[1] + "/" + parts[2])
}

// collectMP4s gathers every .mp4 under each camera folder of base.
func collectMP4s(base string) []scanFile {
	entries, err := os.ReadDir(base)
	if err != nil {
		return nil
	}

	var files []scanFile
	for _, cameraEntry := range entries {
		if !cameraEntry.IsDir() {
			continue
		}
		camera := cameraEntry.Name()
		cameraDir := filepath.Join(base, camera)

		filepath.Walk(cameraDir, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || !strings.HasSuffix(info.Name(), ".mp4") {
				return nil
			}
			files = append(files, scanFile{
				camera: camera,
				path:   path,
				name:   info.Name(),
				size:   info.Size(),
				date:   relDate(base, path),
			})
			return nil
		})
	}
	return files
}

// probeFFmpeg reports whether ffprobe flags the file as damaged. Active
// recordings are skipped by the caller.
func probeFFmpeg(path string) (broken bool, msg string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "ffprobe", "-v", "error",
		"-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", path)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	_ = cmd.Run()
	return stderr.Len() > 0, strings.TrimSpace(stderr.String())
}

// runScan checks every recorded MP4 with ffprobe, in parallel, and updates
// the scan state as it goes.
func (a *API) runScan() {
	files := collectMP4s(a.config.BaseDir)
	if len(files) == 0 {
		if _, err := os.ReadDir(a.config.BaseDir); err != nil {
			a.toolsScan.finish(err.Error())
			log.Printf("tools scan: %v", err)
			return
		}
	}
	a.toolsScan.setTotal(len(files))

	activeRec := a.recorder.ActiveRecordings()

	workers := runtime.NumCPU()
	if workers < 2 {
		workers = 2
	}
	if workers > 8 {
		workers = 8
	}

	fileCh := make(chan scanFile, len(files))
	for _, f := range files {
		fileCh <- f
	}
	close(fileCh)

	resultCh := make(chan scanResult, len(files))
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for f := range fileCh {
				if activeRec[f.path] {
					resultCh <- scanResult{camera: f.camera, path: f.path, name: f.name, size: f.size, date: f.date}
					continue
				}
				a.toolsScan.setCurrent(f.camera + "/" + f.date + "/" + f.name)
				broken, msg := probeFFmpeg(f.path)
				resultCh <- scanResult{camera: f.camera, path: f.path, name: f.name, size: f.size, date: f.date, broken: broken, err: msg}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	for res := range resultCh {
		a.toolsScan.record(res)
	}

	a.toolsScan.finish("")
	total, broken := a.toolsScan.counters()
	log.Printf("tools scan: %d files checked, %d broken", total, broken)
}
