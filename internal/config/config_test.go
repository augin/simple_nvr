package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestParseNVRConfigDefaults(t *testing.T) {
	cfg, err := ParseNVRConfig([]byte("base_dir: /data\nstream_server: rtsp://127.0.0.1:8554\ngo2rtc_config_path: /etc/go2rtc/go2rtc.yaml\n"))
	if err != nil {
		t.Fatalf("ParseNVRConfig: %v", err)
	}

	checks := map[string]struct {
		got  int
		want int
	}{
		"HTTPPort":             {cfg.HTTPPort, 8180},
		"DefaultCameraLimitGB": {cfg.DefaultCameraLimitGB, 90},
		"GlobalSizeGB":         {cfg.GlobalSizeGB, 500},
		"AlarmPort":            {cfg.AlarmPort, 15002},
		"HikvisionAlarmPort":   {cfg.HikvisionAlarmPort, 15003},
		"MQTTPort":             {cfg.MQTTPort, 1883},
		"KioskPort":            {cfg.KioskPort, 8181},
	}
	for name, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %d, want %d", name, c.got, c.want)
		}
	}
	if cfg.ArchiveDir != "/data/archive" {
		t.Errorf("ArchiveDir = %q, want %q", cfg.ArchiveDir, "/data/archive")
	}
	if cfg.UsersFile != "/etc/simple-nvr/users.yaml" {
		t.Errorf("UsersFile = %q", cfg.UsersFile)
	}
}

func TestParseNVRConfigExplicitValuesKept(t *testing.T) {
	cfg, err := ParseNVRConfig([]byte(`
base_dir: /video
archive_dir: /arch
stream_server: rtsp://cam:8554
go2rtc_config_path: /go2rtc.yaml
http_port: 9999
default_camera_limit_gb: 5
global_size_gb: 7
kiosk_port: 1234
users_file: /u.yaml
`))
	if err != nil {
		t.Fatalf("ParseNVRConfig: %v", err)
	}
	if cfg.HTTPPort != 9999 || cfg.DefaultCameraLimitGB != 5 || cfg.GlobalSizeGB != 7 || cfg.KioskPort != 1234 {
		t.Errorf("explicit values overwritten: %+v", cfg)
	}
	if cfg.ArchiveDir != "/arch" || cfg.UsersFile != "/u.yaml" {
		t.Errorf("explicit dirs overwritten: %+v", cfg)
	}
}

func TestParseNVRConfigValidation(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		err  string
	}{
		{"missing base_dir", "stream_server: rtsp://x\ngo2rtc_config_path: /g.yaml\n", "base_dir is required"},
		{"missing stream_server", "base_dir: /d\ngo2rtc_config_path: /g.yaml\n", "stream_server is required"},
		{"missing go2rtc path", "base_dir: /d\nstream_server: rtsp://x\n", "go2rtc_config_path is required"},
		{"invalid yaml", "base_dir: [1,2\n", "parse config"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseNVRConfig([]byte(tc.yaml))
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tc.err) {
				t.Errorf("error %q does not contain %q", err, tc.err)
			}
		})
	}
}

func TestLoadNVRConfigReadsFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nvr.yaml")
	content := "base_dir: /v\nstream_server: rtsp://s\ngo2rtc_config_path: /g.yaml\nhttp_port: 8123\n"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadNVRConfig(path)
	if err != nil {
		t.Fatalf("LoadNVRConfig: %v", err)
	}
	if cfg.HTTPPort != 8123 || cfg.BaseDir != "/v" {
		t.Errorf("unexpected config: %+v", cfg)
	}
}

func TestLoadNVRConfigMissingFile(t *testing.T) {
	orig := fallbackConfigPath
	fallbackConfigPath = filepath.Join(t.TempDir(), "absent.yaml")
	defer func() { fallbackConfigPath = orig }()

	if _, err := LoadNVRConfig(filepath.Join(t.TempDir(), "absent.yaml")); err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestLoadNVRConfigFallsBackToDefaultPath(t *testing.T) {
	dir := t.TempDir()
	fallback := filepath.Join(dir, "nvr.yaml")
	if err := os.WriteFile(fallback, []byte("base_dir: /fallback\nstream_server: rtsp://s\ngo2rtc_config_path: /g.yaml\n"), 0644); err != nil {
		t.Fatal(err)
	}
	orig := fallbackConfigPath
	fallbackConfigPath = fallback
	defer func() { fallbackConfigPath = orig }()

	cfg, err := LoadNVRConfig(filepath.Join(dir, "missing.yaml"))
	if err != nil {
		t.Fatalf("LoadNVRConfig: %v", err)
	}
	if cfg.BaseDir != "/fallback" {
		t.Errorf("BaseDir = %q, want fallback config content", cfg.BaseDir)
	}
}

func TestSaveAndReloadNVRConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nvr.yaml")
	cfg, err := ParseNVRConfig([]byte("base_dir: /v\nstream_server: rtsp://s\ngo2rtc_config_path: /g.yaml\n"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.CameraLimits = map[string]int{"cam1": 10}
	cfg.CameraDayLimits = map[string]int{"cam1": 3}

	if err := SaveNVRConfig(path, cfg); err != nil {
		t.Fatalf("SaveNVRConfig: %v", err)
	}
	reloaded, err := LoadNVRConfig(path)
	if err != nil {
		t.Fatalf("LoadNVRConfig: %v", err)
	}
	if !reflect.DeepEqual(cfg, reloaded) {
		t.Errorf("round trip mismatch:\n%+v\n%+v", cfg, reloaded)
	}
}

func TestCloneCopiesMaps(t *testing.T) {
	cfg := &NVRConfig{CameraLimits: map[string]int{"a": 1}}
	clone := cfg.Clone()
	clone.CameraLimits["a"] = 2
	if cfg.CameraLimits["a"] != 1 {
		t.Error("Clone shares the CameraLimits map")
	}
	empty := (&NVRConfig{}).Clone()
	if empty.CameraLimits != nil || empty.CameraDayLimits != nil {
		t.Error("Clone of empty config should keep nil maps")
	}
}

func TestUpdateFromKeepsPortsOnZero(t *testing.T) {
	cfg, err := ParseNVRConfig([]byte("base_dir: /v\nstream_server: rtsp://s\ngo2rtc_config_path: /g.yaml\nhttp_port: 8180\n"))
	if err != nil {
		t.Fatal(err)
	}
	in := &NVRConfig{BaseDir: "/new", StreamServer: "rtsp://t", Go2RTCConfigPath: "/g2.yaml"}
	cfg.UpdateFrom(in)

	if cfg.BaseDir != "/new" || cfg.StreamServer != "rtsp://t" || cfg.Go2RTCConfigPath != "/g2.yaml" {
		t.Errorf("paths not updated: %+v", cfg)
	}
	if cfg.HTTPPort != 8180 || cfg.AlarmPort != 15002 || cfg.KioskPort != 8181 || cfg.MQTTPort != 1883 {
		t.Errorf("zero values wiped defaults: %+v", cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Errorf("config invalid after update: %v", err)
	}
}

func TestParseGo2RTCConfigOrderAndTypes(t *testing.T) {
	data := []byte(`
api:
  listen: ':1984'
streams:
  cam1: rtsp://admin:pass@192.168.1.10:554/stream0
  cam2: dvrip://admin:pass@192.168.1.11?channel=0&subtype=0
  group:
    - rtsp://192.168.1.10/stream0
    - rtsp://192.168.1.11/stream0
`)
	cfg, err := ParseGo2RTCConfig(data)
	if err != nil {
		t.Fatalf("ParseGo2RTCConfig: %v", err)
	}
	want := []string{"cam1", "cam2", "group"}
	if !reflect.DeepEqual(cfg.StreamOrder, want) {
		t.Errorf("StreamOrder = %v, want %v", cfg.StreamOrder, want)
	}
	group, ok := cfg.Streams["group"].([]any)
	if !ok || len(group) != 2 {
		t.Errorf("group stream should stay a list, got %#v", cfg.Streams["group"])
	}
	if cfg.IPMap["192.168.1.10"] != "cam1" {
		t.Errorf("IPMap = %v", cfg.IPMap)
	}
	if cfg.IPMap["192.168.1.11"] != "cam2" {
		t.Errorf("IPMap misses dvrip host: %v", cfg.IPMap)
	}
	if _, exists := cfg.IPMap["camera.local"]; exists {
		t.Errorf("hostnames must not be in IPMap: %v", cfg.IPMap)
	}
}

func TestParseGo2RTCConfigEmptyAndInvalid(t *testing.T) {
	for _, data := range []string{"", "\n", "api:\n  listen: ':1984'\n", "streams: []\n"} {
		cfg, err := ParseGo2RTCConfig([]byte(data))
		if err != nil {
			t.Fatalf("ParseGo2RTCConfig(%q): %v", data, err)
		}
		if cfg.Streams == nil {
			t.Errorf("Streams map must be initialized for %q", data)
		}
	}
	if _, err := ParseGo2RTCConfig([]byte("streams:\n  - [1\n")); err == nil {
		t.Error("expected error for broken yaml")
	}
}

func TestSaveGo2RTCConfigKeepsTypesAndOrder(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "go2rtc.yaml")

	cfg := &Go2RTCConfig{
		StreamOrder: []string{"second", "first"},
		Streams: map[string]any{
			"first":  "rtsp://192.168.0.1/stream0",
			"second": []any{"rtsp://192.168.0.2/stream0", "rtsp://192.168.0.3/stream0"},
			"gone":   "rtsp://192.168.0.4/stream0",
		},
	}
	if err := cfg.Save(path); err != nil {
		t.Fatalf("Save: %v", err)
	}

	reloaded, err := LoadGo2RTCConfig(path)
	if err != nil {
		t.Fatalf("LoadGo2RTCConfig: %v", err)
	}
	if !reflect.DeepEqual(reloaded.StreamOrder, cfg.StreamOrder) {
		t.Errorf("StreamOrder = %v, want %v", reloaded.StreamOrder, cfg.StreamOrder)
	}
	if _, ok := reloaded.Streams["gone"]; ok {
		t.Error("stream missing from order must not be written")
	}
	if list, ok := reloaded.Streams["second"].([]any); !ok || len(list) != 2 {
		t.Errorf("list stream type lost: %#v", reloaded.Streams["second"])
	}
}

func TestGo2RTCConfigAddRemoveReorder(t *testing.T) {
	cfg := &Go2RTCConfig{
		StreamOrder: []string{"a"},
		Streams:     map[string]any{"a": "rtsp://1.2.3.4/x"},
	}

	cfg.AddStream("b", "rtsp://5.6.7.8/x")
	cfg.AddStream("a", "rtsp://1.2.3.4/y") // update existing
	if !reflect.DeepEqual(cfg.StreamOrder, []string{"a", "b"}) {
		t.Errorf("StreamOrder = %v", cfg.StreamOrder)
	}
	if cfg.Streams["a"] != "rtsp://1.2.3.4/y" {
		t.Errorf("stream not updated: %v", cfg.Streams)
	}

	if _, err := cfg.ReorderStream(1, 0); err != nil {
		t.Fatalf("ReorderStream: %v", err)
	}
	if !reflect.DeepEqual(cfg.StreamOrder, []string{"b", "a"}) {
		t.Errorf("after reorder = %v", cfg.StreamOrder)
	}
	if _, err := cfg.ReorderStream(0, 5); err == nil {
		t.Error("expected error for out of range position")
	}
	if _, err := cfg.ReorderStream(-1, 0); err == nil {
		t.Error("expected error for negative position")
	}

	if !cfg.RemoveStream("b") {
		t.Error("RemoveStream(b) = false")
	}
	if cfg.RemoveStream("b") {
		t.Error("RemoveStream of missing camera = true")
	}
	if !reflect.DeepEqual(cfg.StreamOrder, []string{"a"}) {
		t.Errorf("after remove = %v", cfg.StreamOrder)
	}
	if cfg.IPMap["1.2.3.4"] != "a" {
		t.Errorf("IPMap not refreshed: %v", cfg.IPMap)
	}

	// stream known only from the map gets repaired into the order list
	broken := &Go2RTCConfig{Streams: map[string]any{"x": "rtsp://1.1.1.1/x"}}
	broken.AddStream("x", "rtsp://1.1.1.1/y")
	if !reflect.DeepEqual(broken.StreamOrder, []string{"x"}) {
		t.Errorf("StreamOrder = %v, want [x]", broken.StreamOrder)
	}
}

func TestBuildIPMap(t *testing.T) {
	ipMap := BuildIPMap(map[string]any{
		"ipv4":  "rtsp://admin@10.0.0.5:554/h264",
		"name":  "rtsp://camera.local/h264",
		"list":  []any{"rtsp://10.0.0.6/x"},
		"bad":   "://broken",
		"empty": "",
	})
	if len(ipMap) != 1 || ipMap["10.0.0.5"] != "ipv4" {
		t.Errorf("IPMap = %v", ipMap)
	}
}

func TestSaveNVRConfigProducesParsableYAML(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nvr.yaml")
	cfg := &NVRConfig{BaseDir: "/v", StreamServer: "rtsp://s", Go2RTCConfigPath: "/g.yaml"}
	if err := SaveNVRConfig(path, cfg); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var round NVRConfig
	if err := yaml.Unmarshal(data, &round); err != nil {
		t.Fatalf("saved yaml is invalid: %v", err)
	}
}
