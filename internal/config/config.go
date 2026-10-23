// Package config loads, validates and persists NVR and go2rtc configuration.
package config

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"slices"

	"gopkg.in/yaml.v3"
)

// Paths and defaults used when the config does not specify them.
const (
	// FallbackConfigPath is used when the requested config file cannot be read.
	FallbackConfigPath = "/etc/simple-nvr/nvr.yaml"

	defaultUsersFile     = "/etc/simple-nvr/users.yaml"
	defaultHTTPPort      = 8180
	defaultKioskPort     = 8181
	defaultCameraLimitGB = 90
	defaultGlobalSizeGB  = 500
	defaultAlarmPort     = 15002
	defaultHikvisionPort = 15003
	defaultMQTTPort      = 1883
	unlimited            = 0
	permissionsConfig    = 0644
)

// fallbackConfigPath is a variable so tests can point it elsewhere.
var fallbackConfigPath = FallbackConfigPath

// NVRConfig is the main application configuration (nvr.yaml).
type NVRConfig struct {
	BaseDir              string         `yaml:"base_dir" json:"base_dir"`
	ArchiveDir           string         `yaml:"archive_dir" json:"archive_dir"`
	StreamServer         string         `yaml:"stream_server" json:"stream_server"`
	DefaultCameraLimitGB int            `yaml:"default_camera_limit_gb" json:"default_camera_limit_gb"`
	GlobalSizeGB         int            `yaml:"global_size_gb" json:"global_size_gb"`
	Go2RTCConfigPath     string         `yaml:"go2rtc_config_path" json:"go2rtc_config_path"`
	HTTPPort             int            `yaml:"http_port" json:"http_port"`
	AlarmEnabled         bool           `yaml:"alarm_enabled" json:"alarm_enabled"`
	AlarmPort            int            `yaml:"alarm_port" json:"alarm_port"`
	HikvisionEnabled     bool           `yaml:"hikvision_enabled" json:"hikvision_enabled"`
	HikvisionAlarmPort   int            `yaml:"hikvision_alarm_port" json:"hikvision_alarm_port"`
	MQTTHost             string         `yaml:"mqtt_host" json:"mqtt_host"`
	MQTTPort             int            `yaml:"mqtt_port" json:"mqtt_port"`
	MQTTUser             string         `yaml:"mqtt_user" json:"mqtt_user"`
	MQTTPass             string         `yaml:"mqtt_pass" json:"mqtt_pass"`
	AlarmCommand         string         `yaml:"alarm_command" json:"alarm_command"`
	CameraLimits         map[string]int `yaml:"camera_limits" json:"camera_limits"`
	CameraDayLimits      map[string]int `yaml:"camera_day_limits" json:"camera_day_limits"`
	UsersFile            string         `yaml:"users_file" json:"users_file"`
	KioskEnabled         bool           `yaml:"kiosk_enabled" json:"kiosk_enabled"`
	KioskPort            int            `yaml:"kiosk_port" json:"kiosk_port"`
}

// Go2RTCConfig is the parsed go2rtc.yaml: stream names in declaration order,
// their raw values and an IP -> stream name map used by the alarm servers.
type Go2RTCConfig struct {
	StreamOrder []string
	Streams     map[string]any
	IPMap       map[string]string
}

// LoadNVRConfig reads the config file, falling back to FallbackConfigPath when
// the requested path cannot be read.
func LoadNVRConfig(path string) (*NVRConfig, error) {
	data, err := readNVRFile(path)
	if err != nil {
		return nil, err
	}
	return ParseNVRConfig(data)
}

func readNVRFile(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err == nil || path == fallbackConfigPath {
		return data, err
	}
	// Requested path is unreadable: retry the well-known fallback location.
	return os.ReadFile(fallbackConfigPath)
}

// ParseNVRConfig parses YAML, applies defaults and validates the result.
func ParseNVRConfig(data []byte) (*NVRConfig, error) {
	var cfg NVRConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// ApplyDefaults fills unset fields with their default values.
func (c *NVRConfig) ApplyDefaults() {
	if c.HTTPPort == 0 {
		c.HTTPPort = defaultHTTPPort
	}
	if c.DefaultCameraLimitGB == unlimited {
		c.DefaultCameraLimitGB = defaultCameraLimitGB
	}
	if c.GlobalSizeGB == unlimited {
		c.GlobalSizeGB = defaultGlobalSizeGB
	}
	if c.AlarmPort == 0 {
		c.AlarmPort = defaultAlarmPort
	}
	if c.HikvisionAlarmPort == 0 {
		c.HikvisionAlarmPort = defaultHikvisionPort
	}
	if c.MQTTPort == 0 {
		c.MQTTPort = defaultMQTTPort
	}
	if c.KioskPort == 0 {
		c.KioskPort = defaultKioskPort
	}
	if c.ArchiveDir == "" {
		c.ArchiveDir = c.BaseDir + "/archive"
	}
	if c.UsersFile == "" {
		c.UsersFile = defaultUsersFile
	}
}

// Validate reports whether required fields are present.
func (c *NVRConfig) Validate() error {
	if c.BaseDir == "" {
		return fmt.Errorf("base_dir is required")
	}
	if c.StreamServer == "" {
		return fmt.Errorf("stream_server is required")
	}
	if c.Go2RTCConfigPath == "" {
		return fmt.Errorf("go2rtc_config_path is required")
	}
	return nil
}

// Clone returns a deep copy of the config (maps are copied).
func (c *NVRConfig) Clone() *NVRConfig {
	clone := *c
	clone.CameraLimits = copyIntMap(c.CameraLimits)
	clone.CameraDayLimits = copyIntMap(c.CameraDayLimits)
	return &clone
}

// UpdateFrom copies over the fields that are editable through the web UI.
// Zero values are ignored for numeric settings so a partial payload cannot
// disable ports or wipe limits by accident.
func (c *NVRConfig) UpdateFrom(in *NVRConfig) {
	c.BaseDir = in.BaseDir
	c.ArchiveDir = in.ArchiveDir
	c.StreamServer = in.StreamServer
	c.DefaultCameraLimitGB = in.DefaultCameraLimitGB
	c.GlobalSizeGB = in.GlobalSizeGB
	c.Go2RTCConfigPath = in.Go2RTCConfigPath
	if in.HTTPPort > 0 {
		c.HTTPPort = in.HTTPPort
	}
	c.AlarmEnabled = in.AlarmEnabled
	if in.AlarmPort > 0 {
		c.AlarmPort = in.AlarmPort
	}
	c.HikvisionEnabled = in.HikvisionEnabled
	if in.HikvisionAlarmPort > 0 {
		c.HikvisionAlarmPort = in.HikvisionAlarmPort
	}
	c.MQTTHost = in.MQTTHost
	if in.MQTTPort > 0 {
		c.MQTTPort = in.MQTTPort
	}
	c.MQTTUser = in.MQTTUser
	c.MQTTPass = in.MQTTPass
	c.AlarmCommand = in.AlarmCommand
	if in.CameraLimits != nil {
		c.CameraLimits = in.CameraLimits
	}
	if in.CameraDayLimits != nil {
		c.CameraDayLimits = in.CameraDayLimits
	}
	if in.UsersFile != "" {
		c.UsersFile = in.UsersFile
	}
	c.KioskEnabled = in.KioskEnabled
	if in.KioskPort > 0 {
		c.KioskPort = in.KioskPort
	}
}

func copyIntMap(in map[string]int) map[string]int {
	if in == nil {
		return nil
	}
	out := make(map[string]int, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// LoadGo2RTCConfig parses go2rtc.yaml preserving stream declaration order.
func LoadGo2RTCConfig(path string) (*Go2RTCConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return ParseGo2RTCConfig(data)
}

// ParseGo2RTCConfig parses go2rtc.yaml content.
func ParseGo2RTCConfig(data []byte) (*Go2RTCConfig, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, err
	}

	cfg := &Go2RTCConfig{Streams: make(map[string]any)}
	root := documentRoot(&doc)
	if root == nil {
		return cfg, nil
	}

	streams := mappingValue(root, "streams")
	if streams == nil || streams.Kind != yaml.MappingNode {
		return cfg, nil
	}
	for i := 0; i+1 < len(streams.Content); i += 2 {
		key := streams.Content[i].Value
		var val any
		_ = streams.Content[i+1].Decode(&val)
		cfg.StreamOrder = append(cfg.StreamOrder, key)
		cfg.Streams[key] = val
	}

	cfg.IPMap = BuildIPMap(cfg.Streams)
	return cfg, nil
}

func documentRoot(doc *yaml.Node) *yaml.Node {
	if doc.Kind != yaml.DocumentNode || len(doc.Content) == 0 {
		return nil
	}
	root := doc.Content[0]
	if root.Kind != yaml.MappingNode {
		return nil
	}
	return root
}

func mappingValue(node *yaml.Node, key string) *yaml.Node {
	for i := 0; i+1 < len(node.Content); i += 2 {
		if node.Content[i].Value == key {
			return node.Content[i+1]
		}
	}
	return nil
}

// BuildIPMap maps stream IPs to stream names (only literal IP hosts).
func BuildIPMap(streams map[string]any) map[string]string {
	ipMap := make(map[string]string)
	for name, val := range streams {
		urlStr, ok := val.(string)
		if !ok {
			continue
		}
		u, err := url.Parse(urlStr)
		if err != nil {
			continue
		}
		host := u.Hostname()
		if net.ParseIP(host) != nil {
			ipMap[host] = name
		}
	}
	return ipMap
}

// SaveNVRConfig writes the config as YAML.
func SaveNVRConfig(path string, cfg *NVRConfig) error {
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, permissionsConfig)
}

// SaveGo2RTCConfig writes streams back to go2rtc.yaml keeping the given order.
// Values keep their original YAML type (string, list, ...).
func SaveGo2RTCConfig(path string, streams map[string]any, order []string) error {
	doc := &yaml.Node{Kind: yaml.DocumentNode}
	root := &yaml.Node{Kind: yaml.MappingNode}
	streamsNode := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}

	for _, name := range order {
		val, ok := streams[name]
		if !ok {
			continue
		}
		keyNode := &yaml.Node{Kind: yaml.ScalarNode, Value: name, Tag: "!!str"}
		valNode := &yaml.Node{}
		if err := valNode.Encode(val); err != nil {
			return fmt.Errorf("encode stream %q: %w", name, err)
		}
		streamsNode.Content = append(streamsNode.Content, keyNode, valNode)
	}

	keyStreams := &yaml.Node{Kind: yaml.ScalarNode, Value: "streams", Tag: "!!str"}
	root.Content = append(root.Content, keyStreams, streamsNode)
	doc.Content = append(doc.Content, root)

	data, err := yaml.Marshal(doc)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, permissionsConfig)
}

// AddStream adds or replaces a stream and keeps it in the order list.
func (c *Go2RTCConfig) AddStream(name, streamURL string) {
	c.Streams[name] = streamURL
	if !slices.Contains(c.StreamOrder, name) {
		c.StreamOrder = append(c.StreamOrder, name)
	}
	c.IPMap = BuildIPMap(c.Streams)
}

// RemoveStream deletes a stream; reports whether it existed.
func (c *Go2RTCConfig) RemoveStream(name string) bool {
	if _, ok := c.Streams[name]; !ok {
		return false
	}
	delete(c.Streams, name)
	order := make([]string, 0, len(c.StreamOrder))
	for _, n := range c.StreamOrder {
		if n != name {
			order = append(order, n)
		}
	}
	c.StreamOrder = order
	c.IPMap = BuildIPMap(c.Streams)
	return true
}

// ReorderStream moves a stream from index from to index to.
func (c *Go2RTCConfig) ReorderStream(from, to int) (string, error) {
	if from < 0 || from >= len(c.StreamOrder) || to < 0 || to >= len(c.StreamOrder) {
		return "", fmt.Errorf("invalid position")
	}
	name := c.StreamOrder[from]
	without := make([]string, 0, len(c.StreamOrder)-1)
	for i, n := range c.StreamOrder {
		if i == from {
			continue
		}
		without = append(without, n)
	}
	restored := make([]string, 0, len(c.StreamOrder))
	restored = append(restored, without[:to]...)
	restored = append(restored, name)
	restored = append(restored, without[to:]...)
	c.StreamOrder = restored
	return name, nil
}

// Save writes the config back to path.
func (c *Go2RTCConfig) Save(path string) error {
	return SaveGo2RTCConfig(path, c.Streams, c.StreamOrder)
}
