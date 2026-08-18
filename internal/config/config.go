package config

import (
	"fmt"
	"net"
	"net/url"
	"os"

	"gopkg.in/yaml.v3"
)

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

type Go2RTCConfig struct {
	StreamOrder []string
	Streams     map[string]any
	IPMap       map[string]string
}

func LoadNVRConfig(path string) (*NVRConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil && path != "/etc/simple-nvr/nvr.yaml" {
		data, err = os.ReadFile("/etc/simple-nvr/nvr.yaml")
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	var cfg NVRConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	if cfg.HTTPPort == 0 {
		cfg.HTTPPort = 8180
	}
	if cfg.DefaultCameraLimitGB == 0 {
		cfg.DefaultCameraLimitGB = 90
	}
	if cfg.GlobalSizeGB == 0 {
		cfg.GlobalSizeGB = 500
	}
	if cfg.AlarmPort == 0 {
		cfg.AlarmPort = 15002
	}
	if cfg.HikvisionAlarmPort == 0 {
		cfg.HikvisionAlarmPort = 15003
	}
	if cfg.MQTTPort == 0 {
		cfg.MQTTPort = 1883
	}
	if cfg.KioskPort == 0 {
		cfg.KioskPort = 8181
	}
	if cfg.ArchiveDir == "" {
		cfg.ArchiveDir = cfg.BaseDir + "/archive"
	}
	if cfg.UsersFile == "" {
		cfg.UsersFile = "/etc/simple-nvr/users.yaml"
	}
	if err := ValidateConfig(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func ValidateConfig(cfg *NVRConfig) error {
	if cfg.BaseDir == "" {
		return fmt.Errorf("base_dir is required")
	}
	if cfg.StreamServer == "" {
		return fmt.Errorf("stream_server is required")
	}
	if cfg.Go2RTCConfigPath == "" {
		return fmt.Errorf("go2rtc_config_path is required")
	}
	return nil
}

func LoadGo2RTCConfig(path string) (*Go2RTCConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	cfg := &Go2RTCConfig{Streams: make(map[string]any)}
	if doc.Kind != yaml.DocumentNode || len(doc.Content) == 0 {
		return cfg, nil
	}
	root := doc.Content[0]
	if root.Kind != yaml.MappingNode {
		return cfg, nil
	}
	for i := 0; i < len(root.Content)-1; i += 2 {
		if root.Content[i].Value == "streams" {
			streamsNode := root.Content[i+1]
			if streamsNode.Kind == yaml.MappingNode {
				for j := 0; j < len(streamsNode.Content)-1; j += 2 {
					key := streamsNode.Content[j].Value
					var val any
					_ = streamsNode.Content[j+1].Decode(&val)
					cfg.StreamOrder = append(cfg.StreamOrder, key)
					cfg.Streams[key] = val
				}
			}
			break
		}
	}
	cfg.IPMap = BuildIPMap(cfg.Streams)
	return cfg, nil
}

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

func SaveNVRConfig(path string, cfg *NVRConfig) error {
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func SaveGo2RTCConfig(path string, streams map[string]any, order []string) error {
	doc := &yaml.Node{Kind: yaml.DocumentNode}
	root := &yaml.Node{Kind: yaml.MappingNode}
	streamsNode := &yaml.Node{Kind: yaml.MappingNode}

	for _, name := range order {
		val, ok := streams[name]
		if !ok {
			continue
		}
		keyNode := &yaml.Node{Kind: yaml.ScalarNode, Value: name, Tag: "!!str"}
		valNode := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: fmt.Sprintf("%v", val)}
		streamsNode.Content = append(streamsNode.Content, keyNode, valNode)
	}

	keyStreams := &yaml.Node{Kind: yaml.ScalarNode, Value: "streams", Tag: "!!str"}
	root.Content = append(root.Content, keyStreams, streamsNode)
	doc.Content = append(doc.Content, root)

	data, err := yaml.Marshal(doc)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}
