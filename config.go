package main

import (
	"os"

	"gopkg.in/yaml.v3"
)

type NVRConfig struct {
	BaseDir          string `yaml:"base_dir" json:"base_dir"`
	ArchiveDir       string `yaml:"archive_dir" json:"archive_dir"`
	StreamServer     string `yaml:"stream_server" json:"stream_server"`
	TargetSizeGB     int    `yaml:"target_size_gb" json:"target_size_gb"`
	Go2RTCConfigPath string `yaml:"go2rtc_config_path" json:"go2rtc_config_path"`
	HTTPPort         int    `yaml:"http_port" json:"http_port"`
	AlarmEnabled     bool   `yaml:"alarm_enabled" json:"alarm_enabled"`
	AlarmPort        int    `yaml:"alarm_port" json:"alarm_port"`
	MQTTHost         string `yaml:"mqtt_host" json:"mqtt_host"`
	MQTTPort         int    `yaml:"mqtt_port" json:"mqtt_port"`
	MQTTUser         string `yaml:"mqtt_user" json:"mqtt_user"`
	MQTTPass         string `yaml:"mqtt_pass" json:"mqtt_pass"`
	AlarmCommand     string `yaml:"alarm_command" json:"alarm_command"`
}

type Go2RTCConfig struct {
	StreamOrder []string
	Streams     map[string]any
}

func loadNVRConfig(path string) (*NVRConfig, error) {
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
	if cfg.TargetSizeGB == 0 {
		cfg.TargetSizeGB = 90
	}
	if cfg.AlarmPort == 0 {
		cfg.AlarmPort = 15002
	}
	if cfg.MQTTPort == 0 {
		cfg.MQTTPort = 1883
	}
	if cfg.ArchiveDir == "" {
		cfg.ArchiveDir = cfg.BaseDir + "/archive"
	}
	return &cfg, nil
}

func loadGo2RTCConfig(path string) (*Go2RTCConfig, error) {
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
	return cfg, nil
}

func saveNVRConfig(path string, cfg *NVRConfig) error {
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}
