// Package go2rtc talks to the go2rtc HTTP API and understands the camera
// stream definitions stored in go2rtc.yaml.
package go2rtc

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Well-known go2rtc ports.
const (
	// APIPort is the go2rtc HTTP API port (next to the RTSP server port).
	APIPort = "1984"
	// DefaultRTSPPort is used when a camera URL has no explicit port.
	DefaultRTSPPort = "554"

	requestTimeout = 15 * time.Second
	maxBodySize    = 8 << 20 // 8 MiB
)

// Stream URL schemes supported when adding cameras through the UI.
const (
	TypeRTSP    = "rtsp"
	TypeDVRIP   = "dvrip"
	TypeONVIF   = "onvif"
	TypeISAPI   = "isapi"
	TypeUnknown = "unknown"
)

// APIBase derives the go2rtc HTTP API base URL from the configured stream
// server, e.g. "rtsp://127.0.0.1:8554" -> "http://127.0.0.1:1984".
func APIBase(streamServer string) string {
	if u, err := url.Parse(streamServer); err == nil {
		if host := u.Hostname(); host != "" {
			return "http://" + host + ":" + APIPort
		}
	}
	return "http://localhost:" + APIPort
}

// Client is a minimal go2rtc API client.
type Client struct {
	// BaseURL is the API root, see APIBase.
	BaseURL string
	// HTTP is the underlying client; defaults to a timeout-bounded client.
	HTTP *http.Client
}

// NewClient builds a client for the go2rtc instance running next to the
// configured stream server.
func NewClient(streamServer string) *Client {
	return &Client{
		BaseURL: APIBase(streamServer),
		HTTP:    &http.Client{Timeout: requestTimeout},
	}
}

func (c *Client) httpClient() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return &http.Client{Timeout: requestTimeout}
}

// Restart asks go2rtc to restart itself.
func (c *Client) Restart() error {
	resp, err := c.httpClient().Post(c.BaseURL+"/api/restart", "", nil)
	if err != nil {
		return fmt.Errorf("go2rtc restart failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("go2rtc restart returned status %d", resp.StatusCode)
	}
	return nil
}

// Info returns the decoded payload of GET /api (version, ports, ...).
func (c *Client) Info() (map[string]any, error) {
	resp, err := c.httpClient().Get(c.BaseURL + "/api")
	if err != nil {
		return nil, fmt.Errorf("go2rtc api: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("go2rtc api returned status %d", resp.StatusCode)
	}

	var info map[string]any
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxBodySize)).Decode(&info); err != nil {
		return nil, fmt.Errorf("go2rtc api decode: %w", err)
	}
	return info, nil
}

// Camera is one entry of the go2rtc "streams" mapping.
type Camera struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	URL     string `json:"url"`
	IP      string `json:"ip"`
	LimitGB int    `json:"limit_gb"`
	Channel string `json:"channel,omitempty"`
}

// CameraType reports the scheme of a stream URL.
func CameraType(streamURL string) string {
	switch {
	case strings.HasPrefix(streamURL, TypeDVRIP+"://"):
		return TypeDVRIP
	case strings.HasPrefix(streamURL, TypeRTSP+"://"):
		return TypeRTSP
	case strings.HasPrefix(streamURL, TypeONVIF+"://"):
		return TypeONVIF
	case strings.HasPrefix(streamURL, TypeISAPI+"://"):
		return TypeISAPI
	default:
		return TypeUnknown
	}
}

// CameraIP extracts the host part of a stream URL (IP or hostname).
func CameraIP(streamURL string) string {
	u, err := url.Parse(streamURL)
	if err != nil {
		return ""
	}
	if host := u.Hostname(); host != "" {
		return host
	}
	return u.Host
}

// CameraChannel extracts the "channel" query parameter of dvrip URLs.
func CameraChannel(streamURL string) string {
	if CameraType(streamURL) != TypeDVRIP {
		return ""
	}
	u, err := url.Parse(streamURL)
	if err != nil {
		return ""
	}
	return u.Query().Get("channel")
}

// BuildURL assembles a go2rtc stream URL for the given camera type. The
// credentials block is always emitted ("user:pass@") to match the format
// historically written into go2rtc.yaml.
func BuildURL(cameraType, user, pass, ip, port, channel string) (string, error) {
	creds := user + ":" + pass + "@"

	switch cameraType {
	case TypeRTSP:
		if port == "" {
			port = DefaultRTSPPort
		}
		return fmt.Sprintf("%s://%s%s:%s/stream0", TypeRTSP, creds, ip, port), nil
	case TypeDVRIP:
		if channel == "" {
			channel = "0"
		}
		return fmt.Sprintf("%s://%s%s?channel=%s&subtype=0", TypeDVRIP, creds, ip, channel), nil
	case TypeONVIF:
		return fmt.Sprintf("%s://%s%s", TypeONVIF, creds, ip), nil
	case TypeISAPI:
		return fmt.Sprintf("%s://%s%s", TypeISAPI, creds, ip), nil
	default:
		return "", fmt.Errorf("unsupported type: %q", cameraType)
	}
}
