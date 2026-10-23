package recovery

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	go2rtcTimeout = 10 * time.Second
	maxInfoSize   = 8 << 20 // 8 MiB
)

// go2rtcStreamInfo is the codec configuration of a live stream as reported by
// go2rtc (used to pick the container codec and to recover dimensions).
type go2rtcStreamInfo struct {
	CodecName string
	Profile   string
	Level     int
	SPS       []byte
	PPS       []byte
	VPS       []byte
	Width     int
	Height    int
}

var infoHTTPClient = &http.Client{Timeout: go2rtcTimeout}

// fetchGo2RTCStreamInfo asks go2rtc about the codec of a stream. Any problem
// (no go2rtc, camera offline, unexpected payload) is reported as an error and
// the caller falls back to scanning the file.
func fetchGo2RTCStreamInfo(go2rtcURL, camera string) (*go2rtcStreamInfo, error) {
	if go2rtcURL == "" {
		return nil, fmt.Errorf("go2rtc URL is empty")
	}

	endpoint := fmt.Sprintf("%s/api/streams?src=%s", go2rtcURL, url.QueryEscape(camera))
	resp, err := infoHTTPClient.Get(endpoint)
	if err != nil {
		return nil, fmt.Errorf("go2rtc request: %w", err)
	}
	defer resp.Body.Close()

	var data map[string]any
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxInfoSize)).Decode(&data); err != nil {
		return nil, fmt.Errorf("go2rtc decode: %w", err)
	}

	streamData, err := pickStreamData(data, camera)
	if err != nil {
		return nil, err
	}

	info := &go2rtcStreamInfo{}
	if err := applyCodecInfo(streamData, camera, info); err != nil {
		return nil, err
	}

	// Parameter sets usually live in the consumer SDP, sometimes in the
	// producer one.
	if !hasParamSets(info) {
		applyFirstSDP(streamData, "consumers", info)
	}
	if !hasParamSets(info) {
		applyFirstSDP(streamData, "producers", info)
	}

	if info.Width == 0 && info.Height == 0 && info.SPS != nil {
		info.Width, info.Height = parseSPSResolution(info.SPS)
	}
	return info, nil
}

// pickStreamData unwraps the "<camera>": {...} envelope of /api/streams.
func pickStreamData(data map[string]any, camera string) (map[string]any, error) {
	if camData, ok := data[camera].(map[string]any); ok {
		return camData, nil
	}
	// go2rtc may answer with a bare stream object when queried by src.
	if _, ok := data["producers"]; ok {
		return data, nil
	}
	return nil, fmt.Errorf("camera %s not found in go2rtc", camera)
}

// applyCodecInfo fills codec name/profile/level from the first receiver.
func applyCodecInfo(streamData map[string]any, camera string, info *go2rtcStreamInfo) error {
	producers, ok := streamData["producers"].([]any)
	if !ok || len(producers) == 0 {
		return fmt.Errorf("no producers for %s", camera)
	}
	producer, ok := producers[0].(map[string]any)
	if !ok {
		return fmt.Errorf("malformed producer for %s", camera)
	}

	receivers, ok := producer["receivers"].([]any)
	if !ok || len(receivers) == 0 {
		return fmt.Errorf("no receivers for %s", camera)
	}
	receiver, ok := receivers[0].(map[string]any)
	if !ok {
		return fmt.Errorf("malformed receiver for %s", camera)
	}

	codec, ok := receiver["codec"].(map[string]any)
	if !ok {
		return fmt.Errorf("no codec for %s", camera)
	}
	name, _ := codec["codec_name"].(string)
	if name == "" {
		return fmt.Errorf("no codec name for %s", camera)
	}
	info.CodecName = name

	if name == "h264" {
		if profile, ok := codec["profile"].(string); ok {
			info.Profile = profile
		}
		if level, ok := codec["level"].(float64); ok {
			info.Level = int(level)
		}
	}
	return nil
}

// applyFirstSDP parses the SDP of the first entry of the given list.
func applyFirstSDP(streamData map[string]any, key string, info *go2rtcStreamInfo) {
	list, ok := streamData[key].([]any)
	if !ok || len(list) == 0 {
		return
	}
	entry, ok := list[0].(map[string]any)
	if !ok {
		return
	}
	sdp, _ := entry["sdp"].(string)
	if sdp != "" {
		parseSDP(sdp, info)
	}
}

func hasParamSets(info *go2rtcStreamInfo) bool {
	return info.SPS != nil || info.PPS != nil || info.VPS != nil
}

// parseSDP extracts sprop parameter sets from an SDP description.
func parseSDP(sdp string, info *go2rtcStreamInfo) {
	for _, line := range strings.Split(sdp, "\r\n") {
		switch {
		case strings.HasPrefix(line, "a=sprop-parameter-sets="):
			applySpropParameterSets(line[len("a=sprop-parameter-sets="):], info)
		case strings.HasPrefix(line, "a=sprop-vps="):
			info.VPS = decodeBase64(line[len("a=sprop-vps="):])
		case strings.HasPrefix(line, "a=sprop-sps="):
			info.SPS = decodeBase64(line[len("a=sprop-sps="):])
		case strings.HasPrefix(line, "a=sprop-pps="):
			info.PPS = decodeBase64(line[len("a=sprop-pps="):])
		case strings.HasPrefix(line, "a=fmtp:"):
			parseFmtpLine(line, info)
		}
	}
}

// parseFmtpLine handles sprop attributes embedded in a=fmtp: lines.
func parseFmtpLine(line string, info *go2rtcStreamInfo) {
	if value, ok := fmtpValue(line, "sprop-parameter-sets="); ok {
		applySpropParameterSets(value, info)
	}
	if value, ok := fmtpValue(line, "sprop-vps="); ok {
		info.VPS = decodeBase64(value)
	}
	if value, ok := fmtpValue(line, "sprop-sps="); ok {
		info.SPS = decodeBase64(value)
	}
	if value, ok := fmtpValue(line, "sprop-pps="); ok {
		info.PPS = decodeBase64(value)
	}
}

// fmtpValue extracts "<key>value" from an fmtp line, stopping at ";".
func fmtpValue(line, key string) (string, bool) {
	idx := strings.Index(line, key)
	if idx < 0 {
		return "", false
	}
	value := line[idx+len(key):]
	if end := strings.Index(value, ";"); end >= 0 {
		value = value[:end]
	}
	return value, true
}

// applySpropParameterSets decodes the "sps,pps" pair of sprop-parameter-sets.
func applySpropParameterSets(value string, info *go2rtcStreamInfo) {
	parts := strings.Split(value, ",")
	if len(parts) < 2 {
		return
	}
	info.SPS = decodeBase64(parts[0])
	info.PPS = decodeBase64(parts[1])
}

// decodeBase64 decodes one sprop element, returning nil when it is malformed.
// SDP sprop values are frequently unpadded, so padding is restored here.
func decodeBase64(value string) []byte {
	s := strings.TrimSpace(value)
	if len(s) == 0 {
		return nil
	}
	if m := len(s) % 4; m != 0 {
		s += strings.Repeat("=", 4-m)
	}
	decoded, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil
	}
	return decoded
}
