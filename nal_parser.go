package main

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

const (
	nalTypeVPS    = 32
	nalTypeSPS    = 33
	nalTypePPS    = 34
	nalTypeIDR    = 19
	nalTypeIDR2   = 20
	nalTypeSEI    = 39
	nalTypeSEI2   = 40
	nalTypeSEI3   = 41
	nalTypeSEI4   = 42
	nalTypeSEI5   = 43
	nalTypeSEI6   = 44
	nalTypeTrailN = 0
	nalTypeTrailR = 1

	nalTypeH264SPS = 7
	nalTypeH264PPS = 8
	nalTypeH264IDR = 5
)

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

func fetchGo2RTCStreamInfo(go2rtcURL, camera string) (*go2rtcStreamInfo, error) {
	if go2rtcURL == "" {
		return nil, fmt.Errorf("go2rtc URL is empty")
	}
	url := fmt.Sprintf("%s/api/streams?src=%s", go2rtcURL, camera)
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("go2rtc request: %w", err)
	}
	defer resp.Body.Close()

	var data map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, fmt.Errorf("go2rtc decode: %w", err)
	}

	camData, ok := data[camera].(map[string]interface{})
	if !ok {
		if _, hasProducers := data["producers"]; hasProducers {
			camData = data
		} else {
			return nil, fmt.Errorf("camera %s not found in go2rtc", camera)
		}
	}

	info := &go2rtcStreamInfo{}

	producers, ok := camData["producers"].([]interface{})
	if !ok || len(producers) == 0 {
		return nil, fmt.Errorf("no producers for %s", camera)
	}
	prod := producers[0].(map[string]interface{})

	receivers, ok := prod["receivers"].([]interface{})
	if !ok || len(receivers) == 0 {
		return nil, fmt.Errorf("no receivers for %s", camera)
	}
	recv := receivers[0].(map[string]interface{})
	codec := recv["codec"].(map[string]interface{})
	info.CodecName = codec["codec_name"].(string)

	if info.CodecName == "h264" {
		info.Profile = codec["profile"].(string)
		info.Level = int(codec["level"].(float64))
	}

	consumers, ok := camData["consumers"].([]interface{})
	if ok && len(consumers) > 0 {
		cons := consumers[0].(map[string]interface{})
		sdp, _ := cons["sdp"].(string)
		if sdp != "" {
			parseSDP(sdp, info)
		}
	}

	if info.SPS == nil && info.PPS == nil && info.VPS == nil {
		if producers, ok := camData["producers"].([]interface{}); ok && len(producers) > 0 {
			prod := producers[0].(map[string]interface{})
			sdp, _ := prod["sdp"].(string)
			if sdp != "" {
				parseSDP(sdp, info)
			}
		}
	}

	if info.Width == 0 && info.Height == 0 && info.SPS != nil {
		info.Width, info.Height = parseSPSResolution(info.SPS)
	}

	return info, nil
}

func parseSDP(sdp string, info *go2rtcStreamInfo) {
	lines := strings.Split(sdp, "\r\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "a=sprop-parameter-sets=") {
			parts := strings.Split(strings.TrimPrefix(line, "a=sprop-parameter-sets="), ",")
			if len(parts) >= 2 {
				sps, _ := base64.StdEncoding.DecodeString(parts[0])
				pps, _ := base64.StdEncoding.DecodeString(parts[1])
				info.SPS = sps
				info.PPS = pps
			}
		} else if strings.HasPrefix(line, "a=sprop-vps=") {
			vps, _ := base64.StdEncoding.DecodeString(strings.TrimPrefix(line, "a=sprop-vps="))
			info.VPS = vps
		} else if strings.HasPrefix(line, "a=sprop-sps=") {
			sps, _ := base64.StdEncoding.DecodeString(strings.TrimPrefix(line, "a=sprop-sps="))
			info.SPS = sps
		} else if strings.HasPrefix(line, "a=sprop-pps=") {
			pps, _ := base64.StdEncoding.DecodeString(strings.TrimPrefix(line, "a=sprop-pps="))
			info.PPS = pps
		} else if strings.HasPrefix(line, "a=fmtp:") {
			if idx := strings.Index(line, "sprop-parameter-sets="); idx != -1 {
				rest := line[idx+len("sprop-parameter-sets="):]
				if idx2 := strings.Index(rest, ";"); idx2 != -1 {
					rest = rest[:idx2]
				}
				parts := strings.Split(rest, ",")
				if len(parts) >= 2 {
					sps, _ := base64.StdEncoding.DecodeString(parts[0])
					pps, _ := base64.StdEncoding.DecodeString(parts[1])
					info.SPS = sps
					info.PPS = pps
				}
			}
			if idx := strings.Index(line, "sprop-vps="); idx != -1 {
				rest := line[idx+len("sprop-vps="):]
				if idx2 := strings.Index(rest, ";"); idx2 != -1 {
					rest = rest[:idx2]
				}
				vps, _ := base64.StdEncoding.DecodeString(rest)
				info.VPS = vps
			}
			if idx := strings.Index(line, "a=sprop-sps="); idx != -1 {
				rest := line[idx+len("a=sprop-sps="):]
				if idx2 := strings.Index(rest, ";"); idx2 != -1 {
					rest = rest[:idx2]
				}
				sps, _ := base64.StdEncoding.DecodeString(rest)
				info.SPS = sps
			}
			if idx := strings.Index(line, "a=sprop-pps="); idx != -1 {
				rest := line[idx+len("a=sprop-pps="):]
				if idx2 := strings.Index(rest, ";"); idx2 != -1 {
					rest = rest[:idx2]
				}
				pps, _ := base64.StdEncoding.DecodeString(rest)
				info.PPS = pps
			}
		}
	}
}

func findMdatDataOffset(data []byte, size int64) (int64, int64, error) {
	for pos := int64(0); pos < size-8; pos++ {
		atomSize := int64(binary.BigEndian.Uint32(data[pos : pos+4]))
		atomType := string(data[pos+4 : pos+8])
		if atomType == "mdat" {
			atomStart := pos
			dataOffset := pos + 8
			if atomSize == 1 {
				if pos+16 > size {
					return 0, 0, fmt.Errorf("mdat extended size out of range")
				}
				atomSize = int64(binary.BigEndian.Uint64(data[pos+8 : pos+16]))
				dataOffset = pos + 16
			}
			if atomSize == 0 || (atomSize >= 8 && pos+atomSize <= size) {
				return dataOffset, atomStart, nil
			}
		}
		if atomSize < 8 || atomSize > size-pos {
			continue
		}
		pos += atomSize - 1
	}
	return 0, 0, fmt.Errorf("mdat atom not found")
}

func findCodecAnchors(mdat []byte) (bool, bool) {
	hasHEVC := false
	hasH264 := false

	for i := 0; i < len(mdat)-4; i++ {
		length := int(binary.BigEndian.Uint32(mdat[i : i+4]))
		if length < 10 || length > 1000000 || i+4+length > len(mdat) {
			continue
		}
		b := mdat[i+4]
		hevcType := (b >> 1) & 0x3f
		h264Type := b & 0x1f

		if hevcType == nalTypeVPS || hevcType == nalTypeSPS || hevcType == nalTypePPS {
			hasHEVC = true
		}
		if h264Type == nalTypeH264SPS || h264Type == nalTypeH264PPS {
			hasH264 = true
		}

		if hasHEVC && hasH264 {
			break
		}
	}

	return hasHEVC, hasH264
}

func findValidVPS(mdat []byte) []int {
	var candidates []int
	mdatLen := len(mdat)

	for i := 0; i < mdatLen-100; i++ {
		length := int(binary.BigEndian.Uint32(mdat[i : i+4]))
		if length < 20 || length > 200 || i+4+length > mdatLen {
			continue
		}
		b := mdat[i+4]
		if (b>>7)&1 != 0 || (b>>1)&0x3f != nalTypeVPS {
			continue
		}
		candidates = append(candidates, i)
	}
	return candidates
}

func findValidSPS(mdat []byte) []int {
	var candidates []int
	mdatLen := len(mdat)

	for i := 0; i < mdatLen-100; i++ {
		length := int(binary.BigEndian.Uint32(mdat[i : i+4]))
		if length < 10 || length > 1000 || i+4+length > mdatLen {
			continue
		}
		b := mdat[i+4]
		if (b>>7)&1 != 0 || (b&0x1f) != nalTypeH264SPS {
			continue
		}
		candidates = append(candidates, i)
	}
	return candidates
}

func findValidHEVCSPS(mdat []byte) []int {
	var candidates []int
	mdatLen := len(mdat)

	for i := 0; i < mdatLen-100; i++ {
		length := int(binary.BigEndian.Uint32(mdat[i : i+4]))
		if length < 10 || length > 1000 || i+4+length > mdatLen {
			continue
		}
		b := mdat[i+4]
		if (b>>7)&1 != 0 || (b>>1)&0x3f != nalTypeSPS {
			continue
		}
		candidates = append(candidates, i)
	}
	return candidates
}

func findValidHEVCPPS(mdat []byte) []int {
	var candidates []int
	mdatLen := len(mdat)

	for i := 0; i < mdatLen-100; i++ {
		length := int(binary.BigEndian.Uint32(mdat[i : i+4]))
		if length < 10 || length > 200 || i+4+length > mdatLen {
			continue
		}
		b := mdat[i+4]
		if (b>>7)&1 != 0 || (b>>1)&0x3f != nalTypePPS {
			continue
		}
		candidates = append(candidates, i)
	}
	return candidates
}

func parseSPSResolution(sps []byte) (int, int) {
	if len(sps) < 4 {
		return 0, 0
	}
	nal := sps[1:]

	pos := 0
	readUe := func() int {
		bits := 0
		for (int(nal[pos/8]) >> (7 - (pos % 8)) & 1) == 0 {
			pos++
			bits++
		}
		pos++
		value := 0
		for i := 0; i < bits; i++ {
			value = (value << 1) | (int(nal[pos/8]) >> (7 - (pos % 8)) & 1)
			pos++
		}
		return value + (1 << bits) - 1
	}
	readSe := func() int {
		ue := readUe()
		if ue%2 == 0 {
			return -(ue / 2)
		}
		return (ue + 1) / 2
	}
	readBits := func(n int) int {
		value := 0
		for i := 0; i < n; i++ {
			value = (value << 1) | (int(nal[pos/8]) >> (7 - (pos % 8)) & 1)
			pos++
		}
		return value
	}

	profile := nal[pos/8]
	pos += 8
	_ = readBits(8)
	_ = readBits(8)
	_ = readUe()

	chromaFormatIdc := 1
	if profile >= 100 && profile <= 244 {
		chromaFormatIdc = readUe()
		if chromaFormatIdc == 3 {
			pos++
		}
		_ = readUe()
		_ = readUe()
		_ = readBits(1)
		if readBits(1) != 0 {
			numLists := 8
			if chromaFormatIdc == 3 {
				numLists = 12
			}
			for i := 0; i < numLists; i++ {
				if readBits(1) != 0 {
					lastScale := 8
					nextScale := 8
					for j := 0; j < 16; j++ {
						if nextScale != 0 {
							deltaScale := readSe()
							nextScale = (lastScale + deltaScale + 256) % 256
						}
						lastScale = nextScale
					}
				}
			}
		}
	}

	_ = readUe()
	picOrderCntType := readUe()
	if picOrderCntType == 0 {
		_ = readUe()
	} else {
		pos++
		_ = readSe()
		_ = readSe()
		numRefFrames := readUe()
		for i := 0; i < numRefFrames; i++ {
			_ = readSe()
		}
	}
	_ = readUe()
	pos++

	picWidthInMbsMinus1 := readUe()
	picHeightInMapUnitsMinus1 := readUe()
	frameMbsOnlyFlag := readBits(1)

	width := (picWidthInMbsMinus1 + 1) * 16
	height := (picHeightInMapUnitsMinus1 + 1) * 16 * (2 - frameMbsOnlyFlag)

	if readBits(1) != 0 {
		cropLeft := readUe()
		cropRight := readUe()
		cropTop := readUe()
		cropBottom := readUe()
		width -= (cropLeft + cropRight) * (1 << (chromaFormatIdc / 3))
		height -= (cropTop + cropBottom) * 2 * (1 - frameMbsOnlyFlag)
	}

	return width, height
}

func extractAllParamSets(mdat []byte, isHEVC bool) []byte {
	var paramSets []byte
	maxSets := 10
	if isHEVC {
		vpsAnchors := findValidVPS(mdat)
		spsAnchors := findValidHEVCSPS(mdat)
		ppsAnchors := findValidHEVCPPS(mdat)
		for _, i := range vpsAnchors {
			if maxSets <= 0 {
				break
			}
			length := int(binary.BigEndian.Uint32(mdat[i : i+4]))
			if length > 0 && length < 10000 && i+4+length <= len(mdat) {
				paramSets = append(paramSets, 0x00, 0x00, 0x00, 0x01)
				paramSets = append(paramSets, mdat[i+4:i+4+length]...)
				maxSets--
			}
		}
		maxSets = 10
		for _, i := range spsAnchors {
			if maxSets <= 0 {
				break
			}
			length := int(binary.BigEndian.Uint32(mdat[i : i+4]))
			if length > 0 && length < 10000 && i+4+length <= len(mdat) {
				paramSets = append(paramSets, 0x00, 0x00, 0x00, 0x01)
				paramSets = append(paramSets, mdat[i+4:i+4+length]...)
				maxSets--
			}
		}
		maxSets = 10
		for _, i := range ppsAnchors {
			if maxSets <= 0 {
				break
			}
			length := int(binary.BigEndian.Uint32(mdat[i : i+4]))
			if length > 0 && length < 10000 && i+4+length <= len(mdat) {
				paramSets = append(paramSets, 0x00, 0x00, 0x00, 0x01)
				paramSets = append(paramSets, mdat[i+4:i+4+length]...)
				maxSets--
			}
		}
	} else {
		spsAnchors := findValidSPS(mdat)
		ppsAnchors := findValidHEVCPPS(mdat)
		for _, i := range spsAnchors {
			if maxSets <= 0 {
				break
			}
			length := int(binary.BigEndian.Uint32(mdat[i : i+4]))
			if length > 0 && length < 10000 && i+4+length <= len(mdat) {
				paramSets = append(paramSets, 0x00, 0x00, 0x00, 0x01)
				paramSets = append(paramSets, mdat[i+4:i+4+length]...)
				maxSets--
			}
		}
		maxSets = 10
		for _, i := range ppsAnchors {
			if maxSets <= 0 {
				break
			}
			length := int(binary.BigEndian.Uint32(mdat[i : i+4]))
			if length > 0 && length < 10000 && i+4+length <= len(mdat) {
				paramSets = append(paramSets, 0x00, 0x00, 0x00, 0x01)
				paramSets = append(paramSets, mdat[i+4:i+4+length]...)
				maxSets--
			}
		}
	}
	return paramSets
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
