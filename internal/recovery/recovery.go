package recovery

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"time"
)

func tryFFmpegMovRepair(badPath string) (bool, error) {
	fixedPath := badPath + ".fixed"
	args := []string{"-v", "warning", "-y",
		"-fflags", "+genpts+discardcorrupt",
		"-err_detect", "ignore_err",
		"-f", "mov", "-i", badPath,
		"-c", "copy", "-movflags", "+faststart", "-f", "mp4", fixedPath}

	cmd := exec.Command("ffmpeg", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		os.Remove(fixedPath)
		return false, fmt.Errorf("ffmpeg mov failed: %v: %s", err, stderr.String())
	}

	fiFixed, err := os.Stat(fixedPath)
	if err != nil || fiFixed.Size() == 0 {
		os.Remove(fixedPath)
		return false, fmt.Errorf("ffmpeg produced empty file")
	}

	os.Rename(badPath, badPath+".bak")
	os.Rename(fixedPath, badPath)

	log.Printf("Recovered %s using ffmpeg mov demuxer (size: %d)", badPath, fiFixed.Size())
	return true, nil
}

func RecoverWithFFmpeg(badPath, camera, go2rtcURL string) error {
	f, err := os.Open(badPath)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat: %w", err)
	}
	badSize := fi.Size()
	if badSize < 8 {
		return fmt.Errorf("file too small: %d bytes", badSize)
	}

	data, err := io.ReadAll(f)
	if err != nil {
		return fmt.Errorf("read: %w", err)
	}

	hasMoov := bytes.Contains(data[:minInt(1024, len(data))], []byte("moov"))

	if !hasMoov {
		if ok, _ := tryFFmpegMovRepair(badPath); ok {
			return nil
		}
	}

	mdatDataOffset, _, err := findMdatDataOffset(data, badSize)
	if err != nil {
		return fmt.Errorf("find mdat: %w", err)
	}

	mdatSize := badSize - mdatDataOffset
	if mdatSize <= 0 {
		return fmt.Errorf("invalid mdat size: %d", mdatSize)
	}

	mdat := data[mdatDataOffset:]
	if int64(len(mdat)) != mdatSize {
		return fmt.Errorf("mdat slice mismatch: %d vs %d", len(mdat), mdatSize)
	}

	hevcAnchors := findValidVPS(mdat)
	h264Anchors := findValidSPS(mdat)

	codecInfo, _ := fetchGo2RTCStreamInfo(go2rtcURL, camera)

	var isHEVC, isH264 bool
	if codecInfo != nil && codecInfo.CodecName != "" {
		isHEVC = codecInfo.CodecName == "hevc"
		isH264 = codecInfo.CodecName == "h264"
	} else {
		isHEVC = len(hevcAnchors) > 0
		isH264 = len(h264Anchors) > 0
	}

	if !isHEVC && !isH264 {
		hevcPresent, h264Present := findCodecAnchors(mdat)
		isHEVC = hevcPresent
		isH264 = h264Present
	}

	if !isHEVC && !isH264 {
		return fmt.Errorf("no valid VPS/SPS sequences found in mdat")
	}

	if isHEVC && isH264 {
		isH264 = false
	}

	if codecInfo != nil && (codecInfo.Width == 0 || codecInfo.Height == 0) && codecInfo.SPS != nil {
		codecInfo.Width, codecInfo.Height = parseSPSResolution(codecInfo.SPS)
	}

	var header []byte
	if isH264 {
		header = extractAllParamSets(mdat, false)
	}

	if isHEVC {
		header = extractAllParamSets(mdat, true)
	}

	var annexB []byte
	pos := 0

	if isHEVC || isH264 {
		var allAnchors []int
		if isHEVC {
			allAnchors = append(allAnchors, findValidVPS(mdat)...)
			allAnchors = append(allAnchors, findValidHEVCSPS(mdat)...)
			allAnchors = append(allAnchors, findValidHEVCPPS(mdat)...)
		}
		if isH264 {
			allAnchors = append(allAnchors, findValidSPS(mdat)...)
			allAnchors = append(allAnchors, findValidHEVCPPS(mdat)...)
		}
		if len(allAnchors) > 0 {
			pos = allAnchors[0]
			for _, a := range allAnchors {
				if a < pos {
					pos = a
				}
			}
		}
	}

	for pos < len(mdat)-4 {
		length := int(binary.BigEndian.Uint32(mdat[pos : pos+4]))
		if length > 2 && length < 50000000 && pos+4+length <= len(mdat) {
			if int64(len(annexB)) > 2*1024*1024*1024 {
				break
			}
			annexB = append(annexB, 0x00, 0x00, 0x00, 0x01)
			annexB = append(annexB, mdat[pos+4:pos+4+length]...)
			pos += 4 + length
		} else {
			pos++
		}
	}

	if len(annexB) == 0 {
		return fmt.Errorf("no NAL units found")
	}

	format := "h264"
	if isHEVC {
		format = "hevc"
	}

	streamPath := badPath + ".stream"
	if err := os.WriteFile(streamPath, append(header, annexB...), 0644); err != nil {
		return fmt.Errorf("write stream: %w", err)
	}
	defer os.Remove(streamPath)

	fixedPath := badPath + ".fixed"

	args := []string{"-v", "warning", "-y",
		"-analyzeduration", "100000000", "-probesize", "1000000",
		"-fflags", "+genpts+discardcorrupt",
		"-err_detect", "ignore_err",
		"-f", format, "-i", streamPath,
		"-c", "copy", "-movflags", "+faststart", "-f", "mp4", fixedPath}

	if codecInfo != nil && codecInfo.Width > 0 && codecInfo.Height > 0 {
		args = append(args, "-s", fmt.Sprintf("%dx%d", codecInfo.Width, codecInfo.Height))
	}

	cmd := exec.Command("ffmpeg", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		os.Remove(fixedPath)
		return fmt.Errorf("ffmpeg failed: %v: %s", err, stderr.String())
	}

	fiFixed, err := os.Stat(fixedPath)
	if err != nil || fiFixed.Size() == 0 {
		os.Remove(fixedPath)
		return fmt.Errorf("ffmpeg produced empty file")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	checkCmd := exec.CommandContext(ctx, "ffprobe", "-v", "error", "-show_entries", "format=duration",
		"-of", "default=noprint_wrappers=1:nokey=1", fixedPath)
	var checkStderr bytes.Buffer
	checkCmd.Stderr = &checkStderr
	checkErr := checkCmd.Run()
	cancel()

	if checkErr != nil {
		os.Remove(fixedPath)
		return fmt.Errorf("ffprobe verification failed: %s", checkStderr.String())
	}

	os.Rename(badPath, badPath+".bak")
	os.Rename(fixedPath, badPath)

	log.Printf("Recovered %s using ffmpeg (codec: %s, size: %d)", badPath, format, fiFixed.Size())
	return nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
