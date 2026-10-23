// Package recovery rebuilds MP4 recordings whose index (moov atom) is
// missing or corrupted: it extracts the raw H.264/HEVC elementary stream
// from the mdat atom and lets ffmpeg remux it into a valid MP4.
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

const (
	// mp4HeaderSize is size(4)+type(4) of an MP4 atom header.
	mp4HeaderSize = 8
	// mp4ExtendedHeaderSize is size(4)+type(4)+large size(8).
	mp4ExtendedHeaderSize = 16
	// maxAtomSize guards against absurd atom headers.
	maxAnnexBNALSize = 50_000_000
	// maxAnnexBSize bounds the reconstructed stream (2 GiB).
	maxAnnexBSize = 2 * 1024 * 1024 * 1024

	ffmpegTimeout = 10 * time.Minute
	probeTimeout  = 10 * time.Second
)

// tryFFmpegMovRepair lets ffmpeg rebuild the container with its MOV demuxer.
func tryFFmpegMovRepair(badPath string) (bool, error) {
	fixedPath := badPath + ".fixed"
	args := []string{"-v", "warning", "-y",
		"-fflags", "+genpts+discardcorrupt",
		"-err_detect", "ignore_err",
		"-f", "mov", "-i", badPath,
		"-c", "copy", "-movflags", "+faststart", "-f", "mp4", fixedPath}

	if stderr, err := runFFmpeg(args); err != nil {
		os.Remove(fixedPath)
		return false, fmt.Errorf("ffmpeg mov failed: %v: %s", err, stderr)
	}

	size, err := nonEmptyFileSize(fixedPath)
	if err != nil {
		os.Remove(fixedPath)
		return false, err
	}

	if err := replaceOriginal(badPath, fixedPath); err != nil {
		return false, err
	}

	log.Printf("Recovered %s using ffmpeg mov demuxer (size: %d)", badPath, size)
	return true, nil
}

// RecoverWithFFmpeg attempts to repair a damaged MP4 file in place. The
// original file is kept as <path>.bak. camera/go2rtcURL are optional and are
// only used to ask go2rtc about the stream codec.
func RecoverWithFFmpeg(badPath, camera, go2rtcURL string) error {
	data, err := readFile(badPath)
	if err != nil {
		return err
	}

	// Faststart files keep moov up front; a missing one means the recorder
	// died before writing the index.
	if !hasMoovAtom(data) {
		if ok, _ := tryFFmpegMovRepair(badPath); ok {
			return nil
		}
		// The MOV attempt rewrites badPath only on success, but reload in
		// case ffmpeg touched the file.
		if data, err = readFile(badPath); err != nil {
			return err
		}
	}

	mdatOffset, err := findMdatDataOffset(data)
	if err != nil {
		return err
	}

	mdat := data[mdatOffset:]
	if len(mdat) == 0 {
		return fmt.Errorf("invalid mdat size: 0")
	}

	codecInfo, _ := fetchGo2RTCStreamInfo(go2rtcURL, camera)
	isHEVC, isH264 := detectCodec(mdat, codecInfo)
	if !isHEVC && !isH264 {
		return fmt.Errorf("no valid VPS/SPS sequences found in mdat")
	}

	if codecInfo != nil && (codecInfo.Width == 0 || codecInfo.Height == 0) && codecInfo.SPS != nil {
		codecInfo.Width, codecInfo.Height = parseSPSResolution(codecInfo.SPS)
	}

	header := extractAllParamSets(mdat, isHEVC)
	annexB := lengthPrefixedToAnnexB(mdat, firstAnchor(mdat, isHEVC, isH264))
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

	if stderr, err := runFFmpeg(args); err != nil {
		os.Remove(fixedPath)
		return fmt.Errorf("ffmpeg failed: %v: %s", err, stderr)
	}

	size, err := nonEmptyFileSize(fixedPath)
	if err != nil {
		os.Remove(fixedPath)
		return err
	}

	if err := verifyWithFFProbe(fixedPath); err != nil {
		os.Remove(fixedPath)
		return err
	}

	if err := replaceOriginal(badPath, fixedPath); err != nil {
		return err
	}

	log.Printf("Recovered %s using ffmpeg (codec: %s, size: %d)", badPath, format, size)
	return nil
}

// readFile loads a file and rejects suspiciously small inputs.
func readFile(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat: %w", err)
	}
	if fi.Size() < mp4HeaderSize {
		return nil, fmt.Errorf("file too small: %d bytes", fi.Size())
	}

	data, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("read: %w", err)
	}
	return data, nil
}

// hasMoovAtom looks for the moov atom type near the start of the file
// (faststart keeps the index in the first kilobytes).
func hasMoovAtom(data []byte) bool {
	limit := min(1024, len(data))
	return bytes.Contains(data[:limit], []byte("moov"))
}

// detectCodec decides the stream codec from go2rtc metadata when available,
// falling back to scanning the mdat for parameter sets. HEVC candidates are
// matched on the full two-byte NAL header (see hevcParamSetHeader), so plain
// H.264 slices no longer masquerade as VPS. If both codecs appear to be
// present, HEVC wins — the historical behavior.
func detectCodec(mdat []byte, codecInfo *go2rtcStreamInfo) (isHEVC, isH264 bool) {
	if codecInfo != nil && codecInfo.CodecName != "" {
		return codecInfo.CodecName == "hevc", codecInfo.CodecName == "h264"
	}

	isHEVC = len(findValidVPS(mdat)) > 0 ||
		len(findValidHEVCSPS(mdat)) > 0 ||
		len(findValidHEVCPPS(mdat)) > 0
	isH264 = len(findValidSPS(mdat)) > 0 || len(findValidH264PPS(mdat)) > 0

	if isHEVC && isH264 {
		isH264 = false
	}
	return isHEVC, isH264
}

// firstAnchor returns the offset of the earliest parameter set so the
// elementary stream starts on a decodable boundary.
func firstAnchor(mdat []byte, isHEVC, isH264 bool) int {
	var anchors []int
	if isHEVC {
		anchors = append(anchors, findValidVPS(mdat)...)
		anchors = append(anchors, findValidHEVCSPS(mdat)...)
		anchors = append(anchors, findValidHEVCPPS(mdat)...)
	}
	if isH264 {
		anchors = append(anchors, findValidSPS(mdat)...)
		anchors = append(anchors, findValidH264PPS(mdat)...)
	}

	first := 0
	for i, offset := range anchors {
		if i == 0 || offset < first {
			first = offset
		}
	}
	return first
}

// lengthPrefixedToAnnexB converts AVCC style NALs (4 byte big endian length)
// starting at start into Annex B format (00 00 00 01 start codes).
func lengthPrefixedToAnnexB(mdat []byte, start int) []byte {
	if start < 0 {
		start = 0
	}
	var annexB []byte

	for pos := start; pos+4 <= len(mdat); {
		if int64(len(annexB)) > maxAnnexBSize {
			break
		}
		length := int(binary.BigEndian.Uint32(mdat[pos : pos+4]))
		if length > 0 && length < maxAnnexBNALSize && pos+4+length <= len(mdat) {
			annexB = append(annexB, 0x00, 0x00, 0x00, 0x01)
			annexB = append(annexB, mdat[pos+4:pos+4+length]...)
			pos += 4 + length
			continue
		}
		pos++
	}
	return annexB
}

// runFFmpeg executes ffmpeg with the given arguments and returns its stderr.
func runFFmpeg(args []string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ffmpegTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	if ctx.Err() != nil {
		return stderr.String(), fmt.Errorf("ffmpeg timed out")
	}
	return stderr.String(), err
}

// verifyWithFFProbe checks that ffmpeg produced a readable file.
func verifyWithFFProbe(path string) error {
	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "ffprobe", "-v", "error", "-show_entries", "format=duration",
		"-of", "default=noprint_wrappers=1:nokey=1", path)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("ffprobe verification failed: %s", stderr.String())
	}
	return nil
}

// nonEmptyFileSize returns the size of an existing, non-empty file.
func nonEmptyFileSize(path string) (int64, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, fmt.Errorf("output missing: %w", err)
	}
	if fi.Size() == 0 {
		return 0, fmt.Errorf("ffmpeg produced empty file")
	}
	return fi.Size(), nil
}

// replaceOriginal moves the damaged file to <path>.bak and puts the repaired
// file in its place.
func replaceOriginal(badPath, fixedPath string) error {
	if err := os.Rename(badPath, badPath+".bak"); err != nil {
		return fmt.Errorf("backup original: %w", err)
	}
	if err := os.Rename(fixedPath, badPath); err != nil {
		// try to restore the original
		os.Rename(badPath+".bak", badPath)
		return fmt.Errorf("move repaired file: %w", err)
	}
	return nil
}
