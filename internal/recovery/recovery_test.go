package recovery

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// --- bitReader --------------------------------------------------------------

func TestBitReaderBasics(t *testing.T) {
	r := &bitReader{data: []byte{0b10110100, 0b01000001}}
	if got := r.readBits(4); got != 0b1011 {
		t.Errorf("readBits(4) = %b", got)
	}
	if got := r.readBit(); got != 0 {
		t.Errorf("readBit = %d", got)
	}
	if got := r.readBits(3); got != 0b100 {
		t.Errorf("readBits(3) = %b", got)
	}
	// Второй байт: 01000001
	if got := r.readBits(8); got != 0b01000001 {
		t.Errorf("second byte = %b", got)
	}
	// Чтение за концом не паникует, а фиксирует ошибку.
	r.readBit()
	if r.err == nil {
		t.Error("expected error after reading past the end")
	}
	errAtEnd := r.err
	r.readBit()
	if r.err != errAtEnd {
		t.Error("first error must be latched")
	}
}

func TestBitReaderGolomb(t *testing.T) {
	// exp-Golomb: 1 -> 0, 010 -> 1, 011 -> 2, 00100 -> 3
	data := []byte{0b10100110, 0b01000000}
	r := &bitReader{data: data}
	want := []int{0, 1, 2, 3}
	for i, w := range want {
		if got := r.readUE(); got != w {
			t.Errorf("readUE #%d = %d, want %d", i, got, w)
		}
	}
	// readSE: ue 0 -> 0, ue 1 -> 1, ue 2 -> -1, ue 3 -> 2
	r = &bitReader{data: []byte{0b10100110, 0b01000000}}
	seWant := []int{0, 1, -1, 2}
	for i, w := range seWant {
		if got := r.readSE(); got != w {
			t.Errorf("readSE #%d = %d, want %d", i, got, w)
		}
	}
}

func TestBitReaderTooLongGolomb(t *testing.T) {
	r := &bitReader{data: make([]byte, 8)} // все нули — 64 нулевых бита
	if got := r.readUE(); got != 0 {
		t.Errorf("readUE = %d, want 0", got)
	}
	if r.err == nil || !strings.Contains(r.err.Error(), "too long") {
		t.Errorf("err = %v", r.err)
	}
}

// --- SPS --------------------------------------------------------------------

// SPS из реального ffmpeg-файла 310x250 (x264, High profile, 4:2:0,
// кроп 0,5,0,3 при cropUnit 2x2).
const realSPSHex = "6764000cacd94140879a4c0440000003004000000503c50a6580"

func TestParseSPSResolutionReal(t *testing.T) {
	w, h := parseSPSResolution(mustHexSPS(t))
	if w != 310 || h != 250 {
		t.Errorf("parseSPSResolution = %dx%d, want 310x250", w, h)
	}
}

func mustHexSPS(t *testing.T) []byte {
	t.Helper()
	sps, err := hex.DecodeString(realSPSHex)
	if err != nil {
		t.Fatal(err)
	}
	return sps
}

func TestParseSPSResolutionInvalid(t *testing.T) {
	if w, h := parseSPSResolution(nil); w != 0 || h != 0 {
		t.Errorf("nil SPS = %dx%d", w, h)
	}
	if w, h := parseSPSResolution([]byte{0x67, 0x64}); w != 0 || h != 0 {
		t.Errorf("short SPS = %dx%d", w, h)
	}
	// Ноль бит за концом — без паники, 0x0.
	if w, h := parseSPSResolution([]byte{0x67, 0x64, 0x00, 0x00}); w != 0 || h != 0 {
		t.Errorf("truncated SPS = %dx%d", w, h)
	}
	// Бит запрета в заголовке NAL.
	if w, h := parseSPSResolution([]byte{0xe7, 0x64, 0x00, 0x00}); w != 0 || h != 0 {
		t.Errorf("forbidden-bit SPS = %dx%d", w, h)
	}
}

func TestStripEmulationPrevention(t *testing.T) {
	// 0x03 после двух нулей — байт подавления эмуляции; одиночный 0x03 в
	// конце остаётся.
	in := []byte{0x67, 0x00, 0x00, 0x03, 0x01, 0x00, 0x00, 0x03, 0x00, 0x00, 0x03, 0x03, 0xff}
	got := stripEmulationPrevention(in)
	want := []byte{0x67, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x03, 0xff}
	if !bytes.Equal(got, want) {
		t.Errorf("stripEmulationPrevention = %x, want %x", got, want)
	}
}

func TestCropUnits(t *testing.T) {
	// cropUnitX = SubWidthC, cropUnitY = SubHeightC * (2 - frame_mbs_only).
	tests := []struct {
		chroma, frameOnly, x, y int
	}{
		{1, 1, 2, 2}, // 4:2:0, frame  -> SubHeightC 2 * 1
		{1, 0, 2, 4}, // 4:2:0, field  -> 2 * 2
		{2, 1, 2, 1}, // 4:2:2, frame  -> SubHeightC 1 * 1
		{2, 0, 2, 2}, // 4:2:2, field  -> 1 * 2
		{0, 1, 1, 1}, // monochrome
		{0, 0, 1, 2},
		{3, 1, 1, 1}, // 4:4:4
		{3, 0, 1, 2},
	}
	for _, tt := range tests {
		x, y := cropUnits(tt.chroma, tt.frameOnly)
		if x != tt.x || y != tt.y {
			t.Errorf("cropUnits(%d,%d) = (%d,%d), want (%d,%d)", tt.chroma, tt.frameOnly, x, y, tt.x, tt.y)
		}
	}
}

// --- MP4 --------------------------------------------------------------------

func mp4Atom(typ string, payload []byte) []byte {
	out := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(out, uint32(8+len(payload)))
	copy(out[4:8], typ)
	copy(out[8:], payload)
	return out
}

func TestFindMdatDataOffset(t *testing.T) {
	file := append(mp4Atom("ftyp", []byte("isom0000")), mp4Atom("mdat", []byte("hello"))...)
	off, err := findMdatDataOffset(file)
	if err != nil {
		t.Fatalf("findMdatDataOffset: %v", err)
	}
	if got := string(file[off:]); got != "hello" {
		t.Errorf("payload = %q", got)
	}

	// Размер 0 — mdat до конца файла.
	zeroSize := []byte{0, 0, 0, 0, 'm', 'd', 'a', 't', 'x'}
	off, err = findMdatDataOffset(zeroSize)
	if err != nil || off != 8 {
		t.Errorf("zero-size mdat: off=%d err=%v", off, err)
	}

	// Расширенный (64-битный) размер.
	ext := make([]byte, 16+4)
	binary.BigEndian.PutUint32(ext, 1)
	copy(ext[4:8], "mdat")
	binary.BigEndian.PutUint64(ext[8:], 20)
	copy(ext[16:], []byte("abcd"))
	off, err = findMdatDataOffset(ext)
	if err != nil || off != 16 || string(ext[16:20]) != "abcd" {
		t.Errorf("extended mdat: off=%d err=%v", off, err)
	}

	// mdat за пределами файла — не принимаем.
	broken := []byte{0, 0, 0, 0xff, 'm', 'd', 'a', 't', 'x'}
	if _, err := findMdatDataOffset(broken); err == nil {
		t.Error("oversized mdat should be rejected")
	}

	// Нет mdat вовсе.
	if _, err := findMdatDataOffset(mp4Atom("ftyp", []byte("isom"))); err == nil {
		t.Error("missing mdat should error")
	}

	// Битый заголовок перед mdat: переходим к сканированию байт за байтом.
	junk := append([]byte{0xff, 0xff, 0xff, 0xff, 'j', 'u', 'n', 'k'}, mp4Atom("mdat", []byte("ok"))...)
	off, err = findMdatDataOffset(junk)
	if err != nil {
		t.Fatalf("scan-after-junk: %v", err)
	}
	if string(junk[off:]) != "ok" {
		t.Errorf("payload = %q", junk[off:])
	}
}

func TestHasMoovAtom(t *testing.T) {
	if hasMoovAtom(mp4Atom("moov", nil)) != true {
		t.Error("moov in first bytes not found")
	}
	if hasMoovAtom(bytes.Repeat([]byte("x"), 2000)) {
		t.Error("no moov expected")
	}
	// moov глубже 1024 байт не считается (faststart держит его в начале).
	big := append(mp4Atom("ftyp", bytes.Repeat([]byte("y"), 1100)), mp4Atom("moov", nil)...)
	if hasMoovAtom(big) {
		t.Error("moov beyond the faststart window not expected")
	}
}

func TestReadFileRejectsTinyFiles(t *testing.T) {
	dir := t.TempDir()

	small := filepath.Join(dir, "small.mp4")
	if err := os.WriteFile(small, []byte("abc"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readFile(small); err == nil || !strings.Contains(err.Error(), "too small") {
		t.Errorf("err = %v", err)
	}

	if _, err := readFile(filepath.Join(dir, "missing.mp4")); err == nil {
		t.Error("missing file should error")
	}
}

// --- NAL сканирование --------------------------------------------------------

// lengthPrefixed собирает AVCC-поток из NAL-юнитов.
func lengthPrefixed(nals ...[]byte) []byte {
	var out []byte
	for _, n := range nals {
		var l [4]byte
		binary.BigEndian.PutUint32(l[:], uint32(len(n)))
		out = append(out, l[:]...)
		out = append(out, n...)
	}
	return out
}

func TestFindCodecAnchorsAndParamSets(t *testing.T) {
	h264SPS := append([]byte{0x67}, bytes.Repeat([]byte{0x42}, 30)...)
	h264PPS := append([]byte{0x68}, bytes.Repeat([]byte{0x68}, 20)...)
	h264Slice := append([]byte{0x41}, bytes.Repeat([]byte{0xaa}, 50)...)

	mdat := lengthPrefixed(h264SPS, h264PPS, h264Slice)

	// Раньше для H.264 использовался HEVC-поиск PPS — тест фиксирует, что
	// H.264 PPS находится именно findValidH264PPS, а HEVC-кандидатов нет,
	// несмотря на 0x41-заголовок среза (он читается как HEVC VPS).
	if offs := findValidH264PPS(mdat); len(offs) != 1 || offs[0] != 4+len(h264SPS) {
		t.Errorf("findValidH264PPS = %v", offs)
	}
	if offs := findValidSPS(mdat); len(offs) != 1 || offs[0] != 0 {
		t.Errorf("findValidSPS = %v", offs)
	}
	if offs := findValidVPS(mdat); len(offs) != 0 {
		t.Errorf("findValidVPS = %v, want none (H.264 slice 0x41 must not match)", offs)
	}
	if offs := findValidHEVCSPS(mdat); len(offs) != 0 {
		t.Errorf("findValidHEVCSPS = %v", offs)
	}

	// Annex-B заголовки параметров + путь до первого якоря.
	header := extractAllParamSets(mdat, false)
	if !bytes.HasPrefix(header, []byte{0, 0, 0, 1, 0x67}) {
		t.Errorf("param set header = %x", header[:5])
	}

	anchor := firstAnchor(mdat, false, true)
	if anchor != 0 {
		t.Errorf("firstAnchor = %d", anchor)
	}
	converted := lengthPrefixedToAnnexB(mdat, anchor)
	if !bytes.Contains(converted, []byte{0, 0, 0, 1, 0x41}) {
		t.Errorf("annex-B stream misses slice: %x", converted[:16])
	}

	// Отрицательный offset и мусор не паникуют.
	if out := lengthPrefixedToAnnexB(mdat, -5); len(out) == 0 {
		t.Error("negative start should behave as 0")
	}
	if out := lengthPrefixedToAnnexB([]byte{1, 2, 3}, 0); len(out) != 0 {
		t.Errorf("garbage produced output: %x", out)
	}
}

func TestDetectCodec(t *testing.T) {
	h264 := lengthPrefixed(append([]byte{0x67}, bytes.Repeat([]byte{0x42}, 30)...))
	isHEVC, isH264 := detectCodec(h264, nil)
	if isHEVC || !isH264 {
		t.Errorf("scan: hevc=%v h264=%v", isHEVC, isH264)
	}

	// go2rtc сообщает кодек — приоритет над сканированием.
	isHEVC, isH264 = detectCodec(nil, &go2rtcStreamInfo{CodecName: "hevc"})
	if !isHEVC || isH264 {
		t.Errorf("go2rtc hevc: hevc=%v h264=%v", isHEVC, isH264)
	}
	isHEVC, isH264 = detectCodec(nil, &go2rtcStreamInfo{CodecName: "h264"})
	if isHEVC || !isH264 {
		t.Errorf("go2rtc h264: hevc=%v h264=%v", isHEVC, isH264)
	}
	// Неизвестный кодек от go2rtc не должен ничего угадывать.
	isHEVC, isH264 = detectCodec(h264, &go2rtcStreamInfo{CodecName: "pcmu"})
	if isHEVC || isH264 {
		t.Errorf("unknown codec: hevc=%v h264=%v", isHEVC, isH264)
	}

	// HEVC побеждает при наличии обоих.
	vps := append([]byte{0x40}, bytes.Repeat([]byte{0x01}, 40)...)
	both := append(lengthPrefixed(vps), h264...)
	isHEVC, isH264 = detectCodec(both, nil)
	if !isHEVC || isH264 {
		t.Errorf("both: hevc=%v h264=%v", isHEVC, isH264)
	}
}

// --- SDP / go2rtc -----------------------------------------------------------

func TestParseSDP(t *testing.T) {
	sps := base64.StdEncoding.EncodeToString(mustHexSPS(t))
	pps := "aM48gA=="
	sdp := strings.Join([]string{
		"v=0",
		"a=rtpmap:96 H264/90000",
		"a=fmtp:96 packetization-mode=1;sprop-parameter-sets=" + sps + "," + pps + ";profile-level-id=42C01E",
		"",
	}, "\r\n")

	info := &go2rtcStreamInfo{}
	parseSDP(sdp, info)
	if len(info.SPS) == 0 || len(info.PPS) == 0 {
		t.Fatalf("SPS/PPS not extracted: sps=%d pps=%d", len(info.SPS), len(info.PPS))
	}

	// Отдельные a=sprop-* строки.
	info = &go2rtcStreamInfo{}
	parseSDP("a=sprop-sps="+sps+"\r\na=sprop-pps="+pps+"\r\na=sprop-vps=AAAA", info)
	if len(info.SPS) == 0 || len(info.PPS) == 0 || len(info.VPS) == 0 {
		t.Errorf("sprop lines: sps=%d pps=%d vps=%d", len(info.SPS), len(info.PPS), len(info.VPS))
	}

	// Битый base64 не паникует.
	info = &go2rtcStreamInfo{}
	parseSDP("a=sprop-parameter-sets=!!!,???", info)
	if info.SPS != nil || info.PPS != nil {
		t.Errorf("bad base64 accepted: sps=%v pps=%v", info.SPS, info.PPS)
	}

	// sprop-parameter-sets без запятой игнорируется.
	info = &go2rtcStreamInfo{}
	applySpropParameterSets("onlyone", info)
	if info.SPS != nil || info.PPS != nil {
		t.Error("single element accepted")
	}
}

func TestFmtpValue(t *testing.T) {
	line := "a=fmtp:96 profile-level-id=42C01E;sprop-parameter-sets=AA,BB;x=1"
	if v, ok := fmtpValue(line, "sprop-parameter-sets="); !ok || v != "AA,BB" {
		t.Errorf("fmtpValue = (%q, %v)", v, ok)
	}
	if v, ok := fmtpValue(line, "absent="); ok || v != "" {
		t.Errorf("missing key: (%q, %v)", v, ok)
	}
}

func TestFetchGo2RTCStreamInfo(t *testing.T) {
	// Пустой URL — сразу ошибка (вызывается без сети).
	if _, err := fetchGo2RTCStreamInfo("", "cam"); err == nil {
		t.Error("empty URL should fail")
	}

	const sdp = "v=0\r\na=sprop-parameter-sets=Z2QADKzZQUCHmkwEQAAAAwBAAAAFA8UKZYA=,aM48gA==\r\n"
	// Неправильный JSON.
	srv := httptestJSON(t, `{"broken":`)
	if _, err := fetchGo2RTCStreamInfo(srv, "cam"); err == nil {
		t.Error("bad json should fail")
	}

	// Камера отсутствует.
	srv = httptestJSON(t, `{"other": {"producers": []}}`)
	if _, err := fetchGo2RTCStreamInfo(srv, "cam"); err == nil {
		t.Error("missing camera should fail")
	}

	// Полный ответ go2rtc.
	srv = httptestJSON(t, `{"cam": {"producers": [{"receivers": [{"codec": {"codec_name": "h264", "profile": "High", "level": 31}}]}], "consumers": [{"sdp": `+jsonStr(sdp)+`}]}}`)
	info, err := fetchGo2RTCStreamInfo(srv, "cam")
	if err != nil {
		t.Fatalf("fetchGo2RTCStreamInfo: %v", err)
	}
	if info.CodecName != "h264" || info.Profile != "High" || info.Level != 31 {
		t.Errorf("codec = %+v", info)
	}
	if len(info.SPS) == 0 || len(info.PPS) == 0 {
		t.Errorf("parameter sets missing: %+v", info)
	}
	if info.Width != 310 || info.Height != 250 {
		t.Errorf("resolution = %dx%d, want 310x250 (from SPS)", info.Width, info.Height)
	}

	// Нет producers.
	srv = httptestJSON(t, `{"cam": {"consumers": []}}`)
	if _, err := fetchGo2RTCStreamInfo(srv, "cam"); err == nil {
		t.Error("missing producers should fail")
	}
}

// httptestJSON поднимает тестовый сервер, отдающий тело body.
func httptestJSON(t *testing.T, body string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// --- E2E: восстановление файла без moov -------------------------------------

// brokenMP4 удаляет moov-атом (faststart держит его в первых килобайтах),
// оставляя ftyp+mdat — «умерший» запись.
func brokenMP4(t *testing.T, src []byte) []byte {
	t.Helper()
	// Найдём moov и вырежем его.
	pos := bytes.Index(src[:min(4096, len(src))], []byte("moov"))
	if pos < 4 {
		t.Fatalf("moov atom not found at %d", pos)
	}
	moovStart := pos - 4
	size := int(binary.BigEndian.Uint32(src[moovStart : moovStart+4]))
	if size < 8 || moovStart+size > len(src) {
		t.Fatalf("bad moov size %d", size)
	}
	out := make([]byte, 0, len(src)-size)
	out = append(out, src[:moovStart]...)
	out = append(out, src[moovStart+size:]...)
	return out
}

func requireTools(t *testing.T) {
	t.Helper()
	for _, tool := range []string{"ffmpeg", "ffprobe"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not available: %v", tool, err)
		}
	}
}

func generateMP4(t *testing.T, path string) {
	t.Helper()
	// Прямое кодирование в mp4 кладёт SPS/PPS только в avcC, а восстановление
	// сканирует mdat. Remux из annex-B .h264 сохраняет параметры in-band.
	raw := filepath.Join(t.TempDir(), "src.h264")
	cmd := exec.Command("ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
		"-f", "lavfi", "-i", "testsrc=size=320x240:rate=10",
		"-t", "1", "-c:v", "libx264", "-pix_fmt", "yuv420p", "-f", "h264", raw)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("ffmpeg generate: %v: %s", err, out)
	}
	cmd = exec.Command("ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
		"-r", "10", "-i", raw, "-c", "copy", "-movflags", "+faststart", path)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("ffmpeg remux: %v: %s", err, out)
	}
}

func TestRecoverWithFFmpegEndToEnd(t *testing.T) {
	requireTools(t)

	good := filepath.Join(t.TempDir(), "good.mp4")
	generateMP4(t, good)
	src, err := os.ReadFile(good)
	if err != nil {
		t.Fatal(err)
	}

	badPath := filepath.Join(t.TempDir(), "rec.mp4")
	if err := os.WriteFile(badPath, brokenMP4(t, src), 0o644); err != nil {
		t.Fatal(err)
	}

	// Файл без moov не должен считаться годным.
	if hasMoovAtom(mustRead(t, badPath)) {
		t.Fatal("test setup: moov still present")
	}

	if err := RecoverWithFFmpeg(badPath, "", ""); err != nil {
		t.Fatalf("RecoverWithFFmpeg: %v", err)
	}

	// Восстановленный файл имеет moov, проходит ffprobe, оригинал в .bak.
	recovered := mustRead(t, badPath)
	if !hasMoovAtom(recovered) {
		t.Error("recovered file has no moov")
	}
	if _, err := os.Stat(badPath + ".bak"); err != nil {
		t.Errorf("backup missing: %v", err)
	}
	if err := verifyWithFFProbe(badPath); err != nil {
		t.Errorf("ffprobe verification failed: %v", err)
	}
}

func TestRecoverWithFFmpegRejectsBadInput(t *testing.T) {
	requireTools(t)
	dir := t.TempDir()

	// Слишком маленький файл.
	tiny := filepath.Join(dir, "tiny.mp4")
	if err := os.WriteFile(tiny, []byte("123"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := RecoverWithFFmpeg(tiny, "", ""); err == nil {
		t.Error("tiny file should fail")
	}

	// Нет mdat.
	noMdat := filepath.Join(dir, "nomdat.mp4")
	if err := os.WriteFile(noMdat, mp4Atom("ftyp", []byte("isom")), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := RecoverWithFFmpeg(noMdat, "", ""); err == nil || !strings.Contains(err.Error(), "mdat") {
		t.Errorf("err = %v", err)
	}

	// mdat без кодека.
	garbage := filepath.Join(dir, "garbage.mp4")
	if err := os.WriteFile(garbage, mp4Atom("mdat", bytes.Repeat([]byte{0x55}, 500)), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := RecoverWithFFmpeg(garbage, "", ""); err == nil {
		t.Error("garbage mdat should fail")
	}

	// Отсутствующий файл.
	if err := RecoverWithFFmpeg(filepath.Join(dir, "missing.mp4"), "", ""); err == nil {
		t.Error("missing file should fail")
	}
}

func TestNonEmptyFileSize(t *testing.T) {
	dir := t.TempDir()
	if _, err := nonEmptyFileSize(filepath.Join(dir, "nope")); err == nil {
		t.Error("missing file should error")
	}
	empty := filepath.Join(dir, "empty")
	if err := os.WriteFile(empty, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := nonEmptyFileSize(empty); err == nil {
		t.Error("empty file should error")
	}
	full := filepath.Join(dir, "full")
	if err := os.WriteFile(full, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	if size, err := nonEmptyFileSize(full); err != nil || size != 4 {
		t.Errorf("size = %d, err = %v", size, err)
	}
}

func TestReplaceOriginal(t *testing.T) {
	dir := t.TempDir()
	bad := filepath.Join(dir, "rec.mp4")
	fixed := filepath.Join(dir, "rec.mp4.fixed")
	if err := os.WriteFile(bad, []byte("bad"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fixed, []byte("good"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := replaceOriginal(bad, fixed); err != nil {
		t.Fatal(err)
	}
	if got := string(mustRead(t, bad)); got != "good" {
		t.Errorf("replaced content = %q", got)
	}
	if got := string(mustRead(t, bad+".bak")); got != "bad" {
		t.Errorf("backup content = %q", got)
	}

	// Нет fixed-файла: оригинал должен остаться на месте.
	bad2 := filepath.Join(dir, "b2.mp4")
	if err := os.WriteFile(bad2, []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := replaceOriginal(bad2, filepath.Join(dir, "absent")); err == nil {
		t.Error("missing fixed file should error")
	}
	if got := string(mustRead(t, bad2)); got != "keep" {
		t.Errorf("original content = %q, want restore", got)
	}
}

func TestRunFFmpegErrorAndTimeout(t *testing.T) {
	requireTools(t)
	// Несуществующая входная команда/файл — stderr и ошибка.
	stderr, err := runFFmpeg([]string{"-v", "error", "-i", filepath.Join(t.TempDir(), "none.h264"), "-f", "null", "-"})
	if err == nil {
		t.Error("expected ffmpeg error")
	}
	if stderr == "" {
		t.Error("expected stderr output")
	}
}

func TestVerifyWithFFProbe(t *testing.T) {
	requireTools(t)
	if err := verifyWithFFProbe(filepath.Join(t.TempDir(), "missing.mp4")); err == nil {
		t.Error("missing file should fail verification")
	}
	good := filepath.Join(t.TempDir(), "good.mp4")
	generateMP4(t, good)
	if err := verifyWithFFProbe(good); err != nil {
		t.Errorf("good file failed verification: %v", err)
	}
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func jsonStr(s string) string {
	b := &strings.Builder{}
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\r':
			b.WriteString(`\r`)
		case '\n':
			b.WriteString(`\n`)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}
