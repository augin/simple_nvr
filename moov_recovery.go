package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

const (
	mdatDataOffset = 44

	nalTypeVPS    = 32
	nalTypeSPS    = 33
	nalTypePPS    = 34
	nalTypeIDR    = 19
	nalTypeIDR2   = 20
	nalTypeSEI    = 39
	nalTypeSEI2   = 40
	nalTypeTrailN = 0
	nalTypeTrailR = 1
)

var validNALTypes = map[byte]bool{
	nalTypeVPS: true, nalTypeSPS: true, nalTypePPS: true,
	nalTypeIDR: true, nalTypeIDR2: true, nalTypeSEI: true, nalTypeSEI2: true,
	nalTypeTrailN: true, nalTypeTrailR: true,
}

type nalEntry struct {
	offset  int64
	size    int
	nalType byte
}

func RecoverMoov(badPath string) error {
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

	if badSize <= mdatDataOffset+8 {
		return fmt.Errorf("file too small")
	}

	mdatSize := badSize - mdatDataOffset
	mdat := make([]byte, mdatSize)
	if _, err := f.ReadAt(mdat, mdatDataOffset); err != nil {
		return fmt.Errorf("read mdat: %w", err)
	}

	refPath := "/tmp/ref_stsd.bin"
	vstsd, astd, err := loadRefSTSD(refPath)
	if err != nil {
		return fmt.Errorf("load ref stsd: %w", err)
	}

	validVPS := findValidVPS(mdat)
	if len(validVPS) < 2 {
		return fmt.Errorf("found only %d valid VPS positions", len(validVPS))
	}

	var videoNALs []nalEntry
	var audioChunks [][2]int64

	pos := int64(validVPS[0])

	for vi := 0; vi < len(validVPS)-1; vi++ {
		nextVPS := int64(validVPS[vi+1])
		for pos < nextVPS {
			if pos+5 > int64(len(mdat)) {
				break
			}
			length := int64(binary.BigEndian.Uint32(mdat[pos : pos+4]))
			b := mdat[pos+4]
			forbidden := (b >> 7) & 1
			naType := (b >> 1) & 0x3f

			if length > 2 && length < 1000000 && forbidden == 0 &&
				pos+4+length <= int64(len(mdat)) && validNALTypes[naType] &&
				pos+4+length <= nextVPS {
				videoNALs = append(videoNALs, nalEntry{pos, 4 + int(length), naType})
				pos += int64(4 + int(length))
			} else {
				nextNAL := int64(-1)
				searchEnd := pos + 2000
				if searchEnd > nextVPS {
					searchEnd = nextVPS
				}
				for test := pos + 1; test < searchEnd; test++ {
					if test+5 > int64(len(mdat)) {
						break
					}
					tl := int64(binary.BigEndian.Uint32(mdat[test : test+4]))
					if tl > 2 && tl < 1000000 && test+4+tl <= int64(len(mdat)) {
						tb := mdat[test+4]
						tf := (tb >> 7) & 1
						tt := (tb >> 1) & 0x3f
						if tf == 0 && validNALTypes[tt] {
							nextNAL = test
							break
						}
					}
				}
				if nextNAL > 0 {
					if nextNAL > pos {
						audioChunks = append(audioChunks, [2]int64{pos, nextNAL - pos})
					}
					pos = nextNAL
				} else {
					audioChunks = append(audioChunks, [2]int64{pos, nextVPS - pos})
					pos = nextVPS
					break
				}
			}
		}
	}

	lastVPS := int64(validVPS[len(validVPS)-1])
	if pos < lastVPS {
		pos = lastVPS
	}
	for pos < int64(len(mdat)) {
		if pos+5 > int64(len(mdat)) {
			break
		}
		length := int64(binary.BigEndian.Uint32(mdat[pos : pos+4]))
		b := mdat[pos+4]
		forbidden := (b >> 7) & 1
		naType := (b >> 1) & 0x3f

		if length > 2 && length < 1000000 && forbidden == 0 &&
			pos+4+length <= int64(len(mdat)) && validNALTypes[naType] {
			videoNALs = append(videoNALs, nalEntry{pos, 4 + int(length), naType})
			pos += int64(4 + int(length))
		} else {
			remaining := int64(len(mdat)) - pos
			if remaining > 4 {
				audioChunks = append(audioChunks, [2]int64{pos, remaining})
			}
			break
		}
	}

	if len(videoNALs) == 0 {
		return fmt.Errorf("no video NALs found")
	}

	nVideo := len(videoNALs)
	nAudio := len(audioChunks)

	vStco := make([]uint32, nVideo)
	vStsz := make([]uint32, nVideo)
	for i, v := range videoNALs {
		vStco[i] = uint32(mdatDataOffset + v.offset)
		vStsz[i] = uint32(v.size)
	}

	aStco := make([]uint32, nAudio)
	aStsz := make([]uint32, nAudio)
	for i, a := range audioChunks {
		aStco[i] = uint32(mdatDataOffset + a[0])
		aStsz[i] = uint32(a[1])
	}

	var keyframes []uint32
	for i, v := range videoNALs {
		if v.nalType == nalTypeVPS {
			keyframes = append(keyframes, uint32(i+1))
		}
	}

	vDur := uint32(nVideo) * 4500
	aDur := uint32(float64(vDur) * 8000 / 90000)

	moov := buildMoov(vstsd, astd, vStco, vStsz, keyframes, aStco, aStsz, vDur, aDur)

	tmpPath := badPath + ".tmp"
	out, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("create tmp: %w", err)
	}

	ftyp := make([]byte, 28)
	f.ReadAt(ftyp, 0)
	out.Write(ftyp)
	writeAtom(out, "free", nil)

	mdatHdr := make([]byte, 8)
	binary.BigEndian.PutUint32(mdatHdr[:4], uint32(8+mdatSize))
	copy(mdatHdr[4:], "mdat")
	out.Write(mdatHdr)

	f.Seek(mdatDataOffset, io.SeekStart)
	buf := make([]byte, 1024*1024)
	for {
		n, err := f.Read(buf)
		if n > 0 {
			out.Write(buf[:n])
		}
		if err != nil {
			break
		}
	}

	writeAtom(out, "free", nil)
	out.Write(moov)
	out.Close()

	os.Rename(badPath, badPath+".bak")
	os.Rename(tmpPath, badPath)

	fmt.Printf("Recovered %s: %d video NALs, %d audio chunks\n", badPath, nVideo, nAudio)
	return nil
}

func findValidVPS(mdat []byte) []int {
	var candidates []int
	mdatLen := int64(len(mdat))

	for i := int64(0); i < mdatLen-100; i++ {
		length := int64(binary.BigEndian.Uint32(mdat[i : i+4]))
		if length < 20 || length > 200 || i+4+length >= mdatLen {
			continue
		}
		b := mdat[i+4]
		if (b>>7)&1 != 0 || (b>>1)&0x3f != nalTypeVPS {
			continue
		}

		spsPos := i + 4 + length
		if spsPos+9 >= mdatLen {
			continue
		}
		spsLen := int64(binary.BigEndian.Uint32(mdat[spsPos : spsPos+4]))
		if spsLen < 10 || spsLen > 1000 {
			continue
		}
		spsB := mdat[spsPos+4]
		if (spsB>>7)&1 != 0 || (spsB>>1)&0x3f != nalTypeSPS {
			continue
		}

		ppsPos := spsPos + 4 + spsLen
		if ppsPos+5 >= mdatLen {
			continue
		}
		ppsLen := int64(binary.BigEndian.Uint32(mdat[ppsPos : ppsPos+4]))
		if ppsLen < 2 || ppsLen > 200 {
			continue
		}
		ppsB := mdat[ppsPos+4]
		if (ppsB>>7)&1 != 0 || (ppsB>>1)&0x3f != nalTypePPS {
			continue
		}

		candidates = append(candidates, int(i))
	}
	return candidates
}

func loadRefSTSD(path string) (vstsd, astsd []byte, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()

	var sz uint32
	if err := binary.Read(f, binary.BigEndian, &sz); err != nil {
		return nil, nil, err
	}
	vstsd = make([]byte, sz)
	if _, err := io.ReadFull(f, vstsd); err != nil {
		return nil, nil, err
	}

	if err := binary.Read(f, binary.BigEndian, &sz); err != nil {
		return nil, nil, err
	}
	astsd = make([]byte, sz)
	if _, err := io.ReadFull(f, astsd); err != nil {
		return nil, nil, err
	}
	return vstsd, astsd, nil
}

func writeAtom(w io.Writer, name string, content []byte) {
	hdr := make([]byte, 8)
	binary.BigEndian.PutUint32(hdr[:4], uint32(8+len(content)))
	copy(hdr[4:8], name)
	w.Write(hdr)
	if len(content) > 0 {
		w.Write(content)
	}
}

func buildBox(name string, parts ...[]byte) []byte {
	var content []byte
	for _, p := range parts {
		content = append(content, p...)
	}
	result := make([]byte, 8+len(content))
	binary.BigEndian.PutUint32(result[:4], uint32(8+len(content)))
	copy(result[4:8], name)
	copy(result[8:], content)
	return result
}

func u32(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

// Template atoms extracted from a working kalitka H265 recording (11-50.mp4).
// Only timescale, duration, trackID, width, height need patching.

// mvhd atom (116 bytes). Fields from a working file:
// Payload bytes 20-23: timescale (default 90000)
// Payload bytes 24-27: duration
// Payload bytes 104-107: next_track_id (set to 3)
var tmpltMVHD = [116]byte{
	0x00, 0x00, 0x00, 0x74, 0x6d, 0x76, 0x68, 0x64, // size=0x74, "mvhd"
	0x00, 0x00, 0x00, 0x00, // version=0, flags=0
	0x00, 0x00, 0x00, 0x00, // creation_time
	0x00, 0x00, 0x00, 0x00, // modification_time
	0x00, 0x01, 0x5f, 0x90, // timescale=90000
	0x00, 0x49, 0x78, 0x98, // duration (patched)
	0x00, 0x00, 0x00, 0x00, // rate
	0x01, 0x00, 0x01, 0x00, // volume + reserved
	0x00, 0x00, 0x00, 0x00, // reserved
	0x00, 0x00, 0x00, 0x00, // reserved
	0x00, 0x00, 0x00, 0x00, // reserved
	0x00, 0x01, 0x00, 0x00, // matrix[0]=0x00010000
	0x00, 0x00, 0x00, 0x00, // matrix[1]
	0x00, 0x00, 0x00, 0x00, // matrix[2]
	0x00, 0x01, 0x00, 0x00, // matrix[3]=0x00010000
	0x00, 0x00, 0x00, 0x00, // matrix[4]
	0x00, 0x00, 0x00, 0x00, // matrix[5]
	0x40, 0x00, 0x00, 0x00, // matrix[6]=0x40000000
	0x00, 0x00, 0x00, 0x00, // matrix[7]
	0x00, 0x00, 0x00, 0x00, // matrix[8]
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // pre_defined
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // pre_defined
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // pre_defined
	0x00, 0x00, 0x00, 0x00, // next_track_id=3
}

// tkhd video (100 bytes). Fields:
// Payload bytes 12-15: creation_time (set to 1 for trackID-like usage)
// Payload bytes 20-23: duration
// Payload bytes 84-87: width (0x0F000000 = 3840<<16)
// Payload bytes 88-91: height (0x08700000 = 2160<<16)
var tmpltTKHDVideo = [100]byte{
	0x00, 0x00, 0x00, 0x64, 0x74, 0x6b, 0x68, 0x64, // size=0x64, "tkhd"
	0x00, 0x00, 0x00, 0x00, // version=0, flags=0
	0x00, 0x00, 0x00, 0x03, // track_id=3 (patched: 1 for video)
	0x00, 0x00, 0x00, 0x00, // reserved
	0x00, 0x00, 0x00, 0x01, // duration
	0x00, 0x00, 0x00, 0x00, // reserved
	0x00, 0x00, 0x00, 0x00, // reserved
	0x00, 0x00, 0x00, 0x00, // layer + alternate_group
	0x00, 0x00, 0x00, 0x00, // volume + reserved
	0x00, 0x01, 0x00, 0x00, // matrix[0]=0x00010000
	0x00, 0x00, 0x00, 0x00, // matrix[1]
	0x00, 0x00, 0x00, 0x00, // matrix[2]
	0x00, 0x01, 0x00, 0x00, // matrix[3]=0x00010000
	0x00, 0x00, 0x00, 0x00, // matrix[4]
	0x00, 0x00, 0x00, 0x00, // matrix[5]
	0x40, 0x00, 0x00, 0x00, // matrix[6]=0x40000000
	0x00, 0x00, 0x00, 0x00, // matrix[7]
	0x00, 0x00, 0x00, 0x00, // matrix[8]
	0x0f, 0x00, 0x00, 0x00, // width=3840<<16
	0x08, 0x70, 0x00, 0x00, // height=2160<<16
}

// tkhd audio (100 bytes). Fields:
// Payload bytes 12-15: track_id (patched to 2)
// Payload bytes 20-23: duration
var tmpltTKHDAudio = [100]byte{
	0x00, 0x00, 0x00, 0x64, 0x74, 0x6b, 0x68, 0x64, // size=0x64, "tkhd"
	0x00, 0x00, 0x00, 0x00, // version=0, flags=0
	0x00, 0x00, 0x00, 0x03, // track_id (patched to 2)
	0x00, 0x00, 0x00, 0x00, // reserved
	0x00, 0x00, 0x00, 0x02, // duration
	0x00, 0x00, 0x00, 0x00, // reserved
	0x00, 0x00, 0x00, 0x00, // reserved
	0x01, 0x00, 0x00, 0x00, // layer + alternate_group
	0x00, 0x01, 0x00, 0x00, // volume=0x0100 + reserved
	0x00, 0x01, 0x00, 0x00, // matrix[0]=0x00010000
	0x00, 0x00, 0x00, 0x00, // matrix[1]
	0x00, 0x00, 0x00, 0x00, // matrix[2]
	0x00, 0x01, 0x00, 0x00, // matrix[3]=0x00010000
	0x00, 0x00, 0x00, 0x00, // matrix[4]
	0x00, 0x00, 0x00, 0x00, // matrix[5]
	0x40, 0x00, 0x00, 0x00, // matrix[6]=0x40000000
	0x00, 0x00, 0x00, 0x00, // matrix[7]
	0x00, 0x00, 0x00, 0x00, // matrix[8]
	0x00, 0x00, 0x00, 0x00, // width
	0x00, 0x00, 0x00, 0x00, // height
}

// mdhd video (40 bytes). Fields:
// Payload bytes 20-23: timescale=90000
// Payload bytes 24-27: duration
var tmpltMDHDVideo = [40]byte{
	0x00, 0x00, 0x00, 0x28, 0x6d, 0x64, 0x68, 0x64, // size=0x28, "mdhd"
	0x00, 0x00, 0x00, 0x00, // version=0, flags=0
	0x00, 0x00, 0x00, 0x00, // creation_time
	0x00, 0x00, 0x00, 0x00, // modification_time
	0x00, 0x01, 0x5f, 0x90, // timescale=90000
	0x00, 0x49, 0x78, 0x98, // duration (patched)
	0x55, 0xc4, 0x00, 0x00, // language + pre_defined
	0x00, 0x00, 0x00, 0x00,
}

// mdhd audio (40 bytes). Fields:
// Payload bytes 20-23: timescale=8000
// Payload bytes 24-27: duration
var tmpltMDHDAudio = [40]byte{
	0x00, 0x00, 0x00, 0x28, 0x6d, 0x64, 0x68, 0x64, // size=0x28, "mdhd"
	0x00, 0x00, 0x00, 0x00, // version=0, flags=0
	0x00, 0x00, 0x00, 0x00, // creation_time
	0x00, 0x00, 0x00, 0x00, // modification_time
	0x00, 0x00, 0x1f, 0x40, // timescale=8000
	0x00, 0x06, 0x87, 0xe0, // duration (patched)
	0x55, 0xc4, 0x00, 0x00, // language + pre_defined
	0x00, 0x00, 0x00, 0x00,
}

func buildMoov(vstsd, astsd []byte, vStco []uint32, vStsz []uint32, keyframes []uint32, aStco []uint32, aStsz []uint32, vDur, aDur uint32) []byte {
	// Patch mvhd
	mvhd := make([]byte, 116)
	copy(mvhd, tmpltMVHD[:])
	binary.BigEndian.PutUint32(mvhd[24:28], vDur)
	binary.BigEndian.PutUint32(mvhd[104:108], 3)

	// Patch video tkhd
	tkhdV := make([]byte, 100)
	copy(tkhdV, tmpltTKHDVideo[:])
	binary.BigEndian.PutUint32(tkhdV[12:16], 1) // track_id=1
	binary.BigEndian.PutUint32(tkhdV[20:24], vDur)

	// Patch audio tkhd
	tkhdA := make([]byte, 100)
	copy(tkhdA, tmpltTKHDAudio[:])
	binary.BigEndian.PutUint32(tkhdA[12:16], 2) // track_id=2
	binary.BigEndian.PutUint32(tkhdA[20:24], aDur)

	// Patch video mdhd
	mdhdV := make([]byte, 40)
	copy(mdhdV, tmpltMDHDVideo[:])
	binary.BigEndian.PutUint32(mdhdV[24:28], vDur)

	// Patch audio mdhd
	mdhdA := make([]byte, 40)
	copy(mdhdA, tmpltMDHDAudio[:])
	binary.BigEndian.PutUint32(mdhdA[24:28], aDur)

	vTrak := buildVideoTrak(tkhdV, mdhdV, vstsd, vStco, vStsz, keyframes)
	aTrak := buildAudioTrak(tkhdA, mdhdA, astsd, aStco, aStsz)

	// Templates already include atom headers, so concatenate + wrap in moov
	var body bytes.Buffer
	body.Write(mvhd)
	body.Write(vTrak)
	body.Write(aTrak)
	return buildBox("moov", body.Bytes())
}

func buildVideoTrak(tkhd, mdhd []byte, vstsd []byte, stco []uint32, stsz []uint32, keyframes []uint32) []byte {
	hdlrPayload := make([]byte, 37)
	copy(hdlrPayload[8:12], "vide")
	copy(hdlrPayload[24:37], "VideoHandler\x00")
	hdlr := buildBox("hdlr", hdlrPayload)

	vmhdPayload := []byte{0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	vmhd := buildBox("vmhd", vmhdPayload)

	drefEntry := make([]byte, 12)
	binary.BigEndian.PutUint32(drefEntry[0:4], 12)
	copy(drefEntry[4:8], "url ")
	binary.BigEndian.PutUint32(drefEntry[8:12], 1)
	dref := buildBox("dref", u32(0), u32(1), drefEntry)
	dinf := buildBox("dinf", dref)

	stbl := buildVideoSTBL(vstsd, stco, stsz, keyframes)
	minf := buildBox("minf", vmhd, dinf, stbl)
	mdia := buildBox("mdia", mdhd, hdlr, minf)
	return buildBox("trak", tkhd, mdia)
}

func buildVideoSTBL(vstsd []byte, stco []uint32, stsz []uint32, keyframes []uint32) []byte {
	n := uint32(len(stsz))

	sttsPayload := make([]byte, 0, 20)
	sttsPayload = append(sttsPayload, u32(0)...)
	sttsPayload = append(sttsPayload, u32(1)...)
	sttsPayload = append(sttsPayload, u32(n)...)
	sttsPayload = append(sttsPayload, u32(4500)...)
	stts := buildBox("stts", sttsPayload)

	stssPayload := make([]byte, 0, 8+4*len(keyframes))
	stssPayload = append(stssPayload, u32(0)...)
	stssPayload = append(stssPayload, u32(uint32(len(keyframes)))...)
	for _, k := range keyframes {
		stssPayload = append(stssPayload, u32(k)...)
	}
	stss := buildBox("stss", stssPayload)

	stsc := buildBox("stsc", u32(0), u32(1), u32(1), u32(1), u32(1))

	stszPayload := make([]byte, 0, 12+4*int(n))
	stszPayload = append(stszPayload, u32(0)...)
	stszPayload = append(stszPayload, u32(0)...)
	stszPayload = append(stszPayload, u32(n)...)
	for _, s := range stsz {
		stszPayload = append(stszPayload, u32(s)...)
	}
	stszAtom := buildBox("stsz", stszPayload)

	stcoPayload := make([]byte, 0, 8+4*len(stco))
	stcoPayload = append(stcoPayload, u32(0)...)
	stcoPayload = append(stcoPayload, u32(uint32(len(stco)))...)
	for _, c := range stco {
		stcoPayload = append(stcoPayload, u32(c)...)
	}
	stcoAtom := buildBox("stco", stcoPayload)

	return buildBox("stbl", vstsd, stts, stss, stsc, stszAtom, stcoAtom)
}

func buildAudioTrak(tkhd, mdhd []byte, astsd []byte, stco []uint32, stsz []uint32) []byte {
	hdlrPayload := make([]byte, 37)
	copy(hdlrPayload[8:12], "soun")
	copy(hdlrPayload[24:37], "SoundHandler\x00")
	hdlr := buildBox("hdlr", hdlrPayload)

	smhd := buildBox("smhd", []byte{0, 0, 0, 0})

	drefEntry := make([]byte, 12)
	binary.BigEndian.PutUint32(drefEntry[0:4], 12)
	copy(drefEntry[4:8], "url ")
	binary.BigEndian.PutUint32(drefEntry[8:12], 1)
	dref := buildBox("dref", u32(0), u32(1), drefEntry)
	dinf := buildBox("dinf", dref)

	stbl := buildAudioSTBL(astsd, stco, stsz)
	minf := buildBox("minf", smhd, dinf, stbl)
	mdia := buildBox("mdia", mdhd, hdlr, minf)
	return buildBox("trak", tkhd, mdia)
}

func buildAudioSTBL(astsd []byte, stco []uint32, stsz []uint32) []byte {
	n := uint32(len(stsz))

	sttsPayload := make([]byte, 0, 20)
	sttsPayload = append(sttsPayload, u32(0)...)
	sttsPayload = append(sttsPayload, u32(1)...)
	sttsPayload = append(sttsPayload, u32(n)...)
	sttsPayload = append(sttsPayload, u32(1024)...)
	stts := buildBox("stts", sttsPayload)

	stss := buildBox("stss", u32(0), u32(0))
	stsc := buildBox("stsc", u32(0), u32(1), u32(1), u32(1), u32(1))

	stszPayload := make([]byte, 0, 12+4*int(n))
	stszPayload = append(stszPayload, u32(0)...)
	stszPayload = append(stszPayload, u32(0)...)
	stszPayload = append(stszPayload, u32(n)...)
	for _, s := range stsz {
		stszPayload = append(stszPayload, u32(s)...)
	}
	stszAtom := buildBox("stsz", stszPayload)

	stcoPayload := make([]byte, 0, 8+4*len(stco))
	stcoPayload = append(stcoPayload, u32(0)...)
	stcoPayload = append(stcoPayload, u32(uint32(len(stco)))...)
	for _, c := range stco {
		stcoPayload = append(stcoPayload, u32(c)...)
	}
	stcoAtom := buildBox("stco", stcoPayload)

	return buildBox("stbl", astsd, stts, stss, stsc, stszAtom, stcoAtom)
}
