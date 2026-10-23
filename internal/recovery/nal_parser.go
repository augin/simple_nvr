package recovery

import (
	"encoding/binary"
	"fmt"
)

// NAL unit types (H.264 and HEVC) we care about while scanning a stream.
const (
	nalTypeVPS = 32
	nalTypeSPS = 33
	nalTypePPS = 34

	nalTypeH264SPS = 7
	nalTypeH264PPS = 8
)

// Size limits used to filter plausible length-prefixed NALs while scanning.
const (
	vpsLenMin, vpsLenMax         = 20, 200
	spsLenMin, spsLenMax         = 10, 1000
	ppsLenMin, ppsLenMax         = 10, 1000
	hevcPPSLenMin, hevcPPSLenMax = 10, 200
	maxParamSets                 = 10
)

// bitReader reads big-endian bits from a byte slice. Reading past the end
// yields zero bits and latches an error instead of panicking.
type bitReader struct {
	data []byte
	pos  int
	err  error
}

func (r *bitReader) readBit() int {
	idx := r.pos / 8
	if idx >= len(r.data) {
		if r.err == nil {
			r.err = fmt.Errorf("ran out of bits at %d", r.pos)
		}
		r.pos++
		return 0
	}
	bit := (r.data[idx] >> (7 - r.pos%8)) & 1
	r.pos++
	return int(bit)
}

func (r *bitReader) readBits(n int) int {
	value := 0
	for i := 0; i < n; i++ {
		value = (value << 1) | r.readBit()
	}
	return value
}

func (r *bitReader) skip(n int) {
	for i := 0; i < n; i++ {
		r.readBit()
	}
}

// readUE reads an unsigned exp-Golomb code.
func (r *bitReader) readUE() int {
	leadingZeroes := 0
	for r.readBit() == 0 {
		leadingZeroes++
		if leadingZeroes > 31 {
			if r.err == nil {
				r.err = fmt.Errorf("exp-golomb code too long")
			}
			return 0
		}
	}
	value := 0
	for i := 0; i < leadingZeroes; i++ {
		value = (value << 1) | r.readBit()
	}
	return value + (1 << leadingZeroes) - 1
}

// readSE reads a signed exp-Golomb code.
func (r *bitReader) readSE() int {
	ue := r.readUE()
	if ue%2 == 0 {
		return -(ue / 2)
	}
	return (ue + 1) / 2
}

// stripEmulationPrevention removes 0x03 emulation prevention bytes from an
// RBSP (the NAL header byte is kept).
func stripEmulationPrevention(nal []byte) []byte {
	out := make([]byte, 0, len(nal))
	zeros := 0
	for i, b := range nal {
		if i > 0 && zeros >= 2 && b == 0x03 {
			zeros = 0
			continue
		}
		if b == 0 {
			zeros++
		} else {
			zeros = 0
		}
		out = append(out, b)
	}
	return out
}

// parseSPSResolution extracts the encoded picture size from an H.264 SPS NAL.
// Returns 0,0 when the SPS cannot be decoded.
func parseSPSResolution(sps []byte) (int, int) {
	if len(sps) < 4 {
		return 0, 0
	}
	sps = stripEmulationPrevention(sps)
	if len(sps) < 4 {
		return 0, 0
	}
	r := &bitReader{data: sps[1:]} // skip the NAL header byte

	profileIDC := r.readBits(8)
	r.skip(8)     // constraint set flags + reserved bits
	r.readBits(8) // level_idc
	r.readUE()    // seq_parameter_set_id

	chromaFormatIDC := 1
	if profileIDC >= 100 && profileIDC <= 244 {
		chromaFormatIDC = r.readUE()
		if chromaFormatIDC == 3 {
			r.skip(1) // separate_colour_plane_flag
		}
		r.readUE()            // bit_depth_luma_minus8
		r.readUE()            // bit_depth_chroma_minus8
		r.skip(1)             // qpprime_y_zero_transform_bypass_flag
		if r.readBit() != 0 { // seq_scaling_matrix_present_flag
			numLists := 8
			if chromaFormatIDC == 3 {
				numLists = 12
			}
			for i := 0; i < numLists; i++ {
				if r.readBit() == 0 {
					continue
				}
				size := 16
				if i == 1 && chromaFormatIDC != 3 {
					size = 64
				}
				readScalingList(r, size)
			}
		}
	}

	r.readUE() // log2_max_frame_num_minus4
	picOrderCntType := r.readUE()
	switch picOrderCntType {
	case 0:
		r.readUE() // log2_max_pic_order_cnt_lsb_minus4
	case 1:
		r.skip(1)  // delta_pic_order_always_zero_flag
		r.readSE() // offset_for_non_ref_pic
		r.readSE() // offset_for_top_to_bottom_field
		numRefFrames := r.readUE()
		for i := 0; i < numRefFrames; i++ {
			r.readSE()
		}
	}

	r.readUE() // max_num_ref_frames
	r.skip(1)  // gaps_in_frame_num_value_allowed_flag

	picWidthInMbsMinus1 := r.readUE()
	picHeightInMapUnitsMinus1 := r.readUE()
	frameMbsOnlyFlag := r.readBit()
	if frameMbsOnlyFlag == 0 {
		r.skip(1) // mb_adaptive_frame_field_flag
	}
	r.skip(1) // direct_8x8_inference_flag

	width := (picWidthInMbsMinus1 + 1) * 16
	height := (picHeightInMapUnitsMinus1 + 1) * 16 * (2 - frameMbsOnlyFlag)

	if r.readBit() != 0 { // frame_cropping_flag
		cropLeft := r.readUE()
		cropRight := r.readUE()
		cropTop := r.readUE()
		cropBottom := r.readUE()

		cropUnitX, cropUnitY := cropUnits(chromaFormatIDC, frameMbsOnlyFlag)
		width -= (cropLeft + cropRight) * cropUnitX
		height -= (cropTop + cropBottom) * cropUnitY
	}

	if r.err != nil || width <= 0 || height <= 0 {
		return 0, 0
	}
	return width, height
}

// cropUnits returns the luma sample step of one crop unit, see ITU-T H.264
// section 7.4.2.1.
func cropUnits(chromaFormatIDC, frameMbsOnlyFlag int) (x, y int) {
	y = 2 - frameMbsOnlyFlag
	switch chromaFormatIDC {
	case 1: // 4:2:0
		return 2, 2 * y
	case 2: // 4:2:2
		return 2, y
	default: // monochrome and 4:4:4
		return 1, y
	}
}

// readScalingList consumes a scaling list of the given size (inverse scan is
// irrelevant here: we only need the correct bit position).
func readScalingList(r *bitReader, size int) {
	lastScale := 8
	nextScale := 8
	for j := 0; j < size; j++ {
		if nextScale != 0 {
			deltaScale := r.readSE()
			nextScale = (lastScale + deltaScale + 256) % 256
		}
		if nextScale != 0 {
			lastScale = nextScale
		}
	}
}

// findMdatDataOffset returns the offset of the mdat payload (after its
// header) or an error when the file has no usable mdat atom.
func findMdatDataOffset(data []byte) (int, error) {
	size := int64(len(data))

	for pos := int64(0); pos+mp4HeaderSize <= size; {
		atomSize := int64(binary.BigEndian.Uint32(data[pos : pos+4]))
		atomType := string(data[pos+4 : pos+8])

		if atomType == "mdat" {
			dataOffset := pos + mp4HeaderSize
			if atomSize == 1 { // 64 bit extended size
				if pos+mp4ExtendedHeaderSize > size {
					return 0, fmt.Errorf("mdat extended size out of range")
				}
				atomSize = int64(binary.BigEndian.Uint64(data[pos+8 : pos+16]))
				dataOffset = pos + mp4ExtendedHeaderSize
			}
			if atomSize == 0 || (atomSize >= mp4HeaderSize && pos+atomSize <= size) {
				return int(dataOffset), nil
			}
		}

		if atomSize < mp4HeaderSize || atomSize > size-pos {
			// Broken header: scan forward byte by byte for the next atom.
			pos++
			continue
		}
		pos += atomSize
	}
	return 0, fmt.Errorf("mdat atom not found")
}

// nalTypeHEVC extracts the HEVC NAL type from the first payload byte.
func nalTypeHEVC(b byte) byte { return (b >> 1) & 0x3f }

// nalTypeH264 extracts the H.264 NAL type from the first payload byte.
func nalTypeH264(b byte) byte { return b & 0x1f }

// forbiddenBit reports whether the NAL header forbidden bit is set.
func forbiddenBit(b byte) bool { return b>>7&1 != 0 }

// findNALOffsets scans length-prefixed NALs and returns offsets of those whose
// first two header bytes and length pass the given filters.
func findNALOffsets(mdat []byte, lenMin, lenMax int, matches func(b0, b1 byte) bool) []int {
	var offsets []int

	for i := 0; i+4 < len(mdat); i++ {
		length := int(binary.BigEndian.Uint32(mdat[i : i+4]))
		if length < lenMin || length > lenMax || i+4+length > len(mdat) {
			continue
		}
		b := mdat[i+4]
		if forbiddenBit(b) {
			continue
		}
		if i+5 < len(mdat) && matches(b, mdat[i+5]) {
			offsets = append(offsets, i)
		}
	}
	return offsets
}

// hevcParamSetHeader matches the two-byte HEVC NAL header of a parameter set:
// the first byte must be exactly forbidden|type (no layer bits), the second
// byte must carry nuh_layer_id=0 and a non-zero nuh_temporal_id_plus1.
//
// This matters because a one-byte view of an H.264 slice (0x41 = type 1,
// nal_ref_idc 2) reads as HEVC VPS (type 32), so accepting 0x41/0x43/0x45 as
// HEVC headers would misclassify short H.264 slices as parameter sets.
func hevcParamSetHeader(b0, b1, expectedType byte) bool {
	return nalTypeHEVC(b0) == expectedType && b0&1 == 0 &&
		b1>>3 == 0 && b1&7 != 0
}

// findValidVPS returns offsets of HEVC VPS NALs.
func findValidVPS(mdat []byte) []int {
	return findNALOffsets(mdat, vpsLenMin, vpsLenMax, func(b0, b1 byte) bool {
		return hevcParamSetHeader(b0, b1, nalTypeVPS)
	})
}

// findValidHEVCSPS returns offsets of HEVC SPS NALs.
func findValidHEVCSPS(mdat []byte) []int {
	return findNALOffsets(mdat, spsLenMin, spsLenMax, func(b0, b1 byte) bool {
		return hevcParamSetHeader(b0, b1, nalTypeSPS)
	})
}

// findValidHEVCPPS returns offsets of HEVC PPS NALs.
func findValidHEVCPPS(mdat []byte) []int {
	return findNALOffsets(mdat, hevcPPSLenMin, hevcPPSLenMax, func(b0, b1 byte) bool {
		return hevcParamSetHeader(b0, b1, nalTypePPS)
	})
}

// findValidSPS returns offsets of H.264 SPS NALs.
func findValidSPS(mdat []byte) []int {
	return findNALOffsets(mdat, spsLenMin, spsLenMax, func(b0, _ byte) bool {
		return nalTypeH264(b0) == nalTypeH264SPS
	})
}

// findValidH264PPS returns offsets of H.264 PPS NALs.
func findValidH264PPS(mdat []byte) []int {
	return findNALOffsets(mdat, ppsLenMin, ppsLenMax, func(b0, _ byte) bool {
		return nalTypeH264(b0) == nalTypeH264PPS
	})
}

// extractAllParamSets prepends the parameter sets of the selected codec as
// Annex B units so decoders get configuration before the first frame.
func extractAllParamSets(mdat []byte, isHEVC bool) []byte {
	var anchors []int
	if isHEVC {
		anchors = append(anchors, findValidVPS(mdat)...)
		anchors = append(anchors, findValidHEVCSPS(mdat)...)
		anchors = append(anchors, findValidHEVCPPS(mdat)...)
	} else {
		anchors = append(anchors, findValidSPS(mdat)...)
		anchors = append(anchors, findValidH264PPS(mdat)...)
	}

	var out []byte
	added := 0
	for _, offset := range anchors {
		if added >= maxParamSets {
			break
		}
		length := int(binary.BigEndian.Uint32(mdat[offset : offset+4]))
		if length <= 0 || offset+4+length > len(mdat) {
			continue
		}
		out = append(out, 0x00, 0x00, 0x00, 0x01)
		out = append(out, mdat[offset+4:offset+4+length]...)
		added++
	}
	return out
}
