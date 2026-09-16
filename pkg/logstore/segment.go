package logstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"time"
	"unsafe"
)

const (
	// BlockSize is the direct I/O paging unit in bytes.
	BlockSize = 4096

	// SegmentHeaderMagic is "PLXS" (0x504C5853).
	SegmentHeaderMagic uint32 = 0x504C5853

	// SegmentVersion is the on-disk segment format version.
	SegmentVersion uint16 = 1

	// BlockSizeExp is 12 for 4096 bytes (2^12).
	BlockSizeExp uint16 = 12

	// EntryMagic is "JF" (0x4A46).
	EntryMagic uint16 = 0x4A46

	// EntryHeaderSize is the fixed 26-byte entry header.
	EntryHeaderSize = 26

	// DefaultSegmentSize is 64 MiB.
	DefaultSegmentSize = 64 * 1024 * 1024

	// DefaultMaxPayloadSize is 8 MiB.
	DefaultMaxPayloadSize = 8 * 1024 * 1024
)

var (
	// ErrCorruptEntry is returned when magic or CRC validation fails.
	ErrCorruptEntry = errors.New("corrupt log entry")
	// ErrCorruptHeader is returned when segment header validation fails.
	ErrCorruptHeader = errors.New("corrupt segment header")
	// ErrPayloadTooLarge is returned when payload exceeds configured maximum.
	ErrPayloadTooLarge = errors.New("payload exceeds maximum size")
)

// AlignedBuffer returns a byte slice of given size aligned to 4096-byte boundary.
func AlignedBuffer(size int) []byte {
	buf := make([]byte, size+BlockSize)
	addr := uintptr(unsafe.Pointer(&buf[0]))
	offset := int((BlockSize - (addr % BlockSize)) % BlockSize)
	return buf[offset : offset+size : offset+size]
}

// EncodeSegmentHeader serializes the 4096-byte header block for a segment.
func EncodeSegmentHeader(baseIndex uint64) []byte {
	buf := AlignedBuffer(BlockSize)
	binary.BigEndian.PutUint32(buf[0:4], SegmentHeaderMagic)
	binary.BigEndian.PutUint16(buf[4:6], SegmentVersion)
	binary.BigEndian.PutUint16(buf[6:8], BlockSizeExp)
	binary.BigEndian.PutUint64(buf[8:16], baseIndex)
	binary.BigEndian.PutUint64(buf[16:24], uint64(time.Now().UnixNano()))

	crc := crc32.ChecksumIEEE(buf[0:24])
	binary.BigEndian.PutUint32(buf[24:28], crc)
	// Remaining bytes [28:4096] are zero
	return buf
}

// ValidateSegmentHeader checks the magic, version, and CRC of a segment header block.
func ValidateSegmentHeader(buf []byte) (baseIndex uint64, err error) {
	if len(buf) < BlockSize {
		return 0, fmt.Errorf("%w: header block shorter than %d bytes", ErrCorruptHeader, BlockSize)
	}
	magic := binary.BigEndian.Uint32(buf[0:4])
	if magic != SegmentHeaderMagic {
		return 0, fmt.Errorf("%w: invalid magic 0x%X", ErrCorruptHeader, magic)
	}
	version := binary.BigEndian.Uint16(buf[4:6])
	if version != SegmentVersion {
		return 0, fmt.Errorf("%w: unsupported version %d", ErrCorruptHeader, version)
	}
	expectedCRC := binary.BigEndian.Uint32(buf[24:28])
	actualCRC := crc32.ChecksumIEEE(buf[0:24])
	if expectedCRC != actualCRC {
		return 0, fmt.Errorf("%w: CRC mismatch (expected %d, got %d)", ErrCorruptHeader, expectedCRC, actualCRC)
	}
	baseIndex = binary.BigEndian.Uint64(buf[8:16])
	return baseIndex, nil
}

// EntryHeader represents the 26-byte binary framing of a Raft log entry.
type EntryHeader struct {
	Magic uint16
	Term  uint64
	Index uint64
	Size  uint32
	CRC32 uint32
}

// EncodeEntry writes the 26-byte header and payload into a continuous buffer.
func EncodeEntry(term, index uint64, payload []byte) ([]byte, error) {
	if len(payload) > DefaultMaxPayloadSize {
		return nil, ErrPayloadTooLarge
	}
	totalLen := EntryHeaderSize + len(payload)
	buf := make([]byte, totalLen)

	binary.BigEndian.PutUint16(buf[0:2], EntryMagic)
	binary.BigEndian.PutUint64(buf[2:10], term)
	binary.BigEndian.PutUint64(buf[10:18], index)
	binary.BigEndian.PutUint32(buf[18:22], uint32(len(payload)))

	// CRC covers header [0:22) + payload
	h := crc32.NewIEEE()
	h.Write(buf[0:22])
	h.Write(payload)
	crc := h.Sum32()

	binary.BigEndian.PutUint32(buf[22:26], crc)
	copy(buf[26:], payload)

	return buf, nil
}

// DecodeEntryHeader decodes and validates only the 26-byte header from a slice.
func DecodeEntryHeader(buf []byte) (*EntryHeader, error) {
	if len(buf) < EntryHeaderSize {
		return nil, errors.New("buffer too short for entry header")
	}
	magic := binary.BigEndian.Uint16(buf[0:2])
	if magic != EntryMagic {
		return nil, fmt.Errorf("%w: invalid magic 0x%X", ErrCorruptEntry, magic)
	}
	hdr := &EntryHeader{
		Magic: magic,
		Term:  binary.BigEndian.Uint64(buf[2:10]),
		Index: binary.BigEndian.Uint64(buf[10:18]),
		Size:  binary.BigEndian.Uint32(buf[18:22]),
		CRC32: binary.BigEndian.Uint32(buf[22:26]),
	}
	if hdr.Size > DefaultMaxPayloadSize {
		return nil, fmt.Errorf("%w: size %d exceeds limit", ErrCorruptEntry, hdr.Size)
	}
	return hdr, nil
}

// ValidateEntryCRC verifies the checksum of the entry buffer (header + payload).
func ValidateEntryCRC(buf []byte) error {
	if len(buf) < EntryHeaderSize {
		return errors.New("buffer too short")
	}
	hdr, err := DecodeEntryHeader(buf[:EntryHeaderSize])
	if err != nil {
		return err
	}
	if len(buf) < EntryHeaderSize+int(hdr.Size) {
		return fmt.Errorf("%w: payload truncated (expected %d, got %d)", ErrCorruptEntry, hdr.Size, len(buf)-EntryHeaderSize)
	}

	payload := buf[EntryHeaderSize : EntryHeaderSize+int(hdr.Size)]
	h := crc32.NewIEEE()
	h.Write(buf[0:22])
	h.Write(payload)
	actualCRC := h.Sum32()

	if actualCRC != hdr.CRC32 {
		return fmt.Errorf("%w: CRC mismatch (expected %d, got %d)", ErrCorruptEntry, hdr.CRC32, actualCRC)
	}
	return nil
}
