package main

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

// The entry codec from specs/raft_log_store.md §8.1. Both segment-based arms
// use it verbatim so that framing overhead is identical between them.

const (
	HeaderSize = 26
	MagicNum   = 0x4A46 // "JF"
)

var ErrCorruptEntry = errors.New("plexus: log entry corruption detected")

type Entry struct {
	Term    uint64
	Index   uint64
	Payload []byte
}

func EncodeEntry(e Entry, dest []byte) int {
	payloadLen := uint32(len(e.Payload))

	binary.BigEndian.PutUint16(dest[0:2], MagicNum)
	binary.BigEndian.PutUint64(dest[2:10], e.Term)
	binary.BigEndian.PutUint64(dest[10:18], e.Index)
	binary.BigEndian.PutUint32(dest[18:22], payloadLen)

	copy(dest[HeaderSize:HeaderSize+payloadLen], e.Payload)

	sum := crc32.ChecksumIEEE(dest[0 : HeaderSize-4])
	sum = crc32.Update(sum, crc32.IEEETable, e.Payload)
	binary.BigEndian.PutUint32(dest[22:26], sum)

	return HeaderSize + int(payloadLen)
}

func DecodeEntry(src []byte, maxPayload int) (Entry, int, error) {
	if len(src) < HeaderSize {
		return Entry{}, 0, ErrCorruptEntry
	}
	if binary.BigEndian.Uint16(src[0:2]) != MagicNum {
		return Entry{}, 0, ErrCorruptEntry
	}

	term := binary.BigEndian.Uint64(src[2:10])
	index := binary.BigEndian.Uint64(src[10:18])
	payloadLen := int(binary.BigEndian.Uint32(src[18:22]))
	storedCRC := binary.BigEndian.Uint32(src[22:26])

	total := HeaderSize + payloadLen
	if payloadLen > maxPayload || total > len(src) {
		return Entry{}, 0, ErrCorruptEntry
	}

	sum := crc32.ChecksumIEEE(src[0 : HeaderSize-4])
	sum = crc32.Update(sum, crc32.IEEETable, src[HeaderSize:total])
	if sum != storedCRC {
		return Entry{}, 0, ErrCorruptEntry
	}

	payload := make([]byte, payloadLen)
	copy(payload, src[HeaderSize:total])
	return Entry{Term: term, Index: index, Payload: payload}, total, nil
}

// payloadFor builds a deterministic payload so the verification pass can
// confirm an arm returned exactly what was written.
func payloadFor(index uint64, size int) []byte {
	p := make([]byte, size)
	for i := range p {
		p[i] = byte(index>>(8*(i%8)) ^ uint64(i))
	}
	return p
}
