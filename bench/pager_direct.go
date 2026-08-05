//go:build linux

package main

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

const BlockSize = 4096

// alignedBuffer returns a slice whose first byte sits on a BlockSize boundary,
// as O_DIRECT requires (specs/raft_log_store.md §4.2).
func alignedBuffer(size int) []byte {
	buf := make([]byte, size+BlockSize)
	addr := uintptr(unsafe.Pointer(&buf[0]))
	off := int((BlockSize - (addr % BlockSize)) % BlockSize)
	return buf[off : off+size : off+size]
}

// directPager implements the paging layer described in §4.3 and §4.4: entries
// are staged in an aligned user-space buffer and flushed in whole 4 KiB blocks
// with O_DIRECT|O_DSYNC. A partially filled tail block is zero-padded, written,
// kept live in the buffer, and rewritten by the next flush.
type directPager struct {
	f       *os.File
	segSize int64

	buf     []byte // aligned staging buffer
	bufLen  int    // bytes currently staged
	baseOff int64  // block-aligned file offset that buf[0] maps to
	logical int64  // logical end-of-data within the segment

	scratch []byte // one aligned block, for unaligned reads
	stats   pagerStats

	tailDirty bool // the tail block was already written once

	// Read-ahead window (§4.5.5). O_DIRECT forgoes kernel read-ahead, so a
	// sequential reader must batch device reads itself or pay a full device
	// round trip per entry.
	raBuf []byte
	raOff int64
	raLen int
}

// directReadUnit is the read-ahead span in bytes; 0 disables read-ahead, which
// is the naive O_DIRECT implementation.
var directReadUnit = 1 << 20

func newDirectPager(path string, size int64, stagingSize int) (*directPager, error) {
	f, err := os.OpenFile(path,
		os.O_CREATE|os.O_RDWR|syscall.O_DIRECT|syscall.O_DSYNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open direct: %w", err)
	}
	// Pre-allocate so appends never trigger filesystem metadata work (§3.1.4).
	if err := syscall.Fallocate(int(f.Fd()), 0, 0, size); err != nil {
		f.Close()
		return nil, fmt.Errorf("fallocate: %w", err)
	}
	// The staging buffer must be a whole number of blocks (spec §9), otherwise
	// zero-padding the final partial block runs past the end of the slice.
	staging := ((stagingSize + BlockSize - 1) / BlockSize) * BlockSize
	return &directPager{
		f:       f,
		segSize: size,
		buf:     alignedBuffer(staging),
		scratch: alignedBuffer(BlockSize),
	}, nil
}

func (p *directPager) Offset() int64 { return p.logical }

func (p *directPager) Stage(b []byte) error {
	// Flush if this entry would not fit alongside what is already staged.
	if p.bufLen+len(b) > len(p.buf) {
		if err := p.Flush(); err != nil {
			return err
		}
	}
	if p.bufLen+len(b) > len(p.buf) {
		return fmt.Errorf("entry of %d bytes exceeds staging buffer", len(b))
	}
	copy(p.buf[p.bufLen:], b)
	p.bufLen += len(b)
	p.logical += int64(len(b))
	p.stats.PayloadBytes += int64(len(b))
	return nil
}

func (p *directPager) Flush() error {
	if p.bufLen == 0 {
		return nil
	}
	// Round up to a whole number of blocks and zero the padding region.
	n := ((p.bufLen + BlockSize - 1) / BlockSize) * BlockSize
	padding := n - p.bufLen
	for i := p.bufLen; i < n; i++ {
		p.buf[i] = 0
	}

	if _, err := p.f.WriteAt(p.buf[:n], p.baseOff); err != nil {
		return fmt.Errorf("direct write at %d len %d: %w", p.baseOff, n, err)
	}
	p.raLen = 0 // the window may now be stale
	p.stats.DeviceWrites++
	p.stats.DeviceBytes += int64(n)
	p.stats.PaddingBytes += int64(padding)
	if p.tailDirty {
		p.stats.RewrittenBlks++
	}
	// O_DSYNC already made this durable; no separate sync syscall is needed.

	// Retain the partial tail block so the next flush overwrites its padding
	// with real entry data (§4.4.3, §4.4.4).
	keep := p.bufLen % BlockSize
	if keep > 0 {
		copy(p.buf[:keep], p.buf[p.bufLen-keep:p.bufLen])
		p.baseOff += int64(p.bufLen - keep)
		p.tailDirty = true
	} else {
		p.baseOff += int64(p.bufLen)
		p.tailDirty = false
	}
	p.bufLen = keep
	return nil
}

func (p *directPager) ReadAt(dst []byte, off int64) error {
	// Serve bytes still sitting in the staging buffer from memory (§4.5.4).
	staged := p.baseOff
	if off >= staged && off+int64(len(dst)) <= staged+int64(p.bufLen) {
		copy(dst, p.buf[off-staged:])
		return nil
	}

	// Serve from the read-ahead window if it covers the request.
	if p.raLen > 0 && off >= p.raOff && off+int64(len(dst)) <= p.raOff+int64(p.raLen) {
		copy(dst, p.raBuf[off-p.raOff:])
		return nil
	}

	// Expand the request out to block boundaries, since O_DIRECT reads must be
	// aligned too (§4.5.2).
	start := (off / BlockSize) * BlockSize
	end := ((off + int64(len(dst)) + BlockSize - 1) / BlockSize) * BlockSize
	span := int(end - start)

	// Pull a whole read-ahead window instead of just the requested blocks, so
	// the next sequential entries are served from memory.
	if n := int64(directReadUnit); n >= int64(span) && start+n <= p.segSize {
		if p.raBuf == nil {
			p.raBuf = alignedBuffer(directReadUnit)
		}
		if _, err := p.f.ReadAt(p.raBuf[:n], start); err == nil {
			p.raOff, p.raLen = start, int(n)
			copy(dst, p.raBuf[off-start:])
			return nil
		}
		p.raLen = 0
	}

	var tmp []byte
	if span <= len(p.scratch) {
		tmp = p.scratch[:span]
	} else {
		tmp = alignedBuffer(span)
	}
	if _, err := p.f.ReadAt(tmp, start); err != nil {
		return fmt.Errorf("direct read at %d len %d: %w", start, span, err)
	}
	copy(dst, tmp[off-start:])
	return nil
}

func (p *directPager) SeekTo(off int64) error {
	p.logical = off
	p.raLen = 0
	// Reload the block containing the new tail so later appends rewrite it
	// without clobbering the live entries in front of it (§6.1.5).
	base := (off / BlockSize) * BlockSize
	keep := int(off - base)
	if keep > 0 {
		if _, err := p.f.ReadAt(p.scratch[:BlockSize], base); err != nil {
			return fmt.Errorf("truncate reload at %d: %w", base, err)
		}
		copy(p.buf[:keep], p.scratch[:keep])
		p.tailDirty = true
	} else {
		p.tailDirty = false
	}
	p.baseOff = base
	p.bufLen = keep
	return nil
}

// ZeroTo physically overwrites the truncated region, the strategy §6.1 rejects.
// Present only so the benchmark can quantify what rejecting it saves.
func (p *directPager) ZeroTo(off int64) error {
	if off >= p.logical {
		return nil
	}
	base := (off / BlockSize) * BlockSize
	end := ((p.logical + BlockSize - 1) / BlockSize) * BlockSize

	// The block holding the truncation point still contains live entries in
	// front of it, so zero only its tail rather than the whole block.
	start := base
	if keep := int(off - base); keep > 0 {
		if _, err := p.f.ReadAt(p.scratch[:BlockSize], base); err != nil {
			return fmt.Errorf("zero read-modify-write at %d: %w", base, err)
		}
		for i := keep; i < BlockSize; i++ {
			p.scratch[i] = 0
		}
		if _, err := p.f.WriteAt(p.scratch[:BlockSize], base); err != nil {
			return fmt.Errorf("zero write at %d: %w", base, err)
		}
		p.stats.DeviceWrites++
		p.stats.DeviceBytes += BlockSize
		p.stats.PaddingBytes += int64(BlockSize - keep)
		start = base + BlockSize
	}

	zero := alignedBuffer(1 << 20)
	for pos := start; pos < end; {
		n := int64(len(zero))
		if pos+n > end {
			n = end - pos
		}
		if _, err := p.f.WriteAt(zero[:n], pos); err != nil {
			return fmt.Errorf("zero write at %d: %w", pos, err)
		}
		p.stats.DeviceWrites++
		p.stats.DeviceBytes += n
		p.stats.PaddingBytes += n
		pos += n
	}
	return p.SeekTo(off)
}

func (p *directPager) Stats() pagerStats { return p.stats }

func (p *directPager) Close() error {
	if err := p.Flush(); err != nil {
		p.f.Close()
		return err
	}
	return p.f.Close()
}
