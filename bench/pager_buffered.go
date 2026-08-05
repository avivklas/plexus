//go:build linux

package main

import (
	"fmt"
	"os"
	"syscall"
)

// bufferedPager is the mechanism used by mature engines such as etcd and
// hashicorp/raft-wal: ordinary buffered writes through the OS page cache, made
// durable with fdatasync. It stages in user space exactly like directPager so
// the two differ only in alignment and durability mechanism, never in framing.
type bufferedPager struct {
	f *os.File

	buf     []byte
	bufLen  int
	baseOff int64
	logical int64

	stats pagerStats
}

func newBufferedPager(path string, size int64, stagingSize int) (*bufferedPager, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open buffered: %w", err)
	}
	if err := syscall.Fallocate(int(f.Fd()), 0, 0, size); err != nil {
		f.Close()
		return nil, fmt.Errorf("fallocate: %w", err)
	}
	return &bufferedPager{f: f, buf: make([]byte, stagingSize)}, nil
}

func (p *bufferedPager) Offset() int64 { return p.logical }

func (p *bufferedPager) Stage(b []byte) error {
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

func (p *bufferedPager) Flush() error {
	if p.bufLen == 0 {
		return nil
	}
	// No alignment constraint: write exactly the bytes we have, no padding.
	if _, err := p.f.WriteAt(p.buf[:p.bufLen], p.baseOff); err != nil {
		return fmt.Errorf("buffered write at %d: %w", p.baseOff, err)
	}
	p.stats.DeviceWrites++
	p.stats.DeviceBytes += int64(p.bufLen)

	// fdatasync rather than fsync: we pre-allocated, so file size metadata is
	// not changing and there is no reason to pay for an inode update.
	if err := syscall.Fdatasync(int(p.f.Fd())); err != nil {
		return fmt.Errorf("fdatasync: %w", err)
	}
	p.stats.SyncCalls++

	p.baseOff += int64(p.bufLen)
	p.bufLen = 0
	return nil
}

func (p *bufferedPager) ReadAt(dst []byte, off int64) error {
	staged := p.baseOff
	if off >= staged && off+int64(len(dst)) <= staged+int64(p.bufLen) {
		copy(dst, p.buf[off-staged:])
		return nil
	}
	if _, err := p.f.ReadAt(dst, off); err != nil {
		return fmt.Errorf("buffered read at %d: %w", off, err)
	}
	return nil
}

func (p *bufferedPager) SeekTo(off int64) error {
	p.logical = off
	p.baseOff = off
	p.bufLen = 0
	return nil
}

func (p *bufferedPager) ZeroTo(off int64) error {
	if off >= p.logical {
		return nil
	}
	zero := make([]byte, 1<<20)
	for pos := off; pos < p.logical; {
		n := int64(len(zero))
		if pos+n > p.logical {
			n = p.logical - pos
		}
		if _, err := p.f.WriteAt(zero[:n], pos); err != nil {
			return fmt.Errorf("zero write at %d: %w", pos, err)
		}
		p.stats.DeviceWrites++
		p.stats.DeviceBytes += n
		p.stats.PaddingBytes += n
		pos += n
	}
	if err := syscall.Fdatasync(int(p.f.Fd())); err != nil {
		return err
	}
	p.stats.SyncCalls++
	return p.SeekTo(off)
}

func (p *bufferedPager) Stats() pagerStats { return p.stats }

func (p *bufferedPager) Close() error {
	if err := p.Flush(); err != nil {
		p.f.Close()
		return err
	}
	return p.f.Close()
}
