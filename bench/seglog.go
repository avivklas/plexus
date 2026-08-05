//go:build linux

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// Store is the contract every benchmark arm implements. Append MUST NOT return
// until the entries are durable, so all arms are compared under the same
// durability promise.
type Store interface {
	Append(entries []Entry) error
	Get(index uint64) (Entry, error)
	TruncateTail(lastIndex uint64) error
	FirstIndex() uint64
	LastIndex() uint64
	Stats() pagerStats
	Close() error
}

const segHeaderSize = BlockSize

type pagerFactory func(path string, size int64, staging int) (pager, error)

type segment struct {
	baseIndex uint64
	path      string
	pg        pager
}

type loc struct {
	seg    int32
	off    int64
	length int32
	term   uint64
}

// segLog is the append-only segmented log from the spec. It is shared verbatim
// by the direct-I/O and buffered arms; only newPager differs.
type segLog struct {
	dir      string
	segSize  int64
	staging  int
	newPager pagerFactory
	name     string

	// zeroOnTruncate physically overwrites truncated bytes instead of dropping
	// them logically, so the benchmark can price the strategy §6.1 rejects.
	zeroOnTruncate bool

	segs  []*segment
	index []loc // index[i] describes entry firstIdx+i
	first uint64
	last  uint64

	scratch []byte
	retired pagerStats // stats from sealed segments
}

func newSegLog(dir, name string, segSize int64, staging int, f pagerFactory, zeroOnTruncate bool) (*segLog, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &segLog{
		dir: dir, name: name, segSize: segSize, staging: staging,
		newPager: f, zeroOnTruncate: zeroOnTruncate,
		scratch: make([]byte, staging),
		first:   1,
	}
	if err := s.recover(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *segLog) segPath(base uint64) string {
	return filepath.Join(s.dir, fmt.Sprintf("%015d.seg", base))
}

func (s *segLog) openSegment(base uint64, fresh bool) (*segment, error) {
	path := s.segPath(base)
	pg, err := s.newPager(path, s.segSize, s.staging)
	if err != nil {
		return nil, err
	}
	if fresh {
		// Reserve the first block as the segment header so entry data always
		// starts on a block boundary (§3.2).
		hdr := make([]byte, segHeaderSize)
		copy(hdr, []byte("PLXS"))
		if err := pg.Stage(hdr); err != nil {
			return nil, err
		}
		if err := pg.Flush(); err != nil {
			return nil, err
		}
	}
	return &segment{baseIndex: base, path: path, pg: pg}, nil
}

func (s *segLog) active() *segment { return s.segs[len(s.segs)-1] }

func (s *segLog) Append(entries []Entry) error {
	if len(s.segs) == 0 {
		sg, err := s.openSegment(s.last+1, true)
		if err != nil {
			return err
		}
		s.segs = append(s.segs, sg)
	}
	for _, e := range entries {
		encLen := HeaderSize + len(e.Payload)
		act := s.active()

		// An entry never spans two segments (§3.1.6).
		if act.pg.Offset()+int64(encLen) > s.segSize {
			// Seal the segment but keep its handle open: sealed segments are
			// still read by lagging followers and by verification.
			if err := act.pg.Flush(); err != nil {
				return err
			}
			sg, err := s.openSegment(e.Index, true)
			if err != nil {
				return err
			}
			s.segs = append(s.segs, sg)
			act = sg
		}

		off := act.pg.Offset()
		n := EncodeEntry(e, s.scratch[:encLen])
		if err := act.pg.Stage(s.scratch[:n]); err != nil {
			return err
		}
		s.index = append(s.index, loc{
			seg: int32(len(s.segs) - 1), off: off, length: int32(n), term: e.Term,
		})
		s.last = e.Index
	}
	// One flush per batch: this is the group commit (§4.3.3).
	return s.active().pg.Flush()
}

func (s *segLog) Get(index uint64) (Entry, error) {
	if index < s.first || index > s.last {
		return Entry{}, fmt.Errorf("index %d out of range [%d,%d]", index, s.first, s.last)
	}
	l := s.index[index-s.first]
	sg := s.segs[l.seg]
	if sg.pg == nil {
		return Entry{}, fmt.Errorf("segment %d closed", l.seg)
	}
	buf := make([]byte, l.length)
	if err := sg.pg.ReadAt(buf, l.off); err != nil {
		return Entry{}, err
	}
	e, _, err := DecodeEntry(buf, 1<<24)
	return e, err
}

func (s *segLog) TruncateTail(lastIndex uint64) error {
	if lastIndex >= s.last {
		return nil
	}
	// Byte offset where the first dropped entry began.
	drop := s.index[lastIndex+1-s.first]
	target := s.segs[drop.seg]

	// Unlink segments entirely above the truncation point (§6.1.2).
	for i := len(s.segs) - 1; i > int(drop.seg); i-- {
		if s.segs[i].pg != nil {
			s.retired.add(s.segs[i].pg.Stats())
			s.segs[i].pg.Close()
		}
		os.Remove(s.segs[i].path)
		s.segs = s.segs[:i]
	}
	if target.pg == nil {
		pg, err := s.newPager(target.path, s.segSize, s.staging)
		if err != nil {
			return err
		}
		target.pg = pg
	}

	if s.zeroOnTruncate {
		if err := target.pg.ZeroTo(drop.off); err != nil {
			return err
		}
	} else if err := target.pg.SeekTo(drop.off); err != nil {
		return err
	}

	s.index = s.index[:lastIndex+1-s.first]
	s.last = lastIndex
	return nil
}

// recover rebuilds the in-memory index by scanning segments, exactly as §7
// requires: no sidecar index file is consulted.
func (s *segLog) recover() error {
	matches, err := filepath.Glob(filepath.Join(s.dir, "*.seg"))
	if err != nil {
		return err
	}
	if len(matches) == 0 {
		s.first, s.last = 1, 0
		return nil
	}
	sort.Strings(matches)

	var bases []uint64
	for _, m := range matches {
		var b uint64
		if _, err := fmt.Sscanf(filepath.Base(m), "%015d.seg", &b); err != nil {
			return fmt.Errorf("bad segment name %q: %w", m, err)
		}
		bases = append(bases, b)
	}

	s.first = bases[0]
	expected := s.first
	for i, b := range bases {
		// R7.1.2: a gap between segments is unrecoverable.
		if b != expected {
			return fmt.Errorf("segment gap: expected base %d, found %d", expected, b)
		}
		sg, err := s.openSegment(b, false)
		if err != nil {
			return err
		}
		s.segs = append(s.segs, sg)
		n, err := s.scanSegment(int32(i), sg)
		if err != nil {
			return err
		}
		expected = b + n
	}
	s.last = expected - 1
	return nil
}

func (s *segLog) scanSegment(idx int32, sg *segment) (uint64, error) {
	const chunk = 1 << 20
	buf := make([]byte, chunk)
	off := int64(segHeaderSize)
	validEnd := off
	var count uint64
	expectedIdx := sg.baseIndex
	var prevTerm uint64

scan:
	for off < s.segSize {
		n := int64(chunk)
		if off+n > s.segSize {
			n = s.segSize - off
		}
		if err := sg.pg.ReadAt(buf[:n], off); err != nil {
			break
		}
		consumed := int64(0)
		for {
			rem := n - consumed
			if rem < HeaderSize {
				break // refill; an entry header straddles the chunk boundary
			}
			size := int64(HeaderSize) + int64(peekPayloadLen(buf[consumed:]))
			if size > chunk {
				break scan // implausible length: corrupt, log ends here
			}
			if size > rem {
				break // entry straddles the chunk boundary; refill and retry
			}
			e, got, err := DecodeEntry(buf[consumed:consumed+size], 1<<24)
			if err != nil {
				// Zero padding or corruption: the live log ends here (§7.2.1).
				break scan
			}
			// The recovery guard that makes logical truncation safe (§7.3).
			if e.Index != expectedIdx || e.Term < prevTerm {
				break scan
			}
			s.index = append(s.index, loc{
				seg: idx, off: off + consumed, length: int32(got), term: e.Term,
			})
			prevTerm = e.Term
			expectedIdx++
			count++
			consumed += int64(got)
			validEnd = off + consumed
		}
		if consumed == 0 {
			break // no progress possible
		}
		off += consumed
	}
	// Leave the pager positioned at the end of valid data so appends resume
	// at the right offset (R7.2.2).
	if err := sg.pg.SeekTo(validEnd); err != nil {
		return count, err
	}
	return count, nil
}

// peekPayloadLen reads the length field without validating anything, so the
// scanner can tell "entry straddles the read chunk" apart from "corrupt".
func peekPayloadLen(b []byte) uint32 {
	return uint32(b[18])<<24 | uint32(b[19])<<16 | uint32(b[20])<<8 | uint32(b[21])
}

func (s *segLog) FirstIndex() uint64 { return s.first }
func (s *segLog) LastIndex() uint64  { return s.last }

func (s *segLog) Stats() pagerStats {
	total := s.retired
	for _, sg := range s.segs {
		if sg.pg != nil {
			total.add(sg.pg.Stats())
		}
	}
	return total
}

func (s *segLog) Close() error {
	for _, sg := range s.segs {
		if sg.pg != nil {
			s.retired.add(sg.pg.Stats())
			if err := sg.pg.Close(); err != nil {
				return err
			}
			sg.pg = nil
		}
	}
	return nil
}
