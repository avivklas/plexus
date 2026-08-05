package main

// A pager owns the bytes of one segment file. It is the ONLY thing that differs
// between the direct-I/O arm and the buffered arm: identical framing, indexing,
// segment rotation and durability contract sit above it. Any performance delta
// between those two arms is therefore attributable to the paging mechanism.
type pager interface {
	// Stage appends p at the current logical end of the segment. It does not
	// guarantee durability.
	Stage(p []byte) error

	// Flush makes every staged byte durable on the storage device. When it
	// returns nil the data must survive power loss.
	Flush() error

	// ReadAt reads exactly len(p) bytes from logical offset off, going to the
	// device (or the staging buffer, if the bytes have not been flushed yet).
	ReadAt(p []byte, off int64) error

	// Offset is the logical end-of-data position within the segment.
	Offset() int64

	// SeekTo moves the logical write position back to off for a tail
	// truncation. It writes no data.
	SeekTo(off int64) error

	// ZeroTo overwrites [off, Offset()) with zeros, for the physical-truncation
	// arm that the spec argues against.
	ZeroTo(off int64) error

	Close() error
	Stats() pagerStats
}

type pagerStats struct {
	DeviceWrites  int64 // number of write syscalls issued
	DeviceBytes   int64 // bytes handed to those writes, padding included
	PaddingBytes  int64 // zero-fill written purely to satisfy block alignment
	PayloadBytes  int64 // bytes of framed entries staged by the caller
	SyncCalls     int64 // explicit fsync/fdatasync calls
	RewrittenBlks int64 // tail blocks written more than once
}

func (s *pagerStats) add(o pagerStats) {
	s.DeviceWrites += o.DeviceWrites
	s.DeviceBytes += o.DeviceBytes
	s.PaddingBytes += o.PaddingBytes
	s.PayloadBytes += o.PayloadBytes
	s.SyncCalls += o.SyncCalls
	s.RewrittenBlks += o.RewrittenBlks
}
