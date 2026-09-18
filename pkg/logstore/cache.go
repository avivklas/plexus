package logstore

import (
	"sync"

	"github.com/hashicorp/raft"
)

const (
	// DefaultLogCacheSize is the default capacity for the in-memory ring buffer log cache.
	DefaultLogCacheSize = 16384
)

// logRingCache provides a concurrent-safe, fixed-capacity ring buffer cache
// for the most recent Raft log entries.
type logRingCache struct {
	mu       sync.RWMutex
	entries  []*raft.Log
	capacity uint64
	mask     uint64
}

// nextPowerOf2 returns the next power of 2 greater than or equal to n.
func nextPowerOf2(n int) uint64 {
	if n <= 1 {
		return 1
	}
	v := uint64(n - 1)
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v |= v >> 32
	return v + 1
}

// newLogRingCache creates a ring buffer cache rounded up to the nearest power of 2.
func newLogRingCache(size int) *logRingCache {
	if size <= 0 {
		size = DefaultLogCacheSize
	}
	cap := nextPowerOf2(size)
	return &logRingCache{
		entries:  make([]*raft.Log, cap),
		capacity: cap,
		mask:     cap - 1,
	}
}

// get checks if the entry for index is in the cache. If present, it copies
// all fields into dst and returns true; otherwise it returns false.
func (c *logRingCache) get(index uint64, dst *raft.Log) bool {
	if c == nil || c.capacity == 0 || dst == nil {
		return false
	}
	slot := index & c.mask

	c.mu.RLock()
	item := c.entries[slot]
	if item == nil || item.Index != index {
		c.mu.RUnlock()
		return false
	}

	dst.Index = item.Index
	dst.Term = item.Term
	dst.Type = item.Type
	if len(item.Data) > 0 {
		dst.Data = append([]byte(nil), item.Data...)
	} else {
		dst.Data = nil
	}
	if len(item.Extensions) > 0 {
		dst.Extensions = append([]byte(nil), item.Extensions...)
	} else {
		dst.Extensions = nil
	}
	dst.AppendedAt = item.AppendedAt
	c.mu.RUnlock()
	return true
}

// put inserts a single log entry into the ring buffer cache.
func (c *logRingCache) put(l *raft.Log) {
	if c == nil || c.capacity == 0 || l == nil {
		return
	}
	var dataCopy []byte
	if len(l.Data) > 0 {
		dataCopy = append([]byte(nil), l.Data...)
	}
	var extCopy []byte
	if len(l.Extensions) > 0 {
		extCopy = append([]byte(nil), l.Extensions...)
	}
	cloned := &raft.Log{
		Index:      l.Index,
		Term:       l.Term,
		Type:       l.Type,
		Data:       dataCopy,
		Extensions: extCopy,
		AppendedAt: l.AppendedAt,
	}

	slot := l.Index & c.mask
	c.mu.Lock()
	c.entries[slot] = cloned
	c.mu.Unlock()
}

// putBatch inserts multiple log entries under a single lock acquisition.
func (c *logRingCache) putBatch(logs []*raft.Log) {
	if c == nil || c.capacity == 0 || len(logs) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, l := range logs {
		if l == nil {
			continue
		}
		slot := l.Index & c.mask
		var dataCopy []byte
		if len(l.Data) > 0 {
			dataCopy = append([]byte(nil), l.Data...)
		}
		var extCopy []byte
		if len(l.Extensions) > 0 {
			extCopy = append([]byte(nil), l.Extensions...)
		}
		c.entries[slot] = &raft.Log{
			Index:      l.Index,
			Term:       l.Term,
			Type:       l.Type,
			Data:       dataCopy,
			Extensions: extCopy,
			AppendedAt: l.AppendedAt,
		}
	}
}

// deleteRange invalidates all cached entries with indices between min and max inclusive.
func (c *logRingCache) deleteRange(min, max uint64) {
	if c == nil || c.capacity == 0 || min > max {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if max-min >= c.capacity-1 {
		for i := range c.entries {
			c.entries[i] = nil
		}
		return
	}

	for i := min; i <= max; i++ {
		slot := i & c.mask
		if item := c.entries[slot]; item != nil && item.Index == i {
			c.entries[slot] = nil
		}
	}
}

// clear invalidates all entries in the cache.
func (c *logRingCache) clear() {
	if c == nil || c.capacity == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.entries {
		c.entries[i] = nil
	}
}
