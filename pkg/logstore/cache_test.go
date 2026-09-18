package logstore

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestLogRingCache_Basic(t *testing.T) {
	c := newLogRingCache(8)
	if c.capacity != 8 {
		t.Fatalf("expected capacity 8, got %d", c.capacity)
	}

	// Empty get
	var dst raft.Log
	if c.get(1, &dst) {
		t.Fatalf("expected get(1) on empty cache to return false")
	}

	// Insert entries 1..5
	for i := uint64(1); i <= 5; i++ {
		c.put(&raft.Log{
			Index:      i,
			Term:       1,
			Type:       raft.LogCommand,
			Data:       []byte(fmt.Sprintf("data-%d", i)),
			Extensions: []byte("ext"),
			AppendedAt: time.Now(),
		})
	}

	// Verify entries 1..5
	for i := uint64(1); i <= 5; i++ {
		var l raft.Log
		if !c.get(i, &l) {
			t.Fatalf("expected get(%d) to return true", i)
		}
		if l.Index != i || l.Term != 1 || !bytes.Equal(l.Data, []byte(fmt.Sprintf("data-%d", i))) {
			t.Fatalf("entry mismatch for %d: %+v", i, l)
		}
	}

	// Delete range 2..3
	c.deleteRange(2, 3)
	if c.get(2, &dst) {
		t.Fatalf("expected entry 2 to be deleted")
	}
	if c.get(3, &dst) {
		t.Fatalf("expected entry 3 to be deleted")
	}
	if !c.get(1, &dst) || !c.get(4, &dst) || !c.get(5, &dst) {
		t.Fatalf("expected entries 1, 4, 5 to remain")
	}

	// Clear
	c.clear()
	if c.get(1, &dst) || c.get(4, &dst) || c.get(5, &dst) {
		t.Fatalf("expected all entries to be cleared")
	}
}

func TestLogRingCache_WrapAround(t *testing.T) {
	// Capacity 4
	c := newLogRingCache(4)
	if c.capacity != 4 {
		t.Fatalf("expected capacity 4, got %d", c.capacity)
	}

	// Insert 1..6
	for i := uint64(1); i <= 6; i++ {
		c.put(&raft.Log{
			Index: i,
			Term:  1,
			Data:  []byte(fmt.Sprintf("data-%d", i)),
		})
	}

	var dst raft.Log
	// Entries 1 and 2 should have been overwritten by 5 and 6
	if c.get(1, &dst) {
		t.Fatalf("expected entry 1 to be evicted by wrap-around")
	}
	if c.get(2, &dst) {
		t.Fatalf("expected entry 2 to be evicted by wrap-around")
	}

	// Entries 3, 4, 5, 6 should still be in cache
	for i := uint64(3); i <= 6; i++ {
		if !c.get(i, &dst) {
			t.Fatalf("expected entry %d to be present", i)
		}
		if dst.Index != i {
			t.Fatalf("expected index %d, got %d", i, dst.Index)
		}
	}
}

func TestLogRingCache_Concurrent(t *testing.T) {
	c := newLogRingCache(128)
	var wg sync.WaitGroup
	workers := 10
	iterations := 1000

	// Writer goroutines
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 1; i <= iterations; i++ {
				idx := uint64(i + workerID*iterations)
				c.put(&raft.Log{
					Index: idx,
					Term:  1,
					Data:  []byte("concurrent-test"),
				})
			}
		}(w)
	}

	// Reader goroutines
	for r := 0; r < workers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var dst raft.Log
			for i := 1; i <= iterations; i++ {
				c.get(uint64(i), &dst)
			}
		}()
	}

	wg.Wait()
}
