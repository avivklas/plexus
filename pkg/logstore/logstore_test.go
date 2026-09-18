package logstore

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/hashicorp/raft"
)

func TestSegmentHeaderValidation(t *testing.T) {
	buf := EncodeSegmentHeader(100)
	baseIndex, err := ValidateSegmentHeader(buf)
	if err != nil {
		t.Fatalf("ValidateSegmentHeader failed: %v", err)
	}
	if baseIndex != 100 {
		t.Fatalf("expected baseIndex 100, got %d", baseIndex)
	}

	// Corrupt magic
	corruptBuf := append([]byte(nil), buf...)
	corruptBuf[0] = 0x00
	_, err = ValidateSegmentHeader(corruptBuf)
	if err == nil {
		t.Errorf("expected error on corrupt magic")
	}

	// Corrupt CRC
	corruptBuf2 := append([]byte(nil), buf...)
	corruptBuf2[10] ^= 0xFF
	_, err = ValidateSegmentHeader(corruptBuf2)
	if err == nil {
		t.Errorf("expected error on corrupt CRC")
	}
}

func TestEntryCRCCorruption(t *testing.T) {
	payload := []byte("hello world raft entry")
	encoded, err := EncodeEntry(1, 42, payload)
	if err != nil {
		t.Fatalf("EncodeEntry failed: %v", err)
	}

	if err := ValidateEntryCRC(encoded); err != nil {
		t.Fatalf("ValidateEntryCRC failed: %v", err)
	}

	// Flip a bit in payload
	corrupt := append([]byte(nil), encoded...)
	corrupt[len(corrupt)-1] ^= 0x01
	if err := ValidateEntryCRC(corrupt); err == nil {
		t.Errorf("expected error on flipped bit")
	}
}

func TestSegmentLogStoreOperations(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "plexus-logstore-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := DefaultConfig(tmpDir)
	cfg.SegmentSize = 8192 // Small segment size to test rotations

	store, err := NewSegmentLogStore(cfg)
	if err != nil {
		t.Fatalf("NewSegmentLogStore failed: %v", err)
	}
	defer store.Close()

	// Store logs 1..10
	var logs []*raft.Log
	for i := uint64(1); i <= 10; i++ {
		logs = append(logs, &raft.Log{
			Index: i,
			Term:  1,
			Type:  raft.LogCommand,
			Data:  []byte("command-data"),
		})
	}

	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("StoreLogs failed: %v", err)
	}

	first, _ := store.FirstIndex()
	last, _ := store.LastIndex()
	if first != 1 || last != 10 {
		t.Fatalf("expected indexes 1..10, got %d..%d", first, last)
	}

	// Read log 5
	var l5 raft.Log
	if err := store.GetLog(5, &l5); err != nil {
		t.Fatalf("GetLog(5) failed: %v", err)
	}
	if l5.Index != 5 || l5.Term != 1 || !bytes.Equal(l5.Data, []byte("command-data")) {
		t.Fatalf("unexpected log entry: %+v", l5)
	}

	// StableStore test
	if err := store.SetUint64([]byte("current-term"), 3); err != nil {
		t.Fatalf("SetUint64 failed: %v", err)
	}
	term, err := store.GetUint64([]byte("current-term"))
	if err != nil || term != 3 {
		t.Fatalf("expected term 3, got %d (err: %v)", term, err)
	}

	// Close and reopen to verify recovery
	_ = store.Close()

	reopenedStore, err := NewSegmentLogStore(cfg)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer reopenedStore.Close()

	rFirst, _ := reopenedStore.FirstIndex()
	rLast, _ := reopenedStore.LastIndex()
	if rFirst != 1 || rLast != 10 {
		t.Fatalf("recovered indexes mismatch: %d..%d", rFirst, rLast)
	}

	var rL10 raft.Log
	if err := reopenedStore.GetLog(10, &rL10); err != nil {
		t.Fatalf("GetLog(10) after reopen failed: %v", err)
	}
	if rL10.Index != 10 {
		t.Fatalf("expected index 10, got %d", rL10.Index)
	}

	// Test Tail Truncation
	if err := reopenedStore.DeleteRange(8, 10); err != nil {
		t.Fatalf("DeleteRange(8, 10) failed: %v", err)
	}
	tLast, _ := reopenedStore.LastIndex()
	if tLast != 7 {
		t.Fatalf("expected last index 7 after truncation, got %d", tLast)
	}

	// Log 8 should now not be found
	var l8 raft.Log
	if err := reopenedStore.GetLog(8, &l8); err != raft.ErrLogNotFound {
		t.Fatalf("expected ErrLogNotFound for index 8, got %v", err)
	}
}

func TestSegmentLogStore_CacheAndOpenFileTable(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "plexus-cache-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := DefaultConfig(tmpDir)
	cfg.LogCacheSize = 64

	store, err := NewSegmentLogStore(cfg)
	if err != nil {
		t.Fatalf("NewSegmentLogStore failed: %v", err)
	}
	defer store.Close()

	// Store 50 entries
	var logs []*raft.Log
	for i := uint64(1); i <= 50; i++ {
		logs = append(logs, &raft.Log{
			Index: i,
			Term:  2,
			Type:  raft.LogCommand,
			Data:  []byte(fmt.Sprintf("entry-payload-%d", i)),
		})
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("StoreLogs failed: %v", err)
	}

	// 1. Verify in-memory cache hit for all 50 entries
	for i := uint64(1); i <= 50; i++ {
		var l raft.Log
		if err := store.GetLog(i, &l); err != nil {
			t.Fatalf("GetLog(%d) failed: %v", i, err)
		}
		if l.Index != i || l.Term != 2 || string(l.Data) != fmt.Sprintf("entry-payload-%d", i) {
			t.Fatalf("unexpected entry data for %d: %+v", i, l)
		}
	}

	// Check that openFiles table has not been populated yet because all reads were cache hits!
	store.fileMu.RLock()
	openCount := len(store.openFiles)
	store.fileMu.RUnlock()
	if openCount != 0 {
		t.Fatalf("expected 0 open files for cache hits, got %d", openCount)
	}

	// 2. Clear cache to force fallback to disk reads using openFiles cache
	store.cache.clear()

	for i := uint64(1); i <= 50; i++ {
		var l raft.Log
		if err := store.GetLog(i, &l); err != nil {
			t.Fatalf("fallback GetLog(%d) failed: %v", i, err)
		}
		if l.Index != i || l.Term != 2 || string(l.Data) != fmt.Sprintf("entry-payload-%d", i) {
			t.Fatalf("unexpected fallback entry data for %d: %+v", i, l)
		}
	}

	// Check that openFiles table now contains the segment file
	store.fileMu.RLock()
	openCount = len(store.openFiles)
	store.fileMu.RUnlock()
	if openCount != 1 {
		t.Fatalf("expected 1 open file handle in openFiles, got %d", openCount)
	}

	// 3. Verify Close() closes all open handles
	if err := store.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	store.fileMu.RLock()
	openCount = len(store.openFiles)
	store.fileMu.RUnlock()
	if openCount != 0 {
		t.Fatalf("expected 0 open files after Close, got %d", openCount)
	}
}

func TestSegmentLogStore_EvictionFallback(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "plexus-evict-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := DefaultConfig(tmpDir)
	cfg.LogCacheSize = 16 // Ring buffer size 16

	store, err := NewSegmentLogStore(cfg)
	if err != nil {
		t.Fatalf("NewSegmentLogStore failed: %v", err)
	}
	defer store.Close()

	// Write 50 entries
	var logs []*raft.Log
	for i := uint64(1); i <= 50; i++ {
		logs = append(logs, &raft.Log{
			Index: i,
			Term:  1,
			Type:  raft.LogCommand,
			Data:  []byte(fmt.Sprintf("log-%d", i)),
		})
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("StoreLogs failed: %v", err)
	}

	// Entries 1..34 have been evicted from ring buffer (capacity 16, latest are 35..50)
	// Reading them should fall back to disk and succeed
	for i := uint64(1); i <= 34; i++ {
		var l raft.Log
		if err := store.GetLog(i, &l); err != nil {
			t.Fatalf("GetLog(%d) evicted fallback failed: %v", i, err)
		}
		if l.Index != i || string(l.Data) != fmt.Sprintf("log-%d", i) {
			t.Fatalf("mismatch for evicted log %d: %+v", i, l)
		}
	}

	// Entries 35..50 should hit cache
	for i := uint64(35); i <= 50; i++ {
		var l raft.Log
		if err := store.GetLog(i, &l); err != nil {
			t.Fatalf("GetLog(%d) cache read failed: %v", i, err)
		}
		if l.Index != i || string(l.Data) != fmt.Sprintf("log-%d", i) {
			t.Fatalf("mismatch for cached log %d: %+v", i, l)
		}
	}
}

func BenchmarkGetLog_CacheHit(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "plexus-bench-cache-*")
	if err != nil {
		b.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := DefaultConfig(tmpDir)
	cfg.LogCacheSize = 16384

	store, err := NewSegmentLogStore(cfg)
	if err != nil {
		b.Fatalf("NewSegmentLogStore failed: %v", err)
	}
	defer store.Close()

	numLogs := 1000
	var logs []*raft.Log
	for i := 1; i <= numLogs; i++ {
		logs = append(logs, &raft.Log{
			Index: uint64(i),
			Term:  1,
			Type:  raft.LogCommand,
			Data:  []byte("sample benchmark payload data 1234567890"),
		})
	}
	if err := store.StoreLogs(logs); err != nil {
		b.Fatalf("StoreLogs failed: %v", err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var l raft.Log
		var i uint64 = 1
		for pb.Next() {
			idx := (i % uint64(numLogs)) + 1
			if err := store.GetLog(idx, &l); err != nil {
				b.Fatalf("GetLog failed: %v", err)
			}
			i++
		}
	})
}

func BenchmarkGetLog_DiskFallback(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "plexus-bench-disk-*")
	if err != nil {
		b.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := DefaultConfig(tmpDir)
	cfg.LogCacheSize = 16384

	store, err := NewSegmentLogStore(cfg)
	if err != nil {
		b.Fatalf("NewSegmentLogStore failed: %v", err)
	}
	defer store.Close()

	numLogs := 1000
	var logs []*raft.Log
	for i := 1; i <= numLogs; i++ {
		logs = append(logs, &raft.Log{
			Index: uint64(i),
			Term:  1,
			Type:  raft.LogCommand,
			Data:  []byte("sample benchmark payload data 1234567890"),
		})
	}
	if err := store.StoreLogs(logs); err != nil {
		b.Fatalf("StoreLogs failed: %v", err)
	}

	// Clear cache to force disk fallback using openFiles table
	store.cache.clear()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var l raft.Log
		var i uint64 = 1
		for pb.Next() {
			idx := (i % uint64(numLogs)) + 1
			if err := store.GetLog(idx, &l); err != nil {
				b.Fatalf("GetLog failed: %v", err)
			}
			i++
		}
	})
}
