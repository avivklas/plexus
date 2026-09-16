package dedup

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestMemoryStore(t *testing.T) {
	s := NewMemoryStore()

	key := "upstream-1:1:100:0"
	rec, found, err := s.Check(key)
	if err != nil || found || rec != nil {
		t.Fatalf("expected not found, got found=%v err=%v", found, err)
	}

	err = s.Record(Record{
		IdempotencyKey: key,
		UpstreamID:     "upstream-1",
		Term:           1,
		Index:          100,
		AppliedAt:      time.Now(),
		Result:         []byte(`{"status":"ok"}`),
	})
	if err != nil {
		t.Fatalf("Record failed: %v", err)
	}

	rec, found, err = s.Check(key)
	if err != nil || !found || rec == nil {
		t.Fatalf("expected found, got found=%v err=%v", found, err)
	}
	if string(rec.Result) != `{"status":"ok"}` {
		t.Errorf("unexpected result: %s", string(rec.Result))
	}

	// Prune older than index 150
	if err := s.Prune("upstream-1", 150); err != nil {
		t.Fatalf("Prune failed: %v", err)
	}

	_, found, _ = s.Check(key)
	if found {
		t.Errorf("expected key to be pruned")
	}
}

func TestMemoryStoreSnapshotRestore(t *testing.T) {
	s1 := NewMemoryStore()
	_ = s1.Record(Record{
		IdempotencyKey: "k1",
		UpstreamID:     "u1",
		Index:          10,
		Result:         []byte("res1"),
	})

	snap, err := s1.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}

	s2 := NewMemoryStore()
	if err := s2.Restore(snap); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	rec, found, err := s2.Check("k1")
	if err != nil || !found || string(rec.Result) != "res1" {
		t.Errorf("unexpected restored record: %+v", rec)
	}
}

func TestFileStorePersistence(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "plexus-dedup-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	filePath := filepath.Join(tmpDir, "dedup.json")
	fs, err := NewFileStore(filePath)
	if err != nil {
		t.Fatalf("NewFileStore failed: %v", err)
	}

	_ = fs.Record(Record{
		IdempotencyKey: "persist-key",
		UpstreamID:     "upstream-2",
		Index:          50,
		Result:         []byte("persisted"),
	})
	_ = fs.Close()

	// Reopen
	fs2, err := NewFileStore(filePath)
	if err != nil {
		t.Fatalf("Reopen NewFileStore failed: %v", err)
	}
	defer fs2.Close()

	rec, found, err := fs2.Check("persist-key")
	if err != nil || !found || string(rec.Result) != "persisted" {
		t.Errorf("persisted key not found or corrupted: %+v", rec)
	}
}
