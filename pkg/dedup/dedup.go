package dedup

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// ErrDuplicate indicates that an operation with this idempotency key was already applied.
var ErrDuplicate = errors.New("duplicate command execution")

// Record stores metadata and result of an applied idempotent operation.
type Record struct {
	IdempotencyKey string    `json:"key"`
	UpstreamID     string    `json:"upstream_id"`
	Term           uint64    `json:"term"`
	Index          uint64    `json:"index"`
	AppliedAt      time.Time `json:"applied_at"`
	Result         []byte    `json:"result,omitempty"`
	Error          string    `json:"error,omitempty"`
}

// Store is the interface for recording and querying processed idempotency keys.
type Store interface {
	// Check returns the existing record if key has already been applied.
	Check(key string) (*Record, bool, error)

	// Record marks an idempotency key as applied with its result/error.
	Record(rec Record) error

	// Prune evicts records with Index older than minIndex for a given upstream.
	Prune(upstreamID string, minIndex uint64) error

	// Snapshot creates a serialized copy of all dedup records.
	Snapshot() ([]byte, error)

	// Restore resets the dedup records from snapshot data.
	Restore(data []byte) error

	// Close flushes and cleans up any open resources.
	Close() error
}

// MemoryStore is an in-memory, thread-safe implementation of Store.
type MemoryStore struct {
	mu      sync.RWMutex
	records map[string]Record
}

// NewMemoryStore creates a new in-memory dedup store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		records: make(map[string]Record),
	}
}

// Check implements Store.
func (m *MemoryStore) Check(key string) (*Record, bool, error) {
	if key == "" {
		return nil, false, nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	rec, ok := m.records[key]
	if !ok {
		return nil, false, nil
	}
	copyRec := rec
	return &copyRec, true, nil
}

// Record implements Store.
func (m *MemoryStore) Record(rec Record) error {
	if rec.IdempotencyKey == "" {
		return nil
	}
	if rec.AppliedAt.IsZero() {
		rec.AppliedAt = time.Now().UTC()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records[rec.IdempotencyKey] = rec
	return nil
}

// Prune implements Store.
func (m *MemoryStore) Prune(upstreamID string, minIndex uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, v := range m.records {
		if (upstreamID == "" || v.UpstreamID == upstreamID) && v.Index < minIndex {
			delete(m.records, k)
		}
	}
	return nil
}

// Snapshot implements Store.
func (m *MemoryStore) Snapshot() ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return json.Marshal(m.records)
}

// Restore implements Store.
func (m *MemoryStore) Restore(data []byte) error {
	var records map[string]Record
	if err := json.Unmarshal(data, &records); err != nil {
		return fmt.Errorf("unmarshal dedup records: %w", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records = records
	return nil
}

// Close implements Store.
func (m *MemoryStore) Close() error {
	return nil
}

// FileStore is an embedded persistent disk-backed dedup store.
type FileStore struct {
	mu       sync.RWMutex
	filePath string
	memory   *MemoryStore
}

// NewFileStore opens or creates a persistent dedup store at the specified file path.
func NewFileStore(path string) (*FileStore, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create dedup dir: %w", err)
	}

	fs := &FileStore{
		filePath: path,
		memory:   NewMemoryStore(),
	}

	data, err := os.ReadFile(path)
	if err == nil && len(data) > 0 {
		if err := fs.memory.Restore(data); err != nil {
			return nil, fmt.Errorf("load dedup file %s: %w", path, err)
		}
	} else if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("read dedup file: %w", err)
	}

	return fs, nil
}

// Check implements Store.
func (fs *FileStore) Check(key string) (*Record, bool, error) {
	return fs.memory.Check(key)
}

// Record implements Store.
func (fs *FileStore) Record(rec Record) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if err := fs.memory.Record(rec); err != nil {
		return err
	}
	return fs.flushLocked()
}

// Prune implements Store.
func (fs *FileStore) Prune(upstreamID string, minIndex uint64) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if err := fs.memory.Prune(upstreamID, minIndex); err != nil {
		return err
	}
	return fs.flushLocked()
}

// Snapshot implements Store.
func (fs *FileStore) Snapshot() ([]byte, error) {
	return fs.memory.Snapshot()
}

// Restore implements Store.
func (fs *FileStore) Restore(data []byte) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if err := fs.memory.Restore(data); err != nil {
		return err
	}
	return fs.flushLocked()
}

// Close implements Store.
func (fs *FileStore) Close() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.flushLocked()
}

func (fs *FileStore) flushLocked() error {
	b, err := fs.memory.Snapshot()
	if err != nil {
		return err
	}
	tmp := fs.filePath + ".tmp"
	if err := os.WriteFile(tmp, b, 0644); err != nil {
		return fmt.Errorf("write dedup tmp file: %w", err)
	}
	if err := os.Rename(tmp, fs.filePath); err != nil {
		return fmt.Errorf("rename dedup tmp file: %w", err)
	}
	return nil
}
