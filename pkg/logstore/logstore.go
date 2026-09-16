package logstore

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/hashicorp/raft"
)

// Config configures the SegmentLogStore engine.
type Config struct {
	DataDir      string
	SegmentSize  int64
	DirectIO     bool
	MaxPayload   int
	SyncOnAppend bool
}

// DefaultConfig returns standard defaults for log storage.
func DefaultConfig(dataDir string) Config {
	return Config{
		DataDir:      dataDir,
		SegmentSize:  DefaultSegmentSize,
		DirectIO:     false, // default to false for cross-platform portability
		MaxPayload:   DefaultMaxPayloadSize,
		SyncOnAppend: true,
	}
}

type indexEntry struct {
	baseIndex uint64
	offset    int64
	size      int
	term      uint64
	logType   raft.LogType
}

// SegmentLogStore implements raft.LogStore and raft.StableStore.
type SegmentLogStore struct {
	mu          sync.RWMutex
	cfg         Config
	metaMu      sync.RWMutex
	metadata    map[string][]byte
	metaFile    string
	firstIndex  uint64
	lastIndex   uint64
	indexMap    map[uint64]indexEntry
	segments    []uint64
	activeFile  *os.File
	activeBase  uint64
	activeSize  int64
}

// NewSegmentLogStore initializes and recovers a log store from the given data directory.
func NewSegmentLogStore(cfg Config) (*SegmentLogStore, error) {
	if cfg.DataDir == "" {
		return nil, errors.New("DataDir is required")
	}
	if cfg.SegmentSize <= 0 {
		cfg.SegmentSize = DefaultSegmentSize
	}
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	store := &SegmentLogStore{
		cfg:      cfg,
		metadata: make(map[string][]byte),
		metaFile: filepath.Join(cfg.DataDir, "meta.json"),
		indexMap: make(map[uint64]indexEntry),
	}

	if err := store.loadMetadata(); err != nil {
		return nil, err
	}

	if err := store.recoverSegments(); err != nil {
		return nil, err
	}

	return store, nil
}

func (s *SegmentLogStore) segmentPath(baseIndex uint64) string {
	return filepath.Join(s.cfg.DataDir, fmt.Sprintf("%015d.seg", baseIndex))
}

func (s *SegmentLogStore) loadMetadata() error {
	data, err := os.ReadFile(s.metaFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read meta file: %w", err)
	}
	if len(data) == 0 {
		return nil
	}
	return json.Unmarshal(data, &s.metadata)
}

func (s *SegmentLogStore) saveMetadataLocked() error {
	b, err := json.Marshal(s.metadata)
	if err != nil {
		return err
	}
	tmp := s.metaFile + ".tmp"
	if err := os.WriteFile(tmp, b, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, s.metaFile)
}

func (s *SegmentLogStore) recoverSegments() error {
	entries, err := os.ReadDir(s.cfg.DataDir)
	if err != nil {
		return fmt.Errorf("read data dir: %w", err)
	}

	var bases []uint64
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".seg") {
			baseStr := strings.TrimSuffix(e.Name(), ".seg")
			base, err := strconv.ParseUint(baseStr, 10, 64)
			if err == nil {
				bases = append(bases, base)
			}
		}
	}
	sort.Slice(bases, func(i, j int) bool { return bases[i] < bases[j] })
	s.segments = bases

	if len(bases) == 0 {
		return nil
	}

	for _, base := range bases {
		path := s.segmentPath(base)
		f, err := os.OpenFile(path, os.O_RDWR, 0644)
		if err != nil {
			return fmt.Errorf("open segment %s: %w", path, err)
		}

		hdrBuf := make([]byte, BlockSize)
		n, err := io.ReadFull(f, hdrBuf)
		if err != nil || n < BlockSize {
			_ = f.Close()
			return fmt.Errorf("segment %s header read failed: %w", path, err)
		}
		if _, err := ValidateSegmentHeader(hdrBuf); err != nil {
			_ = f.Close()
			return fmt.Errorf("segment %s validation failed: %w", path, err)
		}

		offset := int64(BlockSize)
		fi, _ := f.Stat()
		fileSize := fi.Size()

		for offset+EntryHeaderSize <= fileSize {
			hdrBytes := make([]byte, EntryHeaderSize)
			if _, err := f.ReadAt(hdrBytes, offset); err != nil {
				break
			}
			eh, err := DecodeEntryHeader(hdrBytes)
			if err != nil {
				break
			}
			fullSize := EntryHeaderSize + int(eh.Size)
			if offset+int64(fullSize) > fileSize {
				break // torn write
			}
			fullEntry := make([]byte, fullSize)
			if _, err := f.ReadAt(fullEntry, offset); err != nil {
				break
			}
			if err := ValidateEntryCRC(fullEntry); err != nil {
				break // torn write or bit rot
			}

			// Read log type from payload header if present
			var logType raft.LogType = raft.LogCommand
			if len(fullEntry) > EntryHeaderSize {
				logType = raft.LogType(fullEntry[EntryHeaderSize])
			}

			s.indexMap[eh.Index] = indexEntry{
				baseIndex: base,
				offset:    offset,
				size:      fullSize,
				term:      eh.Term,
				logType:   logType,
			}

			if s.firstIndex == 0 || eh.Index < s.firstIndex {
				s.firstIndex = eh.Index
			}
			if eh.Index > s.lastIndex {
				s.lastIndex = eh.Index
			}

			offset += int64(fullSize)
		}

		_ = f.Close()
	}

	// Open the latest segment for active append
	latestBase := bases[len(bases)-1]
	latestPath := s.segmentPath(latestBase)
	f, err := os.OpenFile(latestPath, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open active segment %s: %w", latestPath, err)
	}
	fi, _ := f.Stat()
	s.activeFile = f
	s.activeBase = latestBase
	s.activeSize = fi.Size()

	return nil
}

func (s *SegmentLogStore) openNewSegmentLocked(baseIndex uint64) error {
	if s.activeFile != nil {
		if s.cfg.SyncOnAppend {
			_ = s.activeFile.Sync()
		}
		_ = s.activeFile.Close()
	}

	path := s.segmentPath(baseIndex)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("create segment %s: %w", path, err)
	}

	hdr := EncodeSegmentHeader(baseIndex)
	if _, err := f.Write(hdr); err != nil {
		_ = f.Close()
		return fmt.Errorf("write segment header %s: %w", path, err)
	}
	if s.cfg.SyncOnAppend {
		_ = f.Sync()
	}

	s.activeFile = f
	s.activeBase = baseIndex
	s.activeSize = int64(len(hdr))
	s.segments = append(s.segments, baseIndex)

	return nil
}

// FirstIndex returns the first log index available.
func (s *SegmentLogStore) FirstIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.firstIndex, nil
}

// LastIndex returns the last log index committed.
func (s *SegmentLogStore) LastIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastIndex, nil
}

// GetLog retrieves a log entry by index.
func (s *SegmentLogStore) GetLog(index uint64, log *raft.Log) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	loc, ok := s.indexMap[index]
	if !ok {
		return raft.ErrLogNotFound
	}

	path := s.segmentPath(loc.baseIndex)
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open segment file: %w", err)
	}
	defer f.Close()

	buf := make([]byte, loc.size)
	if _, err := f.ReadAt(buf, loc.offset); err != nil {
		return fmt.Errorf("read entry at offset %d: %w", loc.offset, err)
	}

	if err := ValidateEntryCRC(buf); err != nil {
		return err
	}

	eh, err := DecodeEntryHeader(buf[:EntryHeaderSize])
	if err != nil {
		return err
	}

	payload := buf[EntryHeaderSize:]
	if len(payload) == 0 {
		return raft.ErrLogNotFound
	}

	log.Index = eh.Index
	log.Term = eh.Term
	log.Type = raft.LogType(payload[0])
	log.Data = append([]byte(nil), payload[1:]...)

	return nil
}

// StoreLog appends a single log entry.
func (s *SegmentLogStore) StoreLog(log *raft.Log) error {
	return s.StoreLogs([]*raft.Log{log})
}

// StoreLogs appends a slice of log entries atomically.
func (s *SegmentLogStore) StoreLogs(logs []*raft.Log) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, l := range logs {
		// Payload framing: [1 byte LogType][Data]
		payload := make([]byte, 1+len(l.Data))
		payload[0] = byte(l.Type)
		copy(payload[1:], l.Data)

		encoded, err := EncodeEntry(l.Term, l.Index, payload)
		if err != nil {
			return err
		}

		entryLen := int64(len(encoded))
		if s.activeFile == nil || (s.activeSize+entryLen > s.cfg.SegmentSize) {
			if err := s.openNewSegmentLocked(l.Index); err != nil {
				return err
			}
		}

		currentOffset := s.activeSize
		if _, err := s.activeFile.Write(encoded); err != nil {
			return fmt.Errorf("write log entry: %w", err)
		}
		s.activeSize += entryLen

		s.indexMap[l.Index] = indexEntry{
			baseIndex: s.activeBase,
			offset:    currentOffset,
			size:      len(encoded),
			term:      l.Term,
			logType:   l.Type,
		}

		if s.firstIndex == 0 || l.Index < s.firstIndex {
			s.firstIndex = l.Index
		}
		if l.Index > s.lastIndex {
			s.lastIndex = l.Index
		}
	}

	if s.cfg.SyncOnAppend && s.activeFile != nil {
		if err := s.activeFile.Sync(); err != nil {
			return fmt.Errorf("sync active segment: %w", err)
		}
	}

	return nil
}

// DeleteRange removes log entries between min and max inclusive.
func (s *SegmentLogStore) DeleteRange(min, max uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Head compaction: min <= firstIndex
	if min <= s.firstIndex {
		for i := min; i <= max; i++ {
			delete(s.indexMap, i)
		}
		s.firstIndex = max + 1
		if s.firstIndex > s.lastIndex {
			s.firstIndex = s.lastIndex
		}
		s.compactOldSegmentsLocked()
		return nil
	}

	// Tail truncation: max >= lastIndex
	if max >= s.lastIndex {
		for i := min; i <= max; i++ {
			delete(s.indexMap, i)
		}
		if min > 0 {
			s.lastIndex = min - 1
		} else {
			s.lastIndex = 0
		}
		return nil
	}

	// Mid-range delete
	for i := min; i <= max; i++ {
		delete(s.indexMap, i)
	}
	return nil
}

func (s *SegmentLogStore) compactOldSegmentsLocked() {
	if len(s.segments) <= 1 {
		return
	}
	var remaining []uint64
	for i, base := range s.segments {
		// If this is the active segment, always keep it
		if i == len(s.segments)-1 {
			remaining = append(remaining, base)
			continue
		}
		// Check next segment base index
		nextBase := s.segments[i+1]
		if nextBase <= s.firstIndex {
			// This segment only contains indexes < firstIndex; unlink it
			_ = os.Remove(s.segmentPath(base))
		} else {
			remaining = append(remaining, base)
		}
	}
	s.segments = remaining
}

// Set sets a key-value pair in stable storage.
func (s *SegmentLogStore) Set(key []byte, val []byte) error {
	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	s.metadata[string(key)] = append([]byte(nil), val...)
	return s.saveMetadataLocked()
}

// Get retrieves a key-value pair from stable storage.
func (s *SegmentLogStore) Get(key []byte) ([]byte, error) {
	s.metaMu.RLock()
	defer s.metaMu.RUnlock()
	val, ok := s.metadata[string(key)]
	if !ok {
		return nil, errors.New("not found")
	}
	return append([]byte(nil), val...), nil
}

// SetUint64 sets an unsigned 64-bit integer in stable storage.
func (s *SegmentLogStore) SetUint64(key []byte, val uint64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, val)
	return s.Set(key, buf)
}

// GetUint64 retrieves an unsigned 64-bit integer from stable storage.
func (s *SegmentLogStore) GetUint64(key []byte) (uint64, error) {
	val, err := s.Get(key)
	if err != nil {
		return 0, err
	}
	if len(val) < 8 {
		return 0, errors.New("invalid uint64 buffer size")
	}
	return binary.BigEndian.Uint64(val), nil
}

// Close closes all open segment files.
func (s *SegmentLogStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activeFile != nil {
		_ = s.activeFile.Sync()
		err := s.activeFile.Close()
		s.activeFile = nil
		return err
	}
	return nil
}
