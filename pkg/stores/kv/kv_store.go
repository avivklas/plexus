package kv

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/avivklas/plexus/pkg/store"
)

const (
	CmdSet store.CommandType = "kv.set"
	CmdDel store.CommandType = "kv.del"
)

// SetPayload represents a key-value write operation.
type SetPayload struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// DelPayload represents a key deletion operation.
type DelPayload struct {
	Key string `json:"key"`
}

// Store is an in-memory, Raft-replicated key-value state machine.
type Store struct {
	store.BaseStore
	mu      sync.RWMutex
	data    map[string]string
	mutator store.Mutator
}

// New creates an unattached KV Store.
func New() *Store {
	s := &Store{
		BaseStore: store.NewBaseStore(),
		data:      make(map[string]string),
	}

	store.HandleTyped(s.Router(), CmdSet, func(ctx context.Context, p SetPayload) (string, error) {
		s.mu.Lock()
		s.data[p.Key] = p.Value
		s.mu.Unlock()
		return "OK", nil
	})

	store.HandleTyped(s.Router(), CmdDel, func(ctx context.Context, p DelPayload) (string, error) {
		s.mu.Lock()
		delete(s.data, p.Key)
		s.mu.Unlock()
		return "OK", nil
	})

	return s
}

// AttachMutator connects the Store to the cluster Mutator handle.
func (s *Store) AttachMutator(m store.Mutator) {
	s.mutator = m
}

// ID implements store.Store.
func (s *Store) ID() store.StoreID {
	return "kv"
}

// Set replicates a key-value write across the cluster.
func (s *Store) Set(ctx context.Context, key, val string) error {
	_, err := s.mutator.Apply(ctx, CmdSet, SetPayload{Key: key, Value: val})
	return err
}

// Delete replicates a key deletion across the cluster.
func (s *Store) Delete(ctx context.Context, key string) error {
	_, err := s.mutator.Apply(ctx, CmdDel, DelPayload{Key: key})
	return err
}

// Get reads directly from local memory.
// Because Plexus mutators only return after commitment to quorum AND local FSM apply,
// any previously acknowledged Set on this node is immediately visible here.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.data[key]
	return val, ok
}

// Keys returns all keys currently held in the local state machine.
func (s *Store) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	return keys
}

// Snapshot implements store.Store.
func (s *Store) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(s.data)
}

// Restore implements store.Store.
func (s *Store) Restore(b []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var fresh map[string]string
	if err := json.Unmarshal(b, &fresh); err != nil {
		return err
	}
	s.data = fresh
	return nil
}
