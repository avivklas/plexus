package machine

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/avivklas/plexus/pkg/store"
	"github.com/hashicorp/raft"
)

// mockKVStore is a test store implementing store.Store.
type mockKVStore struct {
	store.BaseStore
	mu   sync.RWMutex
	data map[string]string
}

func newMockKVStore() *mockKVStore {
	m := &mockKVStore{
		BaseStore: store.NewBaseStore(),
		data:      make(map[string]string),
	}

	store.HandleTyped(m.Router(), "kv.set", func(ctx context.Context, req struct{ Key, Val string }) (string, error) {
		m.mu.Lock()
		m.data[req.Key] = req.Val
		m.mu.Unlock()
		return "OK", nil
	})

	return m
}

func (m *mockKVStore) ID() store.StoreID {
	return "mock-kv"
}

func (m *mockKVStore) Get(key string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.data[key]
}

func (m *mockKVStore) Snapshot() ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	cmd, err := store.NewCommand("snap", m.data)
	if err != nil {
		return nil, err
	}
	return cmd.Marshal()
}

func (m *mockKVStore) Restore(b []byte) error {
	cmd, err := store.UnmarshalCommand(b)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = make(map[string]string)
	return cmd.Decode(&m.data)
}

func setupTestRaftMachine(t *testing.T, id string) (*RaftMachine, *mockKVStore) {
	tmpDir, err := os.MkdirTemp("", "plexus-machine-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(tmpDir) })

	_, trans := raft.NewInmemTransport(raft.ServerAddress(id))

	cfg := DefaultConfig(MachineID(id), &Node{ID: id, Address: id, Voter: true}, tmpDir)
	cfg.Bootstrap = true
	cfg.Transport = trans
	cfg.SnapshotStore = raft.NewInmemSnapshotStore()
	cfg.LogStore = raft.NewInmemStore()
	cfg.StableStore = raft.NewInmemStore()
	cfg.ApplyTimeout = 5 * time.Second

	m := NewRaftMachine(cfg)
	kv := newMockKVStore()
	m.Register(kv)

	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	t.Cleanup(func() { _ = m.Stop() })

	// Wait for leadership
	deadline := time.Now().Add(5 * time.Second)
	for !m.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for machine %s to become leader", id)
		}
		time.Sleep(20 * time.Millisecond)
	}

	return m, kv
}

func TestRaftMachineApplyAndLocalRead(t *testing.T) {
	m, kv := setupTestRaftMachine(t, "node-single")

	// Propose mutation
	res, err := m.Apply(context.Background(), "kv.set", struct{ Key, Val string }{
		Key: "greeting",
		Val: "hello plexus",
	})
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	if res != "OK" {
		t.Fatalf("expected result 'OK', got %v", res)
	}

	// Broken CAP / Quorum + Local Apply assertion:
	// Because Apply() only returns after local FSM has applied the committed index,
	// the value MUST immediately be readable from local state without any read barrier!
	if !m.ShouldReadLocally() {
		t.Fatalf("expected ShouldReadLocally to be true")
	}

	val := kv.Get("greeting")
	if val != "hello plexus" {
		t.Fatalf("expected 'hello plexus', got '%s'", val)
	}

	if m.LastCommittedIndex() == 0 {
		t.Fatalf("expected non-zero committed index")
	}
}

func TestRaftMachineSnapshot(t *testing.T) {
	m, kv := setupTestRaftMachine(t, "node-snap")

	_, err := m.Apply(context.Background(), "kv.set", struct{ Key, Val string }{Key: "user", Val: "Alice"})
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	if err := m.TakeSnapshot(); err != nil {
		t.Fatalf("TakeSnapshot failed: %v", err)
	}

	if kv.Get("user") != "Alice" {
		t.Fatalf("expected user=Alice")
	}
}

func TestBatchingFSMApplyBatch(t *testing.T) {
	fsm := NewMultiStoreFSM()
	kv := newMockKVStore()
	fsm.RegisterStore(kv)

	// Verify BatchingFSM interface satisfaction
	var batching raft.BatchingFSM = fsm

	// Prepare a batch of commands
	cmd1, err := store.NewCommand("kv.set", struct{ Key, Val string }{Key: "k1", Val: "v1"})
	if err != nil {
		t.Fatalf("create cmd1: %v", err)
	}
	b1, err := cmd1.Marshal()
	if err != nil {
		t.Fatalf("marshal cmd1: %v", err)
	}

	cmd2, err := store.NewCommand("kv.set", struct{ Key, Val string }{Key: "k2", Val: "v2"})
	if err != nil {
		t.Fatalf("create cmd2: %v", err)
	}
	b2, err := cmd2.Marshal()
	if err != nil {
		t.Fatalf("marshal cmd2: %v", err)
	}

	logs := []*raft.Log{
		{Index: 1, Term: 1, Type: raft.LogCommand, Data: b1},
		{Index: 2, Term: 1, Type: raft.LogCommand, Data: b2},
	}

	res := batching.ApplyBatch(logs)
	if len(res) != 2 {
		t.Fatalf("expected 2 results, got %d", len(res))
	}

	for i, r := range res {
		applyRes, ok := r.(*ApplyResult)
		if !ok {
			t.Fatalf("result %d expected *ApplyResult, got %T", i, r)
		}
		if applyRes.Err != "" {
			t.Fatalf("result %d unexpected error: %s", i, applyRes.Err)
		}
		if applyRes.Res != "OK" {
			t.Fatalf("result %d expected 'OK', got %v", i, applyRes.Res)
		}
	}

	if kv.Get("k1") != "v1" || kv.Get("k2") != "v2" {
		t.Fatalf("batch values not applied to KV store: k1=%s, k2=%s", kv.Get("k1"), kv.Get("k2"))
	}

	if fsm.LastCommittedIndex() != 2 {
		t.Fatalf("expected LastCommittedIndex 2, got %d", fsm.LastCommittedIndex())
	}
}
