package cluster

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/avivklas/plexus/pkg/stores/kv"
	"github.com/hashicorp/raft"
)

func TestClusterUseStateAndKVStore(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "plexus-cluster-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := DefaultConfig("node-1", "127.0.0.1:9091", tmpDir)
	cfg.Bootstrap = true
	cfg.ApplyTimeout = 5 * time.Second

	c, err := New(cfg)
	if err != nil {
		t.Fatalf("New cluster failed: %v", err)
	}

	// Override transport and stores with inmem for fast unit testing
	_, trans := raft.NewInmemTransport(raft.ServerAddress("127.0.0.1:9091"))
	c.DefaultRaftMachine().WithCustomStores(
		raft.NewInmemStore(),
		raft.NewInmemStore(),
		raft.NewInmemSnapshotStore(),
		trans,
	)

	// 1. Create KV store
	kvStore := kv.New()

	// 2. Register into cluster with UseState (super easy, single line!)
	mutator := c.UseState(kvStore)
	kvStore.AttachMutator(mutator)

	// 3. Start cluster
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Cluster Start failed: %v", err)
	}
	defer c.Stop()

	// Wait for leader
	deadline := time.Now().Add(5 * time.Second)
	for !c.defaultMach.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for leader")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// 4. Mutate via Set (quorum + local apply)
	if err := kvStore.Set(context.Background(), "framework", "plexus"); err != nil {
		t.Fatalf("kv.Set failed: %v", err)
	}

	// 5. Read immediately from local memory with guaranteed consistency!
	val, ok := kvStore.Get("framework")
	if !ok || val != "plexus" {
		t.Fatalf("expected key 'framework' to be 'plexus', got ok=%v val=%s", ok, val)
	}

	// 6. Delete
	if err := kvStore.Delete(context.Background(), "framework"); err != nil {
		t.Fatalf("kv.Delete failed: %v", err)
	}

	_, ok = kvStore.Get("framework")
	if ok {
		t.Fatalf("expected key 'framework' to be deleted")
	}
}
