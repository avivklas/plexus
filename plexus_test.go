package plexus_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/avivklas/plexus"
	"github.com/avivklas/plexus/pkg/stores/kv"
	"github.com/hashicorp/raft"
)

func TestPlexusFrameworkTopLevelAPI(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "plexus-root-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := plexus.DefaultClusterConfig("node-alpha", "127.0.0.1:9191", tmpDir)
	cfg.Bootstrap = true
	cfg.ApplyTimeout = 5 * time.Second

	c, err := plexus.NewCluster(cfg)
	if err != nil {
		t.Fatalf("NewCluster failed: %v", err)
	}

	_, trans := raft.NewInmemTransport(raft.ServerAddress("127.0.0.1:9191"))
	c.DefaultRaftMachine().WithCustomStores(
		raft.NewInmemStore(),
		raft.NewInmemStore(),
		raft.NewInmemSnapshotStore(),
		trans,
	)

	// 1. Instantiate KV state machine
	kvStore := kv.New()

	// 2. Register with cluster UseState
	mutator := c.UseState(kvStore)
	kvStore.AttachMutator(mutator)

	// 3. Start cluster
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Cluster Start failed: %v", err)
	}
	defer c.Stop()

	// Wait for leadership
	deadline := time.Now().Add(5 * time.Second)
	for !c.DefaultMachine().IsLeader() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for leader")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// 4. Propose mutation (replicated to quorum and applied to local FSM)
	if err := kvStore.Set(context.Background(), "hero", "antigravity"); err != nil {
		t.Fatalf("kv.Set failed: %v", err)
	}

	// 5. Read immediately from local memory with guaranteed consistency
	val, ok := kvStore.Get("hero")
	if !ok || val != "antigravity" {
		t.Fatalf("expected hero=antigravity, got ok=%v val=%s", ok, val)
	}

	// 6. Raft Graph topology check
	g := plexus.NewGraph("root-topology")
	defer g.Close()

	dedupStore := plexus.NewMemoryDedup()
	g.AddMachine(c.DefaultMachine(), dedupStore)

	if g.Name() != "root-topology" {
		t.Fatalf("unexpected graph name: %s", g.Name())
	}
}
