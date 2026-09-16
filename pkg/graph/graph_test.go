package graph

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/avivklas/plexus/pkg/dedup"
	"github.com/avivklas/plexus/pkg/machine"
	"github.com/avivklas/plexus/pkg/store"
	"github.com/hashicorp/raft"
)

type orderStore struct {
	store.BaseStore
	mu     sync.RWMutex
	orders map[string]int
}

func newOrderStore() *orderStore {
	os := &orderStore{
		BaseStore: store.NewBaseStore(),
		orders:    make(map[string]int),
	}
	store.HandleTyped(os.Router(), "order.create", func(ctx context.Context, req struct {
		ID     string `json:"id"`
		Amount int    `json:"amount"`
	}) (string, error) {
		os.mu.Lock()
		os.orders[req.ID] = req.Amount
		os.mu.Unlock()
		return "ORDER_CREATED", nil
	})
	return os
}

func (s *orderStore) ID() store.StoreID { return "orders" }
func (s *orderStore) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(s.orders)
}
func (s *orderStore) Restore(b []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Unmarshal(b, &s.orders)
}

type billingStore struct {
	store.BaseStore
	mu           sync.RWMutex
	invoiceCount int
	invoices     map[string]int
}

func newBillingStore() *billingStore {
	bs := &billingStore{
		BaseStore: store.NewBaseStore(),
		invoices:  make(map[string]int),
	}
	store.HandleTyped(bs.Router(), "billing.invoice", func(ctx context.Context, req struct {
		OrderID string `json:"order_id"`
		Amount  int    `json:"amount"`
	}) (string, error) {
		bs.mu.Lock()
		bs.invoiceCount++
		bs.invoices[req.OrderID] = req.Amount
		bs.mu.Unlock()
		return "INVOICE_CREATED", nil
	})
	return bs
}

func (s *billingStore) ID() store.StoreID { return "billing" }
func (s *billingStore) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(s.invoices)
}
func (s *billingStore) Restore(b []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Unmarshal(b, &s.invoices)
}

func setupTestMachine(t *testing.T, id string, s store.Store) *machine.RaftMachine {
	tmpDir, err := os.MkdirTemp("", "plexus-graph-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(tmpDir) })

	_, trans := raft.NewInmemTransport(raft.ServerAddress(id))
	cfg := machine.DefaultConfig(machine.MachineID(id), &machine.Node{ID: id, Address: id, Voter: true}, tmpDir)
	cfg.Bootstrap = true
	cfg.Transport = trans
	cfg.SnapshotStore = raft.NewInmemSnapshotStore()
	cfg.LogStore = raft.NewInmemStore()
	cfg.StableStore = raft.NewInmemStore()
	cfg.ApplyTimeout = 5 * time.Second

	m := machine.NewRaftMachine(cfg)
	m.Register(s)

	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("Start machine %s failed: %v", id, err)
	}
	t.Cleanup(func() { _ = m.Stop() })

	deadline := time.Now().Add(5 * time.Second)
	for !m.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for leadership %s", id)
		}
		time.Sleep(20 * time.Millisecond)
	}

	return m
}

func TestRaftGraphPipelineAndExactlyOnce(t *testing.T) {
	ordersSt := newOrderStore()
	billingSt := newBillingStore()

	ordersMachine := setupTestMachine(t, "orders-cluster", ordersSt)
	billingMachine := setupTestMachine(t, "billing-cluster", billingSt)

	// Create Dedup store for billing machine
	billingDedup := dedup.NewMemoryStore()

	// Build Graph
	g := New("ecommerce-topology")
	defer g.Close()

	g.AddMachine(ordersMachine, nil)
	g.AddMachine(billingMachine, billingDedup)
	g.EnableDedupOnStore(billingMachine, billingSt)

	// Define Pipeline: Order created -> Emit billing invoice
	g.Pipe(ordersMachine, billingMachine, func(ctx context.Context, cmd *store.Command, res any) ([]*store.Command, error) {
		var req struct {
			ID     string `json:"id"`
			Amount int    `json:"amount"`
		}
		if err := cmd.Decode(&req); err != nil {
			return nil, err
		}

		invCmd, err := store.NewCommand("billing.invoice", struct {
			OrderID string `json:"order_id"`
			Amount  int    `json:"amount"`
		}{
			OrderID: req.ID,
			Amount:  req.Amount,
		})
		if err != nil {
			return nil, err
		}

		return []*store.Command{invCmd}, nil
	}, "order.create")

	// Apply an order mutation on upstream
	_, err := ordersMachine.Apply(context.Background(), "order.create", struct {
		ID     string `json:"id"`
		Amount int    `json:"amount"`
	}{
		ID:     "order-101",
		Amount: 250,
	})
	if err != nil {
		t.Fatalf("upstream Apply failed: %v", err)
	}

	// Wait for downstream pipeline propagation
	deadline := time.Now().Add(3 * time.Second)
	for {
		billingSt.mu.RLock()
		amt, ok := billingSt.invoices["order-101"]
		billingSt.mu.RUnlock()
		if ok && amt == 250 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for downstream billing invoice propagation")
		}
		time.Sleep(20 * time.Millisecond)
	}

	billingSt.mu.RLock()
	if billingSt.invoiceCount != 1 {
		t.Fatalf("expected invoiceCount 1, got %d", billingSt.invoiceCount)
	}
	billingSt.mu.RUnlock()

	// --- Exactly-Once Semantics (EOS) Duplicate Test ---
	// Retrieve the exact key recorded by the downstream dedup store
	var recordedKey string
	billingDedupSnap, err := billingDedup.Snapshot()
	if err != nil {
		t.Fatalf("billingDedup.Snapshot failed: %v", err)
	}
	var records map[string]dedup.Record
	if err := json.Unmarshal(billingDedupSnap, &records); err != nil {
		t.Fatalf("unmarshal dedup records failed: %v", err)
	}
	for k := range records {
		recordedKey = k
		break
	}
	if recordedKey == "" {
		t.Fatalf("expected at least one idempotency key recorded in dedup store")
	}

	duplicateCmd, err := store.NewCommand("billing.invoice", struct {
		OrderID string `json:"order_id"`
		Amount  int    `json:"amount"`
	}{
		OrderID: "order-101",
		Amount:  250,
	})
	if err != nil {
		t.Fatalf("create duplicate cmd failed: %v", err)
	}
	duplicateCmd.SetIdempotencyKey(recordedKey)

	// Re-apply duplicate command 5 times directly to downstream!
	for i := 0; i < 5; i++ {
		res, err := billingMachine.ApplyCommand(context.Background(), duplicateCmd)
		if err != nil {
			t.Fatalf("ApplyCommand duplicate failed: %v", err)
		}
		if res != "INVOICE_CREATED" {
			t.Errorf("expected cached result 'INVOICE_CREATED', got %v", res)
		}
	}

	// Verify invoiceCount is STILL exactly 1 (mutation was NOT re-applied!)
	billingSt.mu.RLock()
	finalCount := billingSt.invoiceCount
	billingSt.mu.RUnlock()

	if finalCount != 1 {
		t.Fatalf("EOS VIOLATED! expected invoiceCount to remain 1, but got %d", finalCount)
	}
}
