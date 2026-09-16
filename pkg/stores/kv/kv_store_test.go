package kv

import (
	"context"
	"testing"

	"github.com/avivklas/plexus/pkg/store"
)

type mockMutator struct {
	store *Store
}

func (m *mockMutator) Apply(ctx context.Context, typ store.CommandType, data any) (any, error) {
	cmd, err := store.NewCommand(typ, data)
	if err != nil {
		return nil, err
	}
	return m.store.Router().Execute(ctx, cmd)
}

func (m *mockMutator) Initialized() bool { return true }
func (m *mockMutator) Do(flag store.StateFlag, fn func()) bool {
	fn()
	return true
}

func TestKVStoreDirect(t *testing.T) {
	s := New()
	m := &mockMutator{store: s}
	s.AttachMutator(m)

	if err := s.Set(context.Background(), "fruit", "apple"); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	val, ok := s.Get("fruit")
	if !ok || val != "apple" {
		t.Fatalf("expected apple, got ok=%v val=%s", ok, val)
	}

	keys := s.Keys()
	if len(keys) != 1 || keys[0] != "fruit" {
		t.Fatalf("unexpected keys: %v", keys)
	}

	snap, err := s.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}

	s2 := New()
	if err := s2.Restore(snap); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	val2, ok2 := s2.Get("fruit")
	if !ok2 || val2 != "apple" {
		t.Fatalf("restored store missing key")
	}

	if err := s.Delete(context.Background(), "fruit"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, ok = s.Get("fruit")
	if ok {
		t.Fatalf("expected key to be deleted")
	}
}
