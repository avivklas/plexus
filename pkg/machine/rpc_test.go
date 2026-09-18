package machine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/avivklas/plexus/pkg/store"
)

type mockRPCHandler struct {
	joinCount  int64
	applyCount int64
	joinErr    error
	applyErr   error
}

func (m *mockRPCHandler) HandleJoin(req *JoinRequest) (*JoinResponse, error) {
	atomic.AddInt64(&m.joinCount, 1)
	if m.joinErr != nil {
		return nil, m.joinErr
	}
	return &JoinResponse{
		CommittedIndex: 42,
	}, nil
}

func (m *mockRPCHandler) HandleApply(req *ApplyRequest) (*ApplyResponse, error) {
	atomic.AddInt64(&m.applyCount, 1)
	if m.applyErr != nil {
		return nil, m.applyErr
	}
	return &ApplyResponse{
		Index:  100,
		Result: []byte(fmt.Sprintf("applied-%s", req.Command.Type)),
	}, nil
}

func TestRPC_Ping(t *testing.T) {
	handler := &mockRPCHandler{}
	srv, err := NewRPCServer("127.0.0.1:0", handler)
	if err != nil {
		t.Fatalf("failed to start rpc server: %v", err)
	}
	defer srv.Close()

	client := NewRPCClient()
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := client.Ping(ctx, srv.Addr()); err != nil {
		t.Fatalf("ping failed: %v", err)
	}
}

func TestRPC_JoinAndApply(t *testing.T) {
	handler := &mockRPCHandler{}
	srv, err := NewRPCServer("127.0.0.1:0", handler)
	if err != nil {
		t.Fatalf("failed to start rpc server: %v", err)
	}
	defer srv.Close()

	client := NewRPCClient()
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Test Join
	node := &Node{ID: "node-2", Address: "127.0.0.1:9001", Voter: true}
	joinResp, err := client.JoinNode(ctx, srv.Addr(), node)
	if err != nil {
		t.Fatalf("join failed: %v", err)
	}
	if joinResp.CommittedIndex != 42 {
		t.Errorf("expected committed index 42, got %d", joinResp.CommittedIndex)
	}

	// Test Apply
	cmd := &store.Command{
		Type: "kv:set",
		Data: []byte("foo=bar"),
	}
	applyResp, err := client.ApplyOnNode(ctx, srv.Addr(), cmd)
	if err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	if applyResp.Index != 100 {
		t.Errorf("expected index 100, got %d", applyResp.Index)
	}
	if string(applyResp.Result) != "applied-kv:set" {
		t.Errorf("expected result 'applied-kv:set', got %q", string(applyResp.Result))
	}
}

func TestRPC_ConnectionPooling(t *testing.T) {
	handler := &mockRPCHandler{}
	srv, err := NewRPCServer("127.0.0.1:0", handler)
	if err != nil {
		t.Fatalf("failed to start rpc server: %v", err)
	}
	defer srv.Close()

	client := NewRPCClient()
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Perform 100 sequential requests - should reuse the single connection in the pool
	for i := 0; i < 100; i++ {
		cmd := &store.Command{Type: store.CommandType(fmt.Sprintf("store-%d", i))}
		resp, err := client.ApplyOnNode(ctx, srv.Addr(), cmd)
		if err != nil {
			t.Fatalf("request %d failed: %v", i, err)
		}
		if resp.Index != 100 {
			t.Fatalf("unexpected index: %d", resp.Index)
		}
	}

	if atomic.LoadInt64(&handler.applyCount) != 100 {
		t.Errorf("expected 100 applies, got %d", handler.applyCount)
	}

	// Perform concurrent requests across 10 goroutines
	var wg sync.WaitGroup
	errCh := make(chan error, 20)
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				cmd := &store.Command{Type: store.CommandType(fmt.Sprintf("conc-%d-%d", gid, i))}
				_, err := client.ApplyOnNode(ctx, srv.Addr(), cmd)
				if err != nil {
					errCh <- err
					return
				}
			}
		}(g)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent apply failed: %v", err)
	}

	if atomic.LoadInt64(&handler.applyCount) != 300 {
		t.Errorf("expected 300 applies, got %d", handler.applyCount)
	}
}

func TestRPC_StaleConnectionRecovery(t *testing.T) {
	handler := &mockRPCHandler{}
	srv, err := NewRPCServer("127.0.0.1:0", handler)
	if err != nil {
		t.Fatalf("failed to start rpc server: %v", err)
	}
	defer srv.Close()

	client := NewRPCClient()
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// First request succeeds
	if err := client.Ping(ctx, srv.Addr()); err != nil {
		t.Fatalf("initial ping failed: %v", err)
	}

	// Forcibly close all active connections on the server side to simulate idle drop / tcp reset
	srv.connsMu.Lock()
	for c := range srv.conns {
		_ = c.Close()
	}
	srv.connsMu.Unlock()

	// Wait a moment for TCP state to settle
	time.Sleep(50 * time.Millisecond)

	// Client should auto-recover transparently and succeed
	if err := client.Ping(ctx, srv.Addr()); err != nil {
		t.Fatalf("transparent recovery ping failed: %v", err)
	}
}

func TestRPC_HandlerErrors(t *testing.T) {
	handler := &mockRPCHandler{
		joinErr:  errors.New("cluster full"),
		applyErr: errors.New("fsm apply failed"),
	}
	srv, err := NewRPCServer("127.0.0.1:0", handler)
	if err != nil {
		t.Fatalf("failed to start rpc server: %v", err)
	}
	defer srv.Close()

	client := NewRPCClient()
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	joinResp, err := client.JoinNode(ctx, srv.Addr(), &Node{ID: "n1"})
	if err != nil {
		t.Fatalf("unexpected transport err: %v", err)
	}
	if joinResp.Error != "cluster full" {
		t.Errorf("expected error 'cluster full', got %q", joinResp.Error)
	}

	applyResp, err := client.ApplyOnNode(ctx, srv.Addr(), &store.Command{Type: "kv"})
	if err != nil {
		t.Fatalf("unexpected transport err: %v", err)
	}
	if applyResp.Error != "fsm apply failed" {
		t.Errorf("expected error 'fsm apply failed', got %q", applyResp.Error)
	}
}
