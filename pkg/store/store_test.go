package store

import (
	"context"
	"errors"
	"testing"
)

type sampleReq struct {
	Name  string `json:"name"`
	Value int    `json:"value"`
}

type sampleResp struct {
	Greeting string `json:"greeting"`
	Doubled  int    `json:"doubled"`
}

func TestCommandSerialization(t *testing.T) {
	cmd, err := NewCommand("test.greet", sampleReq{Name: "Alice", Value: 42})
	if err != nil {
		t.Fatalf("NewCommand failed: %v", err)
	}

	cmd.SetIdempotencyKey("machine-1:1:100:0")
	cmd.SetMetadata("custom", "val")

	b, err := cmd.Marshal()
	if err != nil {
		t.Fatalf("cmd.Marshal failed: %v", err)
	}

	restored, err := UnmarshalCommand(b)
	if err != nil {
		t.Fatalf("UnmarshalCommand failed: %v", err)
	}

	if restored.Type != "test.greet" {
		t.Errorf("expected type test.greet, got %s", restored.Type)
	}
	if restored.IdempotencyKey() != "machine-1:1:100:0" {
		t.Errorf("expected idempotency key machine-1:1:100:0, got %s", restored.IdempotencyKey())
	}
	if restored.GetMetadata("custom") != "val" {
		t.Errorf("expected metadata val, got %s", restored.GetMetadata("custom"))
	}

	var req sampleReq
	if err := restored.Decode(&req); err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	if req.Name != "Alice" || req.Value != 42 {
		t.Errorf("unexpected decoded values: %+v", req)
	}
}

func TestRouterTyped(t *testing.T) {
	r := NewRouter()
	HandleTyped(r, "math.double", func(ctx context.Context, req sampleReq) (sampleResp, error) {
		return sampleResp{
			Greeting: "Hello " + req.Name,
			Doubled:  req.Value * 2,
		}, nil
	})

	cmd, err := NewCommand("math.double", sampleReq{Name: "Bob", Value: 21})
	if err != nil {
		t.Fatalf("NewCommand failed: %v", err)
	}

	res, err := r.Execute(context.Background(), cmd)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	resp, ok := res.(sampleResp)
	if !ok {
		t.Fatalf("expected sampleResp, got %T", res)
	}
	if resp.Greeting != "Hello Bob" || resp.Doubled != 42 {
		t.Errorf("unexpected resp: %+v", resp)
	}
}

func TestRouterHooks(t *testing.T) {
	r := NewRouter()
	HandleTyped(r, "test.hook", func(ctx context.Context, req sampleReq) (sampleResp, error) {
		return sampleResp{Greeting: req.Name, Doubled: req.Value}, nil
	})

	preHookCalled := false
	r.AddPreHook("test.hook", func(ctx context.Context, cmd *Command) (*Command, error) {
		preHookCalled = true
		cmd.SetMetadata("hooked", "true")
		return cmd, nil
	})

	postHookCalled := false
	r.AddPostHook("test.hook", func(ctx context.Context, cmd *Command, applyRes any, applyErr error) (any, error) {
		postHookCalled = true
		resp := applyRes.(sampleResp)
		resp.Greeting += " (modified)"
		return resp, applyErr
	})

	cmd, _ := NewCommand("test.hook", sampleReq{Name: "Carol", Value: 10})
	res, err := r.Execute(context.Background(), cmd)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !preHookCalled || !postHookCalled {
		t.Errorf("hooks not called: pre=%v post=%v", preHookCalled, postHookCalled)
	}

	resp := res.(sampleResp)
	if resp.Greeting != "Carol (modified)" {
		t.Errorf("expected Carol (modified), got %s", resp.Greeting)
	}
}

func TestRouterPreHookAbort(t *testing.T) {
	r := NewRouter()
	r.Handle("test.abort", func(ctx context.Context, data []byte) (any, error) {
		return "should not run", nil
	})

	r.AddPreHook("test.abort", func(ctx context.Context, cmd *Command) (*Command, error) {
		return nil, errors.New("aborted by pre-hook")
	})

	cmd, _ := NewCommand("test.abort", nil)
	_, err := r.Execute(context.Background(), cmd)
	if err == nil || err.Error() != "pre-hook error on test.abort: aborted by pre-hook" {
		t.Errorf("unexpected error: %v", err)
	}
}
