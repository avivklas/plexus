package graph

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/avivklas/plexus/pkg/machine"
	"github.com/avivklas/plexus/pkg/store"
)

// Transformer generates downstream commands based on an upstream applied command.
type Transformer func(ctx context.Context, cmd *store.Command, res any) ([]*store.Command, error)

// Edge binds an upstream machine to a downstream machine through a Transformer.
type Edge struct {
	Upstream    machine.Machine
	Downstream  machine.Machine
	Transform   Transformer
	FilterTypes []store.CommandType
}

// Dispatcher manages the reliable delivery of generated commands between machines.
type Dispatcher struct {
	mu     sync.RWMutex
	edges  []*Edge
	ctx    context.Context
	cancel context.CancelFunc
}

// NewDispatcher creates a new Graph Dispatcher.
func NewDispatcher() *Dispatcher {
	ctx, cancel := context.WithCancel(context.Background())
	return &Dispatcher{
		ctx:    ctx,
		cancel: cancel,
	}
}

// Connect wires an upstream machine to a downstream machine with a transformer.
func (d *Dispatcher) Connect(upstream, downstream machine.Machine, tf Transformer, filterTypes ...store.CommandType) *Edge {
	edge := &Edge{
		Upstream:    upstream,
		Downstream:  downstream,
		Transform:   tf,
		FilterTypes: filterTypes,
	}

	d.mu.Lock()
	d.edges = append(d.edges, edge)
	d.mu.Unlock()

	// Register commit observer on upstream FSM if it's a RaftMachine
	if rm, ok := upstream.(*machine.RaftMachine); ok {
		rm.FSM().RegisterObserver(func(term, index uint64, cmd *store.Command, res any, err error) {
			if err != nil {
				return // Don't propagate failed mutations
			}
			go d.dispatchEdge(edge, term, index, cmd, res)
		})
	}

	return edge
}

func (d *Dispatcher) dispatchEdge(edge *Edge, term, index uint64, cmd *store.Command, res any) {
	// Only upstream leader dispatches to downstream to avoid redundant network traffic
	if !edge.Upstream.IsLeader() {
		return
	}

	if len(edge.FilterTypes) > 0 {
		matched := false
		for _, ft := range edge.FilterTypes {
			if ft == cmd.Type {
				matched = true
				break
			}
		}
		if !matched {
			return
		}
	}

	ctx, cancel := context.WithTimeout(d.ctx, 30*time.Second)
	defer cancel()

	downstreamCmds, err := edge.Transform(ctx, cmd, res)
	if err != nil || len(downstreamCmds) == 0 {
		return
	}

	for seq, dCmd := range downstreamCmds {
		// Formulate the deterministic Idempotency Key: UpstreamID:Term:Index:Seq
		idempKey := fmt.Sprintf("%s:%d:%d:%d", edge.Upstream.ID(), term, index, seq)
		dCmd.SetIdempotencyKey(idempKey)
		dCmd.SetMetadata(store.MetadataKeyOriginMachine, string(edge.Upstream.ID()))
		dCmd.SetMetadata(store.MetadataKeyOriginTerm, strconv.FormatUint(term, 10))
		dCmd.SetMetadata(store.MetadataKeyOriginIndex, strconv.FormatUint(index, 10))
		dCmd.SetMetadata(store.MetadataKeyOriginSeq, strconv.Itoa(seq))

		// Dispatch with retry
		d.sendWithRetry(ctx, edge.Downstream, dCmd)
	}
}

func (d *Dispatcher) sendWithRetry(ctx context.Context, downstream machine.Machine, cmd *store.Command) {
	backoff := 10 * time.Millisecond
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		_, err := downstream.ApplyCommand(ctx, cmd)
		if err == nil {
			return
		}

		time.Sleep(backoff)
		if backoff < 500*time.Millisecond {
			backoff *= 2
		}
	}
}

// Close stops the dispatcher and cancels active dispatches.
func (d *Dispatcher) Close() {
	d.cancel()
}
