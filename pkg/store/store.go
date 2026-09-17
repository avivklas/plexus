package store

import (
	"context"
	"io"
)

// StoreID uniquely identifies a Store inside a Plexus Machine.
type StoreID string

func (s StoreID) String() string {
	return string(s)
}

// StateFlag specifies leadership or cluster conditions for executing functions.
type StateFlag int

const (
	// StateIsLeader specifies that the current node must be the leader.
	StateIsLeader StateFlag = iota
	// StateHasLeader specifies that the cluster has an active elected leader.
	StateHasLeader
	// StateNoLeader specifies that there is currently no active leader.
	StateNoLeader
)

// Store defines the state machine interface for deterministic mutations.
type Store interface {
	// ID returns the unique identifier for this store.
	ID() StoreID

	// Router returns the command routing table for this store.
	Router() *Router

	// PreHooks returns pre-apply hooks mapped by command type.
	PreHooks() map[CommandType]PreHook

	// PostHooks returns post-apply hooks mapped by command type.
	PostHooks() map[CommandType]PostHook

	// Snapshot creates an immutable byte slice representing the full store state.
	Snapshot() ([]byte, error)

	// Restore resets the store's in-memory state from a snapshot.
	Restore(data []byte) error
}

// StreamStore extends Store for high-volume state machines that stream snapshots.
type StreamStore interface {
	Store

	// SnapshotStream writes the snapshot stream directly to a writer.
	SnapshotStream(w io.Writer) error

	// RestoreStream reads and restores state directly from a stream.
	RestoreStream(r io.Reader) error
}

// Mutator is the caller's handle to propose mutations and query state machine condition.
type Mutator interface {
	// Apply proposes a mutation command to the state machine cluster.
	Apply(ctx context.Context, typ CommandType, data any) (any, error)

	// Initialized returns true if the state machine has booted and caught up.
	Initialized() bool

	// Do executes fn if the specified cluster condition is currently met.
	Do(condition StateFlag, fn func()) bool
}

// BaseStore provides convenient default implementations of Router and Hook getters.
// Developers can embed BaseStore into their state machine structs.
type BaseStore struct {
	router *Router
}

// NewBaseStore creates a BaseStore initialized with an empty Router.
func NewBaseStore() BaseStore {
	return BaseStore{
		router: NewRouter(),
	}
}

// Router returns the underlying Router.
func (b *BaseStore) Router() *Router {
	if b.router == nil {
		b.router = NewRouter()
	}
	return b.router
}

// Handle registers a generic, type-safe command handler directly on the store.
func (b *BaseStore) Handle[Req any, Resp any](
	cmdType CommandType,
	fn func(ctx context.Context, req Req) (Resp, error),
) {
	b.Router().HandleTyped(cmdType, fn)
}


// PreHooks returns pre-hooks registered in the router.
func (b *BaseStore) PreHooks() map[CommandType]PreHook {
	return b.Router().PreHooks()
}

// PostHooks returns post-hooks registered in the router.
func (b *BaseStore) PostHooks() map[CommandType]PostHook {
	return b.Router().PostHooks()
}
