package plexus

import (
	"context"

	"github.com/avivklas/plexus/pkg/cluster"
	"github.com/avivklas/plexus/pkg/dedup"
	"github.com/avivklas/plexus/pkg/graph"
	"github.com/avivklas/plexus/pkg/machine"
	"github.com/avivklas/plexus/pkg/store"
)

// Re-exported primary types for ergonomic top-level usage.
type (
	// Store is the state machine interface for deterministic mutations.
	Store = store.Store
	// BaseStore provides default implementations for routers and hooks.
	BaseStore = store.BaseStore
	// Command represents a proposed mutation envelope.
	Command = store.Command
	// CommandType identifies a mutation inside a Store.
	CommandType = store.CommandType
	// StoreID identifies a store within a machine.
	StoreID = store.StoreID
	// Mutator allows proposing mutations and checking cluster conditions.
	Mutator = store.Mutator
	// Router maps command types to handlers.
	Router = store.Router
	// StateFlag specifies cluster conditions.
	StateFlag = store.StateFlag

	// Cluster coordinates embedded machines and RPC in one process.
	Cluster = cluster.Cluster
	// ClusterConfig configures the single-process cluster coordinator.
	ClusterConfig = cluster.Config

	// Machine is the Raft consensus engine interface.
	Machine = machine.Machine
	// RaftMachine is the concrete Raft machine implementation.
	RaftMachine = machine.RaftMachine
	// MachineID identifies a machine within the cluster.
	MachineID = machine.MachineID
	// MachineConfig configures a RaftMachine.
	MachineConfig = machine.Config
	// Node represents a participant in the cluster.
	Node = machine.Node

	// Graph represents a directed command topology of Raft machines.
	Graph = graph.Graph
	// Transformer maps upstream commands into downstream commands.
	Transformer = graph.Transformer

	// DedupStore is the deduplication store interface for exactly-once execution.
	DedupStore = dedup.Store
	// DedupRecord represents an applied idempotency record.
	DedupRecord = dedup.Record
)

const (
	StateIsLeader  = store.StateIsLeader
	StateHasLeader = store.StateHasLeader
	StateNoLeader  = store.StateNoLeader
)

// NewCluster creates an embedded single-process cluster coordinator.
var NewCluster = cluster.New

// DefaultClusterConfig returns standard configuration for a cluster node.
var DefaultClusterConfig = cluster.DefaultConfig

// NewBaseStore creates an initialized BaseStore.
var NewBaseStore = store.NewBaseStore

// NewGraph creates a Raft Graph command topology.
var NewGraph = graph.New

// NewMemoryDedup creates an in-memory dedup store.
var NewMemoryDedup = dedup.NewMemoryStore

// NewFileDedup creates an embedded persistent disk-backed dedup store.
var NewFileDedup = dedup.NewFileStore

// Handle registers a type-safe generic command handler on a router.
func Handle[Req any, Resp any](
	r *store.Router,
	cmdType store.CommandType,
	fn func(ctx context.Context, req Req) (Resp, error),
) {
	store.HandleTyped(r, cmdType, fn)
}
