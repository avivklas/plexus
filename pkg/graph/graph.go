package graph

import (
	"sync"

	"github.com/avivklas/plexus/pkg/dedup"
	"github.com/avivklas/plexus/pkg/machine"
	"github.com/avivklas/plexus/pkg/store"
)

// Graph represents a directed command topology composed of Raft machines.
type Graph struct {
	mu         sync.RWMutex
	name       string
	machines   map[machine.MachineID]machine.Machine
	dedupMap   map[machine.MachineID]dedup.Store
	dispatcher *Dispatcher
}

// New creates a named Raft Graph topology.
func New(name string) *Graph {
	return &Graph{
		name:       name,
		machines:   make(map[machine.MachineID]machine.Machine),
		dedupMap:   make(map[machine.MachineID]dedup.Store),
		dispatcher: NewDispatcher(),
	}
}

// Name returns the topology name.
func (g *Graph) Name() string {
	return g.name
}

// AddMachine adds a Raft machine to the graph. If a dedup.Store is provided,
// all existing and future stores on this machine will have exactly-once deduplication enabled.
func (g *Graph) AddMachine(m machine.Machine, ds dedup.Store) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.machines[m.ID()] = m
	if ds != nil {
		g.dedupMap[m.ID()] = ds
	}
}

// EnableDedupOnStore attaches the machine's dedup store to the specified Store.
func (g *Graph) EnableDedupOnStore(m machine.Machine, s store.Store) {
	g.mu.RLock()
	ds := g.dedupMap[m.ID()]
	g.mu.RUnlock()

	if ds != nil {
		AttachDedupInterceptor(s, ds)
	}
}

// Pipe connects an upstream machine to a downstream machine with a command transformer.
// Whenever upstream applies a matching mutation, the transformer generates downstream commands
// that will be executed with Exactly-Once Semantics (EOS).
func (g *Graph) Pipe(upstream, downstream machine.Machine, tf Transformer, filterTypes ...store.CommandType) *Edge {
	return g.dispatcher.Connect(upstream, downstream, tf, filterTypes...)
}

// Close shuts down the graph and its dispatcher.
func (g *Graph) Close() {
	g.dispatcher.Close()
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, ds := range g.dedupMap {
		_ = ds.Close()
	}
}
