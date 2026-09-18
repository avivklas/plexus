package cluster

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/avivklas/plexus/pkg/machine"
	"github.com/avivklas/plexus/pkg/store"
	"github.com/hashicorp/raft"
)

// Config configures a single-process Plexus cluster node.
type Config struct {
	NodeID        string
	BindAddr      string
	AdvertiseAddr string
	DataDir       string
	JoinAddrs     []string
	Bootstrap     bool
	ApplyTimeout  time.Duration
	SyncLog                bool
	FollowerWaitLocalApply bool
}

// DefaultConfig returns standard cluster node defaults.
func DefaultConfig(nodeID, bindAddr, dataDir string) Config {
	return Config{
		NodeID:                 nodeID,
		BindAddr:               bindAddr,
		DataDir:                dataDir,
		ApplyTimeout:           30 * time.Second,
		Bootstrap:              false,
		FollowerWaitLocalApply: true,
	}
}

// Cluster manages embedded state machines, Raft consensus, and node membership all in one process.
type Cluster struct {
	mu           sync.RWMutex
	cfg          Config
	node         *machine.Node
	machines     map[machine.MachineID]*machine.RaftMachine
	defaultMach  *machine.RaftMachine
	rpcServer    *machine.RPCServer
	mux          *machine.MuxListener
	bootstrapped bool
}

// New creates a new single-process Cluster coordinator.
func New(cfg Config) (*Cluster, error) {
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("NodeID is required")
	}
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("DataDir is required")
	}
	if cfg.ApplyTimeout <= 0 {
		cfg.ApplyTimeout = 30 * time.Second
	}

	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	addr := cfg.AdvertiseAddr
	if addr == "" {
		addr = cfg.BindAddr
	}

	node := &machine.Node{
		ID:      cfg.NodeID,
		Address: addr,
		Voter:   true,
	}

	c := &Cluster{
		cfg:      cfg,
		node:     node,
		machines: make(map[machine.MachineID]*machine.RaftMachine),
	}

	// Create default machine
	defaultMachCfg := machine.DefaultConfig(
		"default",
		node,
		filepath.Join(cfg.DataDir, "default"),
	)
	defaultMachCfg.Bootstrap = cfg.Bootstrap
	defaultMachCfg.JoinAddrs = cfg.JoinAddrs
	defaultMachCfg.ApplyTimeout = cfg.ApplyTimeout
	defaultMachCfg.SyncLog = cfg.SyncLog
	defaultMachCfg.FollowerWaitLocalApply = cfg.FollowerWaitLocalApply

	defaultMach := machine.NewRaftMachine(defaultMachCfg)
	c.machines["default"] = defaultMach
	c.defaultMach = defaultMach

	return c, nil
}

// Me returns the local node descriptor.
func (c *Cluster) Me() *machine.Node {
	return c.node
}

// DefaultMachine returns the primary consensus machine.
func (c *Cluster) DefaultMachine() machine.Machine {
	return c.defaultMach
}

// DefaultRaftMachine returns the concrete RaftMachine implementation.
func (c *Cluster) DefaultRaftMachine() *machine.RaftMachine {
	return c.defaultMach
}

// Machine returns a machine by ID, or nil if not found.
func (c *Cluster) Machine(id machine.MachineID) machine.Machine {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.machines[id]
}

// UseState registers a Store into the default cluster machine and returns a Mutator handle.
// This provides an effortless developer experience for registering stores.
func (c *Cluster) UseState(s store.Store) store.Mutator {
	c.defaultMach.Register(s)
	return &clusterMutator{
		machine: c.defaultMach,
	}
}

// UseStateOn registers a Store onto a specific Machine and returns its Mutator.
func (c *Cluster) UseStateOn(machID machine.MachineID, s store.Store) (store.Mutator, error) {
	c.mu.RLock()
	m, ok := c.machines[machID]
	c.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("machine %s not found", machID)
	}
	m.Register(s)
	return &clusterMutator{
		machine: m,
	}, nil
}

// Start boots all machines and the cluster RPC server.
func (c *Cluster) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 1. Initialize Network Multiplexer and RPC Server if bind address provided
	if c.cfg.BindAddr != "" {
		if c.defaultMach.Transport() == nil {
			adv := c.cfg.AdvertiseAddr
			if adv == "" {
				adv = c.cfg.BindAddr
			}
			mux, raftLayer, httpLn, err := machine.NewMuxListenerWithAdvertise(c.cfg.BindAddr, adv)
			if err != nil {
				return fmt.Errorf("init cluster mux on %s: %w", c.cfg.BindAddr, err)
			}
			c.mux = mux
			c.defaultMach.WithTransport(raft.NewNetworkTransport(raftLayer, 5, 10*time.Second, os.Stderr))
			srv, err := machine.NewRPCServerWithListener(httpLn, c.defaultMach)
			if err != nil {
				return fmt.Errorf("start cluster rpc server: %w", err)
			}
			c.rpcServer = srv
		} else {
			srv, err := machine.NewRPCServer(c.cfg.BindAddr, c.defaultMach)
			if err != nil {
				return fmt.Errorf("start cluster rpc server: %w", err)
			}
			c.rpcServer = srv
		}
	}

	// 2. Start all machines
	for _, m := range c.machines {
		if err := m.Start(ctx); err != nil {
			return fmt.Errorf("start machine %s: %w", m.ID(), err)
		}
	}

	return nil
}

// Stop shuts down the cluster and all underlying machines.
func (c *Cluster) Stop() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.rpcServer != nil {
		_ = c.rpcServer.Close()
	}
	if c.mux != nil {
		_ = c.mux.Close()
	}

	for _, m := range c.machines {
		_ = m.Stop()
	}
	return nil
}

type clusterMutator struct {
	machine *machine.RaftMachine
}

func (m *clusterMutator) Apply(ctx context.Context, typ store.CommandType, data any) (any, error) {
	return m.machine.Apply(ctx, typ, data)
}

func (m *clusterMutator) Initialized() bool {
	return m.machine.ShouldReadLocally()
}

func (m *clusterMutator) Do(flag store.StateFlag, fn func()) bool {
	switch flag {
	case store.StateIsLeader:
		if m.machine.IsLeader() {
			fn()
			return true
		}
	case store.StateHasLeader:
		if m.machine.Leader() != nil {
			fn()
			return true
		}
	case store.StateNoLeader:
		if m.machine.Leader() == nil {
			fn()
			return true
		}
	}
	return false
}
