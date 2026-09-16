package machine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/avivklas/plexus/pkg/logstore"
	"github.com/avivklas/plexus/pkg/store"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

// RaftMachine is the concrete implementation of Machine backed by Raft.
type RaftMachine struct {
	mu            sync.RWMutex
	cfg           Config
	node          *Node
	leader        *Node
	fsm           *MultiStoreFSM
	raft          *raft.Raft
	transport     raft.Transport
	logStore      raft.LogStore
	stableStore   raft.StableStore
	snapshotStore raft.SnapshotStore
	rpcServer     *RPCServer
	rpcClient     *RPCClient
	obsCh         chan raft.Observation
	closeCh       chan struct{}
	closeOnce     sync.Once
}

// NewRaftMachine creates an unstarted RaftMachine.
func NewRaftMachine(cfg Config) *RaftMachine {
	if cfg.Node == nil {
		cfg.Node = &Node{ID: "node-1", Address: "127.0.0.1:9000", Voter: true}
	}
	if cfg.ApplyTimeout <= 0 {
		cfg.ApplyTimeout = 30 * time.Second
	}

	return &RaftMachine{
		cfg:       cfg,
		node:      cfg.Node,
		fsm:       NewMultiStoreFSM(),
		rpcClient: NewRPCClient(),
		obsCh:     make(chan raft.Observation, 512),
		closeCh:   make(chan struct{}),
	}
}

// FSM returns the underlying MultiStoreFSM.
func (m *RaftMachine) FSM() *MultiStoreFSM {
	return m.fsm
}

// WithCustomStores injects custom storage and transport engines (e.g. for testing).
func (m *RaftMachine) WithCustomStores(log raft.LogStore, stable raft.StableStore, snap raft.SnapshotStore, trans raft.Transport) *RaftMachine {
	m.cfg.LogStore = log
	m.cfg.StableStore = stable
	m.cfg.SnapshotStore = snap
	m.cfg.Transport = trans
	return m
}

// ID returns the Machine ID.
func (m *RaftMachine) ID() MachineID {
	return m.cfg.ID
}

// Register registers a Store with this machine's FSM.
func (m *RaftMachine) Register(s store.Store) {
	m.fsm.RegisterStore(s)
}

// Start boots the Raft consensus engine and joins or bootstraps the cluster.
func (m *RaftMachine) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	raftDir := filepath.Join(m.cfg.DataDir, string(m.cfg.ID))
	if err := os.MkdirAll(raftDir, 0755); err != nil {
		return fmt.Errorf("create raft dir %s: %w", raftDir, err)
	}

	// 1. Initialize Log and Stable Store if not provided
	if m.cfg.LogStore != nil && m.cfg.StableStore != nil {
		m.logStore = m.cfg.LogStore
		m.stableStore = m.cfg.StableStore
	} else {
		ls, err := logstore.NewSegmentLogStore(logstore.DefaultConfig(raftDir))
		if err != nil {
			return fmt.Errorf("init segment log store: %w", err)
		}
		m.logStore = ls
		m.stableStore = ls
	}

	// 2. Initialize Snapshot Store if not provided
	if m.cfg.SnapshotStore != nil {
		m.snapshotStore = m.cfg.SnapshotStore
	} else {
		snaps, err := raft.NewFileSnapshotStore(raftDir, 2, os.Stderr)
		if err != nil {
			return fmt.Errorf("init snapshot store: %w", err)
		}
		m.snapshotStore = snaps
	}

	// 3. Initialize Transport if not provided
	if m.cfg.Transport != nil {
		m.transport = m.cfg.Transport
	} else {
		tcpAddr := m.node.Address
		trans, err := raft.NewTCPTransport(tcpAddr, nil, 5, 10*time.Second, os.Stderr)
		if err != nil {
			return fmt.Errorf("init tcp transport on %s: %w", tcpAddr, err)
		}
		m.transport = trans
	}

	// 4. Configure Raft
	raftConf := raft.DefaultConfig()
	raftConf.LocalID = raft.ServerID(m.node.ID)
	raftConf.LogLevel = "WARN"
	raftConf.Logger = hclog.NewNullLogger()

	r, err := raft.NewRaft(raftConf, m.fsm, m.logStore, m.stableStore, m.snapshotStore, m.transport)
	if err != nil {
		return fmt.Errorf("create raft instance: %w", err)
	}
	m.raft = r
	m.raft.RegisterObserver(raft.NewObserver(m.obsCh, false, nil))

	go m.observeRaft()

	// 5. Bootstrap or Join
	if m.cfg.Bootstrap {
		bootConf := raft.Configuration{
			Servers: []raft.Server{{
				ID:      raft.ServerID(m.node.ID),
				Address: raft.ServerAddress(m.transport.LocalAddr()),
			}},
		}
		if err := m.raft.BootstrapCluster(bootConf).Error(); err != nil {
			return fmt.Errorf("bootstrap cluster: %w", err)
		}
	} else if len(m.cfg.JoinAddrs) > 0 {
		go m.autoJoin(m.cfg.JoinAddrs)
	}

	return nil
}

func (m *RaftMachine) autoJoin(addrs []string) {
	node := &Node{
		ID:      m.node.ID,
		Address: string(m.transport.LocalAddr()),
		Voter:   m.node.Voter,
	}

	for _, addr := range addrs {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		res, err := m.rpcClient.JoinNode(ctx, addr, node)
		cancel()
		if err == nil && res.Error == "" {
			_ = m.waitForLocalIndex(context.Background(), res.CommittedIndex)
			return
		}
	}
}

func (m *RaftMachine) observeRaft() {
	for {
		select {
		case <-m.closeCh:
			return
		case obs, ok := <-m.obsCh:
			if !ok {
				return
			}
			switch v := obs.Data.(type) {
			case raft.LeaderObservation:
				m.mu.Lock()
				if v.Leader == "" {
					m.leader = nil
				} else {
					m.leader = &Node{Address: string(v.Leader)}
				}
				m.mu.Unlock()
			}
		}
	}
}

// IsLeader returns true if the current node is the elected leader.
func (m *RaftMachine) IsLeader() bool {
	if m.raft == nil {
		return false
	}
	return m.raft.State() == raft.Leader
}

// Leader returns the current leader node.
func (m *RaftMachine) Leader() *Node {
	if m.raft == nil {
		return nil
	}
	addr := string(m.raft.Leader())
	if addr == "" {
		return nil
	}
	return &Node{Address: addr}
}

// Me returns the local node descriptor.
func (m *RaftMachine) Me() *Node {
	return m.node
}

// Members returns the list of current cluster servers.
func (m *RaftMachine) Members() ([]*Node, error) {
	if m.raft == nil {
		return nil, ErrNoLeader
	}
	cf := m.raft.GetConfiguration()
	if err := cf.Error(); err != nil {
		return nil, err
	}
	var nodes []*Node
	for _, s := range cf.Configuration().Servers {
		nodes = append(nodes, RaftServerToNode(s))
	}
	return nodes, nil
}

// ShouldReadLocally guarantees sequential read consistency.
// Since writes ACK only after quorum + local apply, if this node is leader or has caught up,
// local reads are strongly consistent with all previously acknowledged writes on this node.
func (m *RaftMachine) ShouldReadLocally() bool {
	if m.raft == nil {
		return false
	}
	return m.IsLeader() || m.fsm.LastCommittedIndex() >= m.raft.AppliedIndex()
}

// LastCommittedIndex returns the highest log index applied to the local FSM.
func (m *RaftMachine) LastCommittedIndex() uint64 {
	return m.fsm.LastCommittedIndex()
}

// Apply proposes a typed command to the cluster with quorum + local consistency.
func (m *RaftMachine) Apply(ctx context.Context, cmdType store.CommandType, data any) (any, error) {
	cmd, err := store.NewCommand(cmdType, data)
	if err != nil {
		return nil, err
	}
	return m.ApplyCommand(ctx, cmd)
}

// ApplyCommand proposes a command to the cluster.
// ACK is returned only after commitment to Quorum AND application to the current node.
func (m *RaftMachine) ApplyCommand(ctx context.Context, cmd *store.Command) (any, error) {
	if m.raft == nil {
		return nil, errors.New("raft machine not started")
	}

	if m.IsLeader() {
		return m.applyLeader(ctx, cmd)
	}

	// Follower forwarding
	leader := m.Leader()
	if leader == nil {
		return nil, ErrNoLeader
	}

	resp, err := m.rpcClient.ApplyOnNode(ctx, leader.Address, cmd)
	if err != nil {
		return nil, fmt.Errorf("forward command to leader %s: %w", leader.Address, err)
	}
	if resp.Error != "" {
		return nil, errors.New(resp.Error)
	}

	// Crucial: Wait until THIS follower's local FSM has applied the committed index!
	if err := m.waitForLocalIndex(ctx, resp.Index); err != nil {
		return nil, fmt.Errorf("wait for follower local apply index %d: %w", resp.Index, err)
	}

	var res any
	if len(resp.Result) > 0 {
		_ = json.Unmarshal(resp.Result, &res)
	}
	return res, nil
}

func (m *RaftMachine) applyLeader(ctx context.Context, cmd *store.Command) (any, error) {
	payload, err := cmd.Marshal()
	if err != nil {
		return nil, fmt.Errorf("marshal command: %w", err)
	}

	future := m.raft.Apply(payload, m.cfg.ApplyTimeout)
	if err := future.Error(); err != nil {
		return nil, fmt.Errorf("raft apply: %w", err)
	}

	committedIndex := future.Index()

	// Crucial: Wait for local state machine apply on the leader!
	if err := m.waitForLocalIndex(ctx, committedIndex); err != nil {
		return nil, fmt.Errorf("wait for leader local apply index %d: %w", committedIndex, err)
	}

	res := future.Response()
	if applyRes, ok := res.(*ApplyResult); ok {
		if applyRes.Err != "" {
			return nil, errors.New(applyRes.Err)
		}
		return applyRes.Res, nil
	}

	return res, nil
}

func (m *RaftMachine) waitForLocalIndex(ctx context.Context, expectedIndex uint64) error {
	if expectedIndex == 0 {
		return nil
	}

	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	timeoutCh := time.After(m.cfg.ApplyTimeout)

	for {
		if m.fsm.LastCommittedIndex() >= expectedIndex {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeoutCh:
			return fmt.Errorf("%w: waiting for local index %d, currently at %d", ErrTimeout, expectedIndex, m.fsm.LastCommittedIndex())
		case <-ticker.C:
		}
	}
}

// HandleJoin implements RPCHandler.
func (m *RaftMachine) HandleJoin(req *JoinRequest) (*JoinResponse, error) {
	if !m.IsLeader() {
		return nil, ErrNotLeader
	}

	serverID := raft.ServerID(req.Node.ID)
	serverAddr := raft.ServerAddress(req.Node.Address)

	var future raft.IndexFuture
	if req.Node.Voter {
		future = m.raft.AddVoter(serverID, serverAddr, 0, m.cfg.ApplyTimeout)
	} else {
		future = m.raft.AddNonvoter(serverID, serverAddr, 0, m.cfg.ApplyTimeout)
	}

	if err := future.Error(); err != nil {
		return nil, err
	}

	return &JoinResponse{
		CommittedIndex: future.Index(),
	}, nil
}

// HandleApply implements RPCHandler for follower-forwarded commands.
func (m *RaftMachine) HandleApply(req *ApplyRequest) (*ApplyResponse, error) {
	if !m.IsLeader() {
		return nil, ErrNotLeader
	}

	res, err := m.applyLeader(context.Background(), req.Command)
	resp := &ApplyResponse{
		Index: m.fsm.LastCommittedIndex(),
	}
	if err != nil {
		resp.Error = err.Error()
		return resp, nil
	}

	if res != nil {
		b, _ := json.Marshal(res)
		resp.Result = b
	}

	return resp, nil
}

// TakeSnapshot triggers an immediate state machine snapshot.
func (m *RaftMachine) TakeSnapshot() error {
	if m.raft == nil {
		return errors.New("raft machine not started")
	}
	return m.raft.Snapshot().Error()
}

// Stop shuts down the Raft machine and closes transports.
func (m *RaftMachine) Stop() error {
	m.closeOnce.Do(func() {
		close(m.closeCh)
	})

	if m.rpcServer != nil {
		_ = m.rpcServer.Close()
	}

	if m.raft != nil {
		f := m.raft.Shutdown()
		_ = f.Error()
	}

	if closer, ok := m.logStore.(io.Closer); ok {
		_ = closer.Close()
	}

	return nil
}
