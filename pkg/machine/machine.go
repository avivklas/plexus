package machine

import (
	"context"
	"errors"
	"time"

	"github.com/avivklas/plexus/pkg/store"
	"github.com/hashicorp/raft"
)

var (
	// ErrNoLeader is returned when an operation requires an elected leader but none exists.
	ErrNoLeader = errors.New("no leader elected in cluster")
	// ErrNotLeader is returned when an operation is executed on a follower.
	ErrNotLeader = errors.New("current node is not the cluster leader")
	// ErrTimeout is returned when waiting for quorum or local apply exceeds duration.
	ErrTimeout = errors.New("operation timed out")
)

// MachineID uniquely identifies a Raft machine cluster.
type MachineID string

func (m MachineID) String() string {
	return string(m)
}

// Machine defines the interface of a distributed consensus state machine.
type Machine interface {
	// ID returns the identifier of this machine.
	ID() MachineID

	// Register adds a state store to the state machine FSM.
	Register(s store.Store)

	// Apply proposes a typed command to the cluster with quorum + local consistency.
	Apply(ctx context.Context, cmdType store.CommandType, data any) (any, error)

	// ApplyCommand proposes a pre-built Command to the cluster.
	ApplyCommand(ctx context.Context, cmd *store.Command) (any, error)

	// Start boots consensus, joins or bootstraps the cluster.
	Start(ctx context.Context) error

	// Stop gracefully shuts down the machine.
	Stop() error

	// IsLeader returns true if the current node is the active cluster leader.
	IsLeader() bool

	// Leader returns the current cluster leader node, or nil if unknown.
	Leader() *Node

	// Me returns the local node descriptor.
	Me() *Node

	// Members returns the current list of cluster members.
	Members() ([]*Node, error)

	// ShouldReadLocally returns whether it is safe to read directly from the local node.
	// In Plexus, this is true if the node is leader, or if local FSM has caught up with latest known index.
	ShouldReadLocally() bool

	// TakeSnapshot triggers an immediate state machine snapshot.
	TakeSnapshot() error

	// LastCommittedIndex returns the highest log index applied to the local FSM.
	LastCommittedIndex() uint64
}

// Config configures a Raft Machine.
type Config struct {
	ID             MachineID
	Node           *Node
	DataDir        string
	BindAddr       string
	JoinAddrs      []string
	LogStore       raft.LogStore
	StableStore    raft.StableStore
	SnapshotStore  raft.SnapshotStore
	Transport      raft.Transport
	MaxVoters      int
	ApplyTimeout   time.Duration
	SnapshotThresh uint64
	TrailingLogs   uint64
	Bootstrap              bool
	SyncLog                bool
	FollowerWaitLocalApply bool
}

// DefaultConfig returns sensible defaults for machine configuration.
func DefaultConfig(id MachineID, node *Node, dataDir string) Config {
	return Config{
		ID:                     id,
		Node:                   node,
		DataDir:                dataDir,
		ApplyTimeout:           30 * time.Second,
		SnapshotThresh:         10000,
		TrailingLogs:           1000,
		MaxVoters:              5,
		Bootstrap:              false,
		FollowerWaitLocalApply: true,
	}
}
