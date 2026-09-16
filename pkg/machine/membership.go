package machine

import (
	"fmt"

	"github.com/avivklas/plexus/pkg/store"
	"github.com/hashicorp/raft"
)

// Node represents a cluster participant.
type Node struct {
	ID      string `json:"id"`
	Address string `json:"address"`
	Voter   bool   `json:"voter"`
}

func (n *Node) Equal(other *Node) bool {
	if n == nil || other == nil {
		return n == other
	}
	return n.ID == other.ID && n.Address == other.Address
}

func (n *Node) String() string {
	return fmt.Sprintf("Node(id=%s, addr=%s, voter=%v)", n.ID, n.Address, n.Voter)
}

// JoinRequest is sent by a joining node to the cluster leader.
type JoinRequest struct {
	Node *Node `json:"node"`
}

// JoinResponse is returned by the leader after processing join.
type JoinResponse struct {
	CommittedIndex uint64 `json:"committed_index"`
	Error          string `json:"error,omitempty"`
}

// ApplyRequest is forwarded by a follower to the cluster leader.
type ApplyRequest struct {
	Command *store.Command `json:"command"`
}

// ApplyResponse is returned by the leader to a follower after applying a command.
type ApplyResponse struct {
	Index  uint64 `json:"index"`
	Result []byte `json:"result,omitempty"`
	Error  string `json:"error,omitempty"`
}

// RaftServerToNode converts a raft.Server into a plexus Node.
func RaftServerToNode(s raft.Server) *Node {
	return &Node{
		ID:      string(s.ID),
		Address: string(s.Address),
		Voter:   s.Suffrage == raft.Voter,
	}
}
