package machine

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/avivklas/plexus/pkg/store"
	"github.com/hashicorp/raft"
)

type contextKey string

const (
	CtxKeyLogIndex contextKey = "plexus.log_index"
	CtxKeyLogTerm  contextKey = "plexus.log_term"
	CtxKeyCommand  contextKey = "plexus.command"
)

// ApplyResult wraps the response and metadata from an applied log entry.
type ApplyResult struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
	Res   any    `json:"res,omitempty"`
	Err   string `json:"err,omitempty"`
}

// LogCommitObserver is called whenever a log entry is successfully applied to the FSM.
type LogCommitObserver func(term, index uint64, cmd *store.Command, res any, err error)

// Ensure MultiStoreFSM implements raft.FSM and raft.BatchingFSM.
var (
	_ raft.FSM         = (*MultiStoreFSM)(nil)
	_ raft.BatchingFSM = (*MultiStoreFSM)(nil)
)

// MultiStoreFSM implements raft.FSM and raft.BatchingFSM, multiplexing commands to registered stores.
type MultiStoreFSM struct {
	mu                 sync.RWMutex
	stores             map[store.StoreID]store.Store
	routers            map[store.CommandType]*store.Router
	lastCommittedIndex atomic.Uint64
	lastCommittedTerm  atomic.Uint64
	observers          []LogCommitObserver
	condMu             sync.Mutex
	cond               *sync.Cond
}

// NewMultiStoreFSM creates an empty MultiStoreFSM.
func NewMultiStoreFSM() *MultiStoreFSM {
	fsm := &MultiStoreFSM{
		stores:  make(map[store.StoreID]store.Store),
		routers: make(map[store.CommandType]*store.Router),
	}
	fsm.cond = sync.NewCond(&fsm.condMu)
	return fsm
}

// RegisterStore registers a Store and its command routes with this FSM.
func (fsm *MultiStoreFSM) RegisterStore(s store.Store) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	fsm.stores[s.ID()] = s
	r := s.Router()
	for cmdType := range r.Handlers() {
		fsm.routers[cmdType] = r
	}
}

// RegisterObserver registers a callback for applied log entries.
func (fsm *MultiStoreFSM) RegisterObserver(obs LogCommitObserver) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	fsm.observers = append(fsm.observers, obs)
}

// LastCommittedIndex returns the highest log index applied to this FSM.
func (fsm *MultiStoreFSM) LastCommittedIndex() uint64 {
	return fsm.lastCommittedIndex.Load()
}

// LastCommittedTerm returns the term of the highest log index applied.
func (fsm *MultiStoreFSM) LastCommittedTerm() uint64 {
	return fsm.lastCommittedTerm.Load()
}

// WaitForIndex blocks until the local FSM has applied at least targetIndex or the context/timeout expires.
func (fsm *MultiStoreFSM) WaitForIndex(ctx context.Context, targetIndex uint64, timeout time.Duration) error {
	if fsm.lastCommittedIndex.Load() >= targetIndex {
		return nil
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	done := make(chan struct{})
	defer close(done)

	go func() {
		select {
		case <-ctx.Done():
			fsm.condMu.Lock()
			fsm.cond.Broadcast()
			fsm.condMu.Unlock()
		case <-timer.C:
			fsm.condMu.Lock()
			fsm.cond.Broadcast()
			fsm.condMu.Unlock()
		case <-done:
		}
	}()

	fsm.condMu.Lock()
	defer fsm.condMu.Unlock()

	for fsm.lastCommittedIndex.Load() < targetIndex {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return fmt.Errorf("timeout waiting for local index %d, currently at %d", targetIndex, fsm.lastCommittedIndex.Load())
		default:
		}
		fsm.cond.Wait()
	}

	return nil
}

// Apply implements raft.FSM by executing the command on the matching store router.
func (fsm *MultiStoreFSM) Apply(l *raft.Log) any {
	defer func() {
		fsm.lastCommittedIndex.Store(l.Index)
		fsm.lastCommittedTerm.Store(l.Term)
		fsm.condMu.Lock()
		fsm.cond.Broadcast()
		fsm.condMu.Unlock()
	}()

	if l.Type != raft.LogCommand {
		return &ApplyResult{Index: l.Index, Term: l.Term}
	}

	if len(l.Data) == 0 {
		return &ApplyResult{Index: l.Index, Term: l.Term}
	}

	cmd, err := store.UnmarshalCommand(l.Data)
	if err != nil {
		return &ApplyResult{Index: l.Index, Term: l.Term, Err: fmt.Sprintf("unmarshal command: %v", err)}
	}

	fsm.mu.RLock()
	router, ok := fsm.routers[cmd.Type]
	observers := append([]LogCommitObserver(nil), fsm.observers...)
	fsm.mu.RUnlock()

	if !ok {
		applyErr := fmt.Errorf("no handler for command type %q", cmd.Type)
		for _, obs := range observers {
			obs(l.Term, l.Index, cmd, nil, applyErr)
		}
		return &ApplyResult{Index: l.Index, Term: l.Term, Err: applyErr.Error()}
	}

	ctx := context.WithValue(context.Background(), CtxKeyLogIndex, l.Index)
	ctx = context.WithValue(ctx, CtxKeyLogTerm, l.Term)
	ctx = context.WithValue(ctx, CtxKeyCommand, cmd)

	res, execErr := router.Execute(ctx, cmd)

	for _, obs := range observers {
		obs(l.Term, l.Index, cmd, res, execErr)
	}

	applyRes := &ApplyResult{
		Index: l.Index,
		Term:  l.Term,
		Res:   res,
	}
	if execErr != nil {
		applyRes.Err = execErr.Error()
	}

	return applyRes
}

// ApplyBatch implements raft.BatchingFSM by applying a slice of committed logs in order.
func (fsm *MultiStoreFSM) ApplyBatch(logs []*raft.Log) []any {
	res := make([]any, len(logs))
	for i, l := range logs {
		res[i] = fsm.Apply(l)
	}
	return res
}

// Snapshot implements raft.FSM by serializing all registered stores.
func (fsm *MultiStoreFSM) Snapshot() (raft.FSMSnapshot, error) {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	snapData := make(map[string][]byte)
	for id, s := range fsm.stores {
		b, err := s.Snapshot()
		if err != nil {
			return nil, fmt.Errorf("snapshot store %s: %w", id, err)
		}
		snapData[string(id)] = b
	}

	raw, err := json.Marshal(snapData)
	if err != nil {
		return nil, fmt.Errorf("marshal snapshot: %w", err)
	}

	return &multiStoreSnapshot{data: raw}, nil
}

// Restore implements raft.FSM by resetting all stores from the snapshot stream.
func (fsm *MultiStoreFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("read snapshot: %w", err)
	}

	var snapData map[string][]byte
	if err := json.Unmarshal(data, &snapData); err != nil {
		return fmt.Errorf("unmarshal snapshot data: %w", err)
	}

	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	for id, storeData := range snapData {
		s, ok := fsm.stores[store.StoreID(id)]
		if ok {
			if err := s.Restore(storeData); err != nil {
				return fmt.Errorf("restore store %s: %w", id, err)
			}
		}
	}

	return nil
}

type multiStoreSnapshot struct {
	data []byte
}

func (s *multiStoreSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *multiStoreSnapshot) Release() {}
