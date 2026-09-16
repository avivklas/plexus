package machine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/avivklas/plexus/pkg/store"
)

// RPCHandler provides RPC endpoints for cluster management and forwarded commands.
type RPCHandler interface {
	HandleJoin(req *JoinRequest) (*JoinResponse, error)
	HandleApply(req *ApplyRequest) (*ApplyResponse, error)
}

// RPCServer serves HTTP RPC requests for a Machine.
type RPCServer struct {
	handler RPCHandler
	server  *http.Server
	ln      net.Listener
}

// NewRPCServer starts an HTTP RPC listener.
func NewRPCServer(addr string, handler RPCHandler) (*RPCServer, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", addr, err)
	}

	mux := http.NewServeMux()
	srv := &RPCServer{
		handler: handler,
		ln:      ln,
	}

	mux.HandleFunc("/cluster/join", srv.handleJoinHTTP)
	mux.HandleFunc("/cluster/apply", srv.handleApplyHTTP)

	srv.server = &http.Server{
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	go func() {
		_ = srv.server.Serve(ln)
	}()

	return srv, nil
}

// Addr returns the actual listening address.
func (s *RPCServer) Addr() string {
	return s.ln.Addr().String()
}

// Close shuts down the RPC server.
func (s *RPCServer) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return s.server.Shutdown(ctx)
}

func (s *RPCServer) handleJoinHTTP(w http.ResponseWriter, r *http.Request) {
	var req JoinRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp, err := s.handler.HandleJoin(&req)
	if err != nil {
		resp = &JoinResponse{Error: err.Error()}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *RPCServer) handleApplyHTTP(w http.ResponseWriter, r *http.Request) {
	var req ApplyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp, err := s.handler.HandleApply(&req)
	if err != nil {
		resp = &ApplyResponse{Error: err.Error()}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// RPCClient communicates with other nodes in the cluster.
type RPCClient struct {
	client *http.Client
}

// NewRPCClient creates a new RPC client.
func NewRPCClient() *RPCClient {
	return &RPCClient{
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// JoinNode sends a join request to a remote node address.
func (c *RPCClient) JoinNode(ctx context.Context, targetAddr string, node *Node) (*JoinResponse, error) {
	reqBody, err := json.Marshal(&JoinRequest{Node: node})
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("http://%s/cluster/join", targetAddr)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()

	var resp JoinResponse
	if err := json.NewDecoder(httpResp.Body).Decode(&resp); err != nil {
		return nil, fmt.Errorf("decode join response: %w", err)
	}
	return &resp, nil
}

// ApplyOnNode sends a forwarded command to the cluster leader.
func (c *RPCClient) ApplyOnNode(ctx context.Context, targetAddr string, cmd *store.Command) (*ApplyResponse, error) {
	reqBody, err := json.Marshal(&ApplyRequest{Command: cmd})
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("http://%s/cluster/apply", targetAddr)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(httpResp.Body)
		return nil, fmt.Errorf("leader returned status %d: %s", httpResp.StatusCode, string(b))
	}

	var resp ApplyResponse
	if err := json.NewDecoder(httpResp.Body).Decode(&resp); err != nil {
		return nil, fmt.Errorf("decode apply response: %w", err)
	}
	return &resp, nil
}
