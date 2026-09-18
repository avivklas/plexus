package machine

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/avivklas/plexus/pkg/store"
)

// RPCHandler provides RPC endpoints for cluster management and forwarded commands.
type RPCHandler interface {
	HandleJoin(req *JoinRequest) (*JoinResponse, error)
	HandleApply(req *ApplyRequest) (*ApplyResponse, error)
}

// RPCServer serves high-performance RESP RPC requests for a Machine.
type RPCServer struct {
	handler   RPCHandler
	ln        net.Listener
	closed    chan struct{}
	closeOnce sync.Once
	connsMu   sync.Mutex
	conns     map[net.Conn]struct{}
}

// NewRPCServer starts a RESP RPC listener on addr.
func NewRPCServer(addr string, handler RPCHandler) (*RPCServer, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", addr, err)
	}
	return NewRPCServerWithListener(ln, handler)
}

// NewRPCServerWithListener starts a RESP RPC listener on an existing net.Listener.
func NewRPCServerWithListener(ln net.Listener, handler RPCHandler) (*RPCServer, error) {
	srv := &RPCServer{
		handler: handler,
		ln:      ln,
		closed:  make(chan struct{}),
		conns:   make(map[net.Conn]struct{}),
	}
	go srv.serve()
	return srv, nil
}

// Addr returns the actual listening address.
func (s *RPCServer) Addr() string {
	return s.ln.Addr().String()
}

// Close shuts down the RPC server and closes all active connections.
func (s *RPCServer) Close() error {
	var err error
	s.closeOnce.Do(func() {
		close(s.closed)
		err = s.ln.Close()

		s.connsMu.Lock()
		for c := range s.conns {
			_ = c.Close()
		}
		s.conns = nil
		s.connsMu.Unlock()
	})
	return err
}

func (s *RPCServer) trackConn(c net.Conn, add bool) {
	s.connsMu.Lock()
	defer s.connsMu.Unlock()
	if s.conns == nil {
		return
	}
	if add {
		s.conns[c] = struct{}{}
	} else {
		delete(s.conns, c)
	}
}

func (s *RPCServer) serve() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			select {
			case <-s.closed:
				return
			default:
				time.Sleep(10 * time.Millisecond)
				continue
			}
		}

		s.trackConn(conn, true)
		go func(c net.Conn) {
			defer func() {
				s.trackConn(c, false)
				_ = c.Close()
			}()
			s.handleConn(c)
		}(conn)
	}
}

func (s *RPCServer) handleConn(c net.Conn) {
	rd := bufio.NewReader(c)
	wr := bufio.NewWriter(c)

	for {
		args, err := readRESPCommand(rd)
		if err != nil {
			return
		}
		if len(args) == 0 {
			continue
		}

		cmd := strings.ToUpper(string(args[0]))
		switch cmd {
		case "PING":
			_ = writeRESPSimple(wr, "PONG")

		case "JOIN":
			if len(args) < 2 {
				_ = writeRESPError(wr, "ERR wrong number of arguments for 'join' command")
				_ = wr.Flush()
				continue
			}
			var req JoinRequest
			if err := json.Unmarshal(args[1], &req); err != nil {
				_ = writeRESPError(wr, fmt.Sprintf("ERR invalid join payload: %v", err))
				_ = wr.Flush()
				continue
			}
			resp, err := s.handler.HandleJoin(&req)
			if err != nil {
				resp = &JoinResponse{Error: err.Error()}
			}
			respBytes, _ := json.Marshal(resp)
			_ = writeRESPBulk(wr, respBytes)

		case "APPLY":
			if len(args) < 2 {
				_ = writeRESPError(wr, "ERR wrong number of arguments for 'apply' command")
				_ = wr.Flush()
				continue
			}
			var req ApplyRequest
			if err := json.Unmarshal(args[1], &req); err != nil {
				_ = writeRESPError(wr, fmt.Sprintf("ERR invalid apply payload: %v", err))
				_ = wr.Flush()
				continue
			}
			resp, err := s.handler.HandleApply(&req)
			if err != nil {
				resp = &ApplyResponse{Error: err.Error()}
			}
			respBytes, _ := json.Marshal(resp)
			_ = writeRESPBulk(wr, respBytes)

		default:
			_ = writeRESPError(wr, fmt.Sprintf("ERR unknown command '%s'", cmd))
		}

		if err := wr.Flush(); err != nil {
			return
		}
	}
}

// ----------------------------------------------------------------------------
// RESP Connection Pool & Client
// ----------------------------------------------------------------------------

type pooledConn struct {
	net.Conn
	rd       *bufio.Reader
	wr       *bufio.Writer
	lastUsed time.Time
	pool     *targetPool
}

type targetPool struct {
	addr        string
	conns       chan *pooledConn
	mu          sync.Mutex
	closed      bool
	dialTimeout time.Duration
	idleTimeout time.Duration
}

func (p *targetPool) get(ctx context.Context) (*pooledConn, error) {
	for {
		select {
		case pc, ok := <-p.conns:
			if !ok {
				return nil, errors.New("connection pool closed")
			}
			if time.Since(pc.lastUsed) > p.idleTimeout {
				_ = pc.Conn.Close()
				continue
			}
			return pc, nil
		default:
			return p.dial(ctx)
		}
	}
}

func (p *targetPool) dial(ctx context.Context) (*pooledConn, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, errors.New("connection pool closed")
	}
	p.mu.Unlock()

	d := net.Dialer{Timeout: p.dialTimeout}
	c, err := d.DialContext(ctx, "tcp", p.addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", p.addr, err)
	}

	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
		_ = tc.SetKeepAlive(true)
		_ = tc.SetKeepAlivePeriod(30 * time.Second)
	}

	return &pooledConn{
		Conn:     c,
		rd:       bufio.NewReader(c),
		wr:       bufio.NewWriter(c),
		lastUsed: time.Now(),
		pool:     p,
	}, nil
}

func (p *targetPool) put(pc *pooledConn) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		_ = pc.Conn.Close()
		return
	}
	p.mu.Unlock()

	pc.lastUsed = time.Now()
	select {
	case p.conns <- pc:
	default:
		// Pool is full, close surplus connection
		_ = pc.Conn.Close()
	}
}

func (p *targetPool) discard(pc *pooledConn) {
	_ = pc.Conn.Close()
}

func (p *targetPool) close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.mu.Unlock()

	close(p.conns)
	for pc := range p.conns {
		_ = pc.Conn.Close()
	}
}

// RPCClient communicates with other nodes using pooled RESP persistent connections.
type RPCClient struct {
	mu          sync.RWMutex
	pools       map[string]*targetPool
	maxIdle     int
	dialTimeout time.Duration
	idleTimeout time.Duration
	closed      bool
}

// NewRPCClient creates a new pooled RESP RPC client.
func NewRPCClient() *RPCClient {
	return &RPCClient{
		pools:       make(map[string]*targetPool),
		maxIdle:     32,
		dialTimeout: 5 * time.Second,
		idleTimeout: 60 * time.Second,
	}
}

// Close closes all pooled connections across all targets.
func (c *RPCClient) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	pools := make([]*targetPool, 0, len(c.pools))
	for _, p := range c.pools {
		pools = append(pools, p)
	}
	c.pools = make(map[string]*targetPool)
	c.mu.Unlock()

	for _, p := range pools {
		p.close()
	}
	return nil
}

func (c *RPCClient) getPool(addr string) (*targetPool, error) {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return nil, errors.New("rpc client closed")
	}
	pool, ok := c.pools[addr]
	c.mu.RUnlock()
	if ok {
		return pool, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, errors.New("rpc client closed")
	}
	pool, ok = c.pools[addr]
	if ok {
		return pool, nil
	}

	pool = &targetPool{
		addr:        addr,
		conns:       make(chan *pooledConn, c.maxIdle),
		dialTimeout: c.dialTimeout,
		idleTimeout: c.idleTimeout,
	}
	c.pools[addr] = pool
	return pool, nil
}

func (c *RPCClient) executeOnConn(ctx context.Context, pc *pooledConn, args ...[]byte) ([]byte, error) {
	if d, ok := ctx.Deadline(); ok {
		_ = pc.SetDeadline(d)
	} else {
		_ = pc.SetDeadline(time.Now().Add(30 * time.Second))
	}
	defer pc.SetDeadline(time.Time{})

	if err := writeRESPCmd(pc.wr, args...); err != nil {
		return nil, err
	}
	if err := pc.wr.Flush(); err != nil {
		return nil, err
	}

	return readRESPResponse(pc.rd)
}

func (c *RPCClient) do(ctx context.Context, targetAddr string, args ...[]byte) ([]byte, error) {
	pool, err := c.getPool(targetAddr)
	if err != nil {
		return nil, err
	}

	pc, err := pool.get(ctx)
	if err != nil {
		return nil, err
	}

	res, err := c.executeOnConn(ctx, pc, args...)
	if err == nil {
		pool.put(pc)
		return res, nil
	}

	// Discard stale or failed connection
	pool.discard(pc)

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// Stale connection recovery: retry once with a guaranteed fresh connection
	freshConn, err := pool.dial(ctx)
	if err != nil {
		return nil, err
	}

	res, err = c.executeOnConn(ctx, freshConn, args...)
	if err != nil {
		pool.discard(freshConn)
		return nil, err
	}

	pool.put(freshConn)
	return res, nil
}

// Ping checks whether the target node's RPC server is reachable and alive.
func (c *RPCClient) Ping(ctx context.Context, targetAddr string) error {
	res, err := c.do(ctx, targetAddr, []byte("PING"))
	if err != nil {
		return err
	}
	if string(res) != "PONG" {
		return fmt.Errorf("unexpected ping response: %s", string(res))
	}
	return nil
}

// JoinNode sends a join request to a remote node address.
func (c *RPCClient) JoinNode(ctx context.Context, targetAddr string, node *Node) (*JoinResponse, error) {
	reqBody, err := json.Marshal(&JoinRequest{Node: node})
	if err != nil {
		return nil, err
	}

	respBytes, err := c.do(ctx, targetAddr, []byte("JOIN"), reqBody)
	if err != nil {
		return nil, err
	}

	var resp JoinResponse
	if err := json.Unmarshal(respBytes, &resp); err != nil {
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

	respBytes, err := c.do(ctx, targetAddr, []byte("APPLY"), reqBody)
	if err != nil {
		return nil, err
	}

	var resp ApplyResponse
	if err := json.Unmarshal(respBytes, &resp); err != nil {
		return nil, fmt.Errorf("decode apply response: %w", err)
	}
	return &resp, nil
}

// ----------------------------------------------------------------------------
// RESP Protocol Low-Level Framing
// ----------------------------------------------------------------------------

func readRESPCommand(r *bufio.Reader) ([][]byte, error) {
	b, err := r.Peek(1)
	if err != nil {
		return nil, err
	}

	if b[0] == '*' {
		line, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		line = strings.TrimRight(line, "\r\n")
		count, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, fmt.Errorf("invalid resp array header: %w", err)
		}
		if count <= 0 {
			return nil, nil
		}
		args := make([][]byte, count)
		for i := 0; i < count; i++ {
			arg, err := readRESPBulkString(r)
			if err != nil {
				return nil, err
			}
			args[i] = arg
		}
		return args, nil
	}

	// Support inline commands (e.g. "PING\r\n")
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return nil, nil
	}
	args := make([][]byte, len(fields))
	for i, f := range fields {
		args[i] = []byte(f)
	}
	return args, nil
}

func readRESPBulkString(r *bufio.Reader) ([]byte, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if len(line) == 0 || line[0] != '$' {
		return nil, fmt.Errorf("expected '$' for bulk string, got: %q", line)
	}
	length, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, fmt.Errorf("invalid bulk string length: %w", err)
	}
	if length == -1 {
		return nil, nil
	}
	if length < 0 {
		return nil, fmt.Errorf("negative bulk string length: %d", length)
	}

	data := make([]byte, length+2)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}
	return data[:length], nil
}

func readRESPResponse(r *bufio.Reader) ([]byte, error) {
	prefix, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	switch prefix {
	case '+': // Simple string
		line, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		return []byte(strings.TrimRight(line, "\r\n")), nil

	case '-': // Error
		line, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		return nil, errors.New(strings.TrimRight(line, "\r\n"))

	case ':': // Integer
		line, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		return []byte(strings.TrimRight(line, "\r\n")), nil

	case '$': // Bulk string
		line, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		line = strings.TrimRight(line, "\r\n")
		length, err := strconv.Atoi(line)
		if err != nil {
			return nil, fmt.Errorf("invalid bulk length: %w", err)
		}
		if length == -1 {
			return nil, nil
		}
		if length < 0 {
			return nil, fmt.Errorf("negative bulk length: %d", length)
		}
		data := make([]byte, length+2)
		if _, err := io.ReadFull(r, data); err != nil {
			return nil, err
		}
		return data[:length], nil

	default:
		return nil, fmt.Errorf("unexpected resp prefix: %q", prefix)
	}
}

func writeRESPCmd(w io.Writer, args ...[]byte) error {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("*%d\r\n", len(args)))
	for _, arg := range args {
		buf.WriteString(fmt.Sprintf("$%d\r\n", len(arg)))
		buf.Write(arg)
		buf.WriteString("\r\n")
	}
	_, err := w.Write(buf.Bytes())
	return err
}

func writeRESPBulk(w io.Writer, data []byte) error {
	if data == nil {
		_, err := w.Write([]byte("$-1\r\n"))
		return err
	}
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("$%d\r\n", len(data)))
	buf.Write(data)
	buf.WriteString("\r\n")
	_, err := w.Write(buf.Bytes())
	return err
}

func writeRESPSimple(w io.Writer, s string) error {
	_, err := fmt.Fprintf(w, "+%s\r\n", s)
	return err
}

func writeRESPError(w io.Writer, errStr string) error {
	_, err := fmt.Fprintf(w, "-%s\r\n", errStr)
	return err
}
