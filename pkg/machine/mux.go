package machine

import (
	"bufio"
	"io"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

// MuxListener multiplexes a single TCP listening port between Raft binary RPCs and HTTP RPCs.
type MuxListener struct {
	ln        net.Listener
	raftLn    *chanListener
	httpLn    *chanListener
	closed    chan struct{}
	closeOnce sync.Once
}

type chanListener struct {
	addr    net.Addr
	ch      chan net.Conn
	closed  chan struct{}
	closeFn func() error
}

func (c *chanListener) Accept() (net.Conn, error) {
	select {
	case conn, ok := <-c.ch:
		if !ok {
			return nil, net.ErrClosed
		}
		return conn, nil
	case <-c.closed:
		return nil, net.ErrClosed
	}
}

func (c *chanListener) Close() error {
	return c.closeFn()
}

func (c *chanListener) Addr() net.Addr {
	return c.addr
}

// MuxStreamLayer implements raft.StreamLayer over the multiplexed Raft listener.
type MuxStreamLayer struct {
	*chanListener
}

func (s *MuxStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", string(address), timeout)
}

type prefixConn struct {
	net.Conn
	r io.Reader
}

func (p *prefixConn) Read(b []byte) (int, error) {
	return p.r.Read(b)
}

// NewMuxListener binds to bindAddr and returns a multiplexer, a raft.StreamLayer, and an HTTP net.Listener.
func NewMuxListener(bindAddr string) (*MuxListener, *MuxStreamLayer, net.Listener, error) {
	return NewMuxListenerWithAdvertise(bindAddr, "")
}

// NewMuxListenerWithAdvertise binds to bindAddr and returns a multiplexer whose Raft stream layer advertises advertiseAddr.
func NewMuxListenerWithAdvertise(bindAddr string, advertiseAddr string) (*MuxListener, *MuxStreamLayer, net.Listener, error) {
	ln, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return nil, nil, nil, err
	}

	m := &MuxListener{
		ln:     ln,
		closed: make(chan struct{}),
	}

	raftCh := make(chan net.Conn, 256)
	httpCh := make(chan net.Conn, 256)

	advAddr := ln.Addr()
	if advertiseAddr != "" {
		if tcpAddr, err := net.ResolveTCPAddr("tcp", advertiseAddr); err == nil {
			advAddr = tcpAddr
		}
	}

	m.raftLn = &chanListener{
		addr:    advAddr,
		ch:      raftCh,
		closed:  m.closed,
		closeFn: m.Close,
	}

	m.httpLn = &chanListener{
		addr:    advAddr,
		ch:      httpCh,
		closed:  m.closed,
		closeFn: m.Close,
	}

	go m.serve()

	return m, &MuxStreamLayer{chanListener: m.raftLn}, m.httpLn, nil
}

func (m *MuxListener) Addr() net.Addr {
	return m.ln.Addr()
}

func (m *MuxListener) Close() error {
	m.closeOnce.Do(func() {
		close(m.closed)
		_ = m.ln.Close()
	})
	return nil
}

func (m *MuxListener) serve() {
	for {
		conn, err := m.ln.Accept()
		if err != nil {
			select {
			case <-m.closed:
				return
			default:
				time.Sleep(10 * time.Millisecond)
				continue
			}
		}
		go m.route(conn)
	}
}

func (m *MuxListener) route(conn net.Conn) {
	br := bufio.NewReader(conn)
	firstByte, err := br.Peek(1)
	if err != nil {
		_ = conn.Close()
		return
	}

	pConn := &prefixConn{
		Conn: conn,
		r:    br,
	}

	// ASCII printable characters (e.g. 'P', 'G', 'H', 'O' >= 32) indicate HTTP request
	if firstByte[0] >= 32 {
		select {
		case m.httpLn.ch <- pConn:
		case <-m.closed:
			_ = conn.Close()
		}
	} else {
		// Binary byte indicates HashiCorp Raft RPC (0, 1, 2, 3)
		select {
		case m.raftLn.ch <- pConn:
		case <-m.closed:
			_ = conn.Close()
		}
	}
}
