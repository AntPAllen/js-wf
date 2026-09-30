package testcluster

import (
	"fmt"
	"io"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// ClientProxy is a TCP relay for one NATS client connection. Block cuts active
// connections and refuses reconnects; Heal accepts reconnects again. It is
// suitable for isolating a worker or SDK client from a running cluster.
type ClientProxy struct {
	listener       net.Listener
	target         string
	mu             sync.Mutex
	blocked        bool
	closed         bool
	responsesHeld  chan struct{}
	active         map[net.Conn]net.Conn
	acceptDone     chan struct{}
	sessions       sync.WaitGroup
	clientBytes    uint64
	serverBytes    uint64
	heldBytes      uint64
	nextConnection uint64
	trafficLimit   int
	trafficUsed    int
	traffic        ClientProxyTraffic
}

type ClientProxyTraffic struct {
	Connections []ClientProxyConnection `json:"connections"`
	Frames      []ClientProxyFrame      `json:"frames"`
	Truncated   bool                    `json:"truncated"`
}

type ClientProxyConnection struct {
	ID       uint64    `json:"id"`
	Client   string    `json:"client"`
	Upstream string    `json:"upstream"`
	Target   string    `json:"target"`
	At       time.Time `json:"at"`
}

type ClientProxyFrame struct {
	Connection uint64    `json:"connection"`
	Direction  string    `json:"direction"`
	Data       []byte    `json:"data"`
	At         time.Time `json:"at"`
}

// EnableTrafficTrace captures the connection prefix, including handshake and
// successfully forwarded bytes. Enable before connecting. The limit accounts
// for payload and record overhead; truncation explicitly invalidates a claim
// that the retained prefix is a complete connection transcript.
func (p *ClientProxy) EnableTrafficTrace(limit int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.nextConnection != 0 || p.trafficLimit != 0 || limit < 1024 || limit > 16<<20 {
		return fmt.Errorf("traffic trace requires an unused proxy and a 1 KiB..16 MiB budget")
	}
	p.trafficLimit = limit
	return nil
}

func (p *ClientProxy) TrafficTrace() ClientProxyTraffic {
	p.mu.Lock()
	defer p.mu.Unlock()
	trace := p.traffic
	trace.Connections = append([]ClientProxyConnection(nil), trace.Connections...)
	trace.Frames = append([]ClientProxyFrame(nil), trace.Frames...)
	for i := range trace.Frames {
		trace.Frames[i].Data = append([]byte(nil), trace.Frames[i].Data...)
	}
	return trace
}

func (p *ClientProxy) recordTrafficLocked(connection uint64, direction string, data []byte) {
	if p.trafficLimit == 0 || p.traffic.Truncated || len(data) == 0 {
		return
	}
	cost := len(data) + 128
	if p.trafficUsed+cost > p.trafficLimit {
		p.traffic.Truncated = true
		return
	}
	p.trafficUsed += cost
	p.traffic.Frames = append(p.traffic.Frames, ClientProxyFrame{connection, direction, append([]byte(nil), data...), time.Now().UTC()})
}

func NewClientProxy(targetURL string) (*ClientProxy, error) {
	target, err := natsURLAddress(targetURL)
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &ClientProxy{listener: listener, target: target, active: map[net.Conn]net.Conn{}, acceptDone: make(chan struct{})}
	go p.accept()
	return p, nil
}

func natsURLAddress(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "nats" || u.Host == "" {
		return "", fmt.Errorf("invalid NATS target URL %q", raw)
	}
	return u.Host, nil
}

func (p *ClientProxy) URL() string { return "nats://" + p.listener.Addr().String() }

// Connect returns a reconnecting NATS connection pinned to this proxy.
func (p *ClientProxy) Connect() (*nats.Conn, error) {
	return nats.Connect(p.URL(), nats.IgnoreDiscoveredServers(), nats.ReconnectWait(50*time.Millisecond), nats.MaxReconnects(-1))
}

func (p *ClientProxy) Block() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.blocked = true
	for client, upstream := range p.active {
		_ = client.Close()
		_ = upstream.Close()
	}
	p.resumeResponsesLocked()
}

func (p *ClientProxy) Heal() {
	p.mu.Lock()
	p.blocked = false
	p.mu.Unlock()
}

// HoldResponses stops server-to-client bytes after they reach the proxy while
// still forwarding client-to-server bytes. Call after Connect has completed.
func (p *ClientProxy) HoldResponses() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.responsesHeld == nil {
		p.responsesHeld = make(chan struct{})
	}
}

func (p *ClientProxy) ResumeResponses() {
	p.mu.Lock()
	p.resumeResponsesLocked()
	p.mu.Unlock()
}

func (p *ClientProxy) resumeResponsesLocked() {
	if p.responsesHeld != nil {
		close(p.responsesHeld)
		p.responsesHeld = nil
	}
}

// ClientProxyStats are byte counts at the relay boundaries, including bytes
// awaiting release during an asymmetric server-to-client fault.
type ClientProxyStats struct {
	ClientToServer uint64 `json:"client_to_server"`
	ServerToClient uint64 `json:"server_to_client"`
	HeldBytes      uint64 `json:"held_bytes"`
	ResponsesHeld  bool   `json:"responses_held"`
	Active         int    `json:"active_connections"`
}

func (p *ClientProxy) Stats() ClientProxyStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return ClientProxyStats{p.clientBytes, p.serverBytes, p.heldBytes, p.responsesHeld != nil, len(p.active)}
}
func (p *ClientProxy) waitResponses(bytes int) {
	p.mu.Lock()
	held := p.responsesHeld
	if held != nil {
		p.heldBytes += uint64(bytes)
	}
	p.mu.Unlock()
	if held != nil {
		<-held
	}
}

type clientProxyWriter struct {
	proxy      *ClientProxy
	upstream   net.Conn
	connection uint64
}

func (w clientProxyWriter) Write(data []byte) (int, error) {
	n, err := w.upstream.Write(data)
	w.proxy.mu.Lock()
	w.proxy.clientBytes += uint64(n)
	w.proxy.recordTrafficLocked(w.connection, "client_to_server", data[:n])
	w.proxy.mu.Unlock()
	return n, err
}

func (p *ClientProxy) Close() {
	p.mu.Lock()
	p.closed = true
	p.blocked = true
	_ = p.listener.Close()
	for client, upstream := range p.active {
		_ = client.Close()
		_ = upstream.Close()
	}
	p.resumeResponsesLocked()
	p.mu.Unlock()
	<-p.acceptDone
	p.sessions.Wait()
}

func (p *ClientProxy) accept() {
	defer close(p.acceptDone)
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		blocked := p.blocked || p.closed
		p.mu.Unlock()
		if blocked {
			_ = client.Close()
			continue
		}
		upstream, err := net.DialTimeout("tcp", p.target, time.Second)
		if err != nil {
			_ = client.Close()
			continue
		}
		p.mu.Lock()
		if p.blocked || p.closed {
			p.mu.Unlock()
			_ = client.Close()
			_ = upstream.Close()
			continue
		}
		p.active[client] = upstream
		p.nextConnection++
		connection := p.nextConnection
		if p.trafficLimit != 0 && !p.traffic.Truncated {
			if p.trafficUsed+1024 > p.trafficLimit {
				p.traffic.Truncated = true
			} else {
				p.trafficUsed += 1024
				p.traffic.Connections = append(p.traffic.Connections, ClientProxyConnection{connection, client.RemoteAddr().String(), upstream.LocalAddr().String(), upstream.RemoteAddr().String(), time.Now().UTC()})
			}
		}
		p.sessions.Add(1)
		p.mu.Unlock()
		go p.bridge(client, upstream, connection)
	}
}

func (p *ClientProxy) bridge(client, upstream net.Conn, connection uint64) {
	defer p.sessions.Done()
	copyDone := make(chan struct{})
	go func() {
		_, _ = io.Copy(clientProxyWriter{p, upstream, connection}, client)
		close(copyDone)
		_ = upstream.Close()
		_ = client.Close()
	}()
	buf := make([]byte, 32*1024)
	for {
		n, readErr := upstream.Read(buf)
		if n > 0 {
			p.waitResponses(n)
			for offset := 0; offset < n; {
				start := offset
				written, writeErr := client.Write(buf[offset:n])
				offset += written
				p.mu.Lock()
				p.serverBytes += uint64(written)
				p.recordTrafficLocked(connection, "server_to_client", buf[start:offset])
				p.mu.Unlock()
				if written == 0 && writeErr == nil {
					writeErr = io.ErrShortWrite
				}
				if writeErr != nil {
					readErr = writeErr
					break
				}
			}
		}
		if readErr != nil {
			break
		}
	}
	_ = client.Close()
	_ = upstream.Close()
	<-copyDone
	p.mu.Lock()
	delete(p.active, client)
	p.mu.Unlock()
}
