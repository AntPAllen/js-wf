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
	listener             net.Listener
	target               string
	mu                   sync.Mutex
	blocked              bool
	closed               bool
	responsesHeld        chan struct{}
	active               map[net.Conn]net.Conn
	acceptDone           chan struct{}
	sessions             sync.WaitGroup
	clientBytes          uint64
	serverBytes          uint64
	replyBufferLimit     int
	bufferedBytes        uint64
	peakBufferedBytes    uint64
	bufferOverflows      uint64
	heldBytes            uint64
	nextConnection       uint64
	acceptedConnections  uint64
	upstreamDialFailures uint64
	trafficLimit         int
	trafficUsed          int
	traffic              ClientProxyTraffic
	apiBarrier           *clientAPIBarrier
	fileTrace            *clientProxyFileTrace
}

type ClientProxyTraffic struct {
	Connections    []ClientProxyConnection `json:"connections"`
	Frames         []ClientProxyFrame      `json:"frames"`
	Truncated      bool                    `json:"truncated"`
	FrameFile      string                  `json:"frame_file,omitempty"`
	FrameFileError string                  `json:"frame_file_error,omitempty"`
	FrameRecords   uint64                  `json:"frame_records,omitempty"`
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
	if p.closed || p.acceptedConnections != 0 || p.trafficLimit != 0 || limit < 1024 || limit > 16<<20 {
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
	if p.fileTrace != nil {
		p.recordFileTrafficLocked(connection, direction, data)
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
	p := &ClientProxy{listener: listener, target: target, replyBufferLimit: 8 << 20, active: map[net.Conn]net.Conn{}, acceptDone: make(chan struct{})}
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
// still forwarding client-to-server bytes and draining upstream into a bounded
// per-connection buffer. Overflow closes that connection and leaves a sticky
// failure counter; callers must reject such a fault as invalid fixture evidence.
// Call after Connect has completed.
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
	AcceptedConnections  uint64 `json:"accepted_connections"`
	UpstreamDialFailures uint64 `json:"upstream_dial_failures"`
	ClientToServer       uint64 `json:"client_to_server"`
	ServerToClient       uint64 `json:"server_to_client"`
	HeldBytes            uint64 `json:"held_bytes"`
	ResponsesHeld        bool   `json:"responses_held"`
	Active               int    `json:"active_connections"`
	BufferedBytes        uint64 `json:"buffered_bytes"`
	PeakBufferedBytes    uint64 `json:"peak_buffered_bytes"`
	BufferOverflows      uint64 `json:"buffer_overflows"`
}

func (p *ClientProxy) Stats() ClientProxyStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return ClientProxyStats{AcceptedConnections: p.acceptedConnections, UpstreamDialFailures: p.upstreamDialFailures, ClientToServer: p.clientBytes, ServerToClient: p.serverBytes, HeldBytes: p.heldBytes, ResponsesHeld: p.responsesHeld != nil, Active: len(p.active), BufferedBytes: p.bufferedBytes, PeakBufferedBytes: p.peakBufferedBytes, BufferOverflows: p.bufferOverflows}
}
func (p *ClientProxy) waitResponses(done <-chan struct{}) bool {
	p.mu.Lock()
	held := p.responsesHeld
	p.mu.Unlock()
	if held != nil {
		select {
		case <-held:
		case <-done:
			return false
		}
	}
	return true
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
	if !p.closed && p.apiBarrier != nil {
		close(p.apiBarrier.cancel)
	}
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
	p.closeFileTrace()
}

func (p *ClientProxy) accept() {
	defer close(p.acceptDone)
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		p.acceptedConnections++
		blocked := p.blocked || p.closed
		p.mu.Unlock()
		if blocked {
			_ = client.Close()
			continue
		}
		upstream, err := net.DialTimeout("tcp", p.target, time.Second)
		if err != nil {
			p.mu.Lock()
			p.upstreamDialFailures++
			p.mu.Unlock()
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
	sessionDone := make(chan struct{})
	go func() {
		p.mu.Lock()
		barrier := p.apiBarrier
		p.mu.Unlock()
		if barrier == nil {
			_, _ = io.Copy(clientProxyWriter{p, upstream, connection}, client)
		} else {
			_ = p.copyWithAPIBarrier(client, upstream, connection, sessionDone, barrier)
		}
		close(copyDone)
		_ = upstream.Close()
		_ = client.Close()
	}()
	// Keep reading upstream during the hold. Blocking Read here turns an
	// application reply fault into TCP backpressure and retransmission recovery.
	var queueMu sync.Mutex
	var queue [][]byte
	var queueCost int
	var readFinished bool
	notify := make(chan struct{}, 1)
	readerDone := make(chan struct{})
	wake := func() {
		select {
		case notify <- struct{}{}:
		default:
		}
	}
	go func() {
		defer close(readerDone)
		defer func() {
			queueMu.Lock()
			readFinished = true
			queueMu.Unlock()
			wake()
		}()
		buf := make([]byte, 32*1024)
		for {
			n, err := upstream.Read(buf)
			if n > 0 {
				queueMu.Lock()
				// Charge record overhead too, so tiny reads cannot escape the cap.
				if queueCost+n+64 > p.replyBufferLimit {
					queueMu.Unlock()
					p.mu.Lock()
					p.bufferOverflows++
					p.mu.Unlock()
					_ = upstream.Close()
					_ = client.Close()
					return
				}
				queue = append(queue, append([]byte(nil), buf[:n]...))
				queueCost += n + 64
				p.mu.Lock()
				p.bufferedBytes += uint64(n)
				if p.bufferedBytes > p.peakBufferedBytes {
					p.peakBufferedBytes = p.bufferedBytes
				}
				if p.responsesHeld != nil {
					p.heldBytes += uint64(n)
				}
				p.mu.Unlock()
				queueMu.Unlock()
				wake()
			}
			if err != nil {
				return
			}
		}
	}()
forward:
	for {
		if !p.waitResponses(copyDone) {
			break
		}
		queueMu.Lock()
		if len(queue) == 0 {
			finished := readFinished
			queueMu.Unlock()
			if finished {
				break
			}
			select {
			case <-notify:
			case <-copyDone:
				break forward
			}
			continue
		}
		data := queue[0]
		queue[0] = nil
		queue = queue[1:]
		queueCost -= len(data) + 64
		p.mu.Lock()
		p.bufferedBytes -= uint64(len(data))
		p.mu.Unlock()
		queueMu.Unlock()
		for offset := 0; offset < len(data); {
			start := offset
			written, err := client.Write(data[offset:])
			offset += written
			p.mu.Lock()
			p.serverBytes += uint64(written)
			p.recordTrafficLocked(connection, "server_to_client", data[start:offset])
			p.mu.Unlock()
			if err != nil || written == 0 {
				break forward
			}
		}
	}
	close(sessionDone)
	_ = client.Close()
	_ = upstream.Close()
	<-readerDone
	<-copyDone
	queueMu.Lock()
	p.mu.Lock()
	for _, data := range queue {
		p.bufferedBytes -= uint64(len(data))
	}
	delete(p.active, client)
	p.mu.Unlock()
	queueMu.Unlock()
}
