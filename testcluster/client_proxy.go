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
	listener      net.Listener
	target        string
	mu            sync.Mutex
	blocked       bool
	closed        bool
	responsesHeld chan struct{}
	active        map[net.Conn]net.Conn
	acceptDone    chan struct{}
	sessions      sync.WaitGroup
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

func (p *ClientProxy) waitResponses() {
	p.mu.Lock()
	held := p.responsesHeld
	p.mu.Unlock()
	if held != nil {
		<-held
	}
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
		p.sessions.Add(1)
		p.mu.Unlock()
		go p.bridge(client, upstream)
	}
}

func (p *ClientProxy) bridge(client, upstream net.Conn) {
	defer p.sessions.Done()
	copyDone := make(chan struct{})
	go func() {
		_, _ = io.Copy(upstream, client)
		close(copyDone)
		_ = upstream.Close()
		_ = client.Close()
	}()
	buf := make([]byte, 32*1024)
	for {
		n, readErr := upstream.Read(buf)
		if n > 0 {
			p.waitResponses()
			for offset := 0; offset < n; {
				written, writeErr := client.Write(buf[offset:n])
				offset += written
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
