package testcluster

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// RouteMesh relays all advertised and solicited cluster routes. Blocking a
// node severs both its incoming and outgoing links while other nodes remain
// connected. This is a transport fault: the NATS servers keep running.
type RouteMesh struct {
	mu          sync.Mutex
	blocked     int
	blockedPair [2]int
	closed      bool
	proxies     []*routeProxy
	sessions    map[*routeSession]struct{}
}

type routeProxy struct {
	mesh     *RouteMesh
	node     int
	listener net.Listener
	target   string
	done     chan struct{}
}

type routeSession struct {
	from     int
	to       int
	client   net.Conn
	upstream net.Conn
}

func newRouteMesh(routePorts, reservedPorts []int) (*RouteMesh, error) {
	m := &RouteMesh{blocked: -1, blockedPair: [2]int{-1, -1}, sessions: make(map[*routeSession]struct{})}
	reserved := make(map[int]bool, len(reservedPorts))
	for _, port := range reservedPorts {
		reserved[port] = true
	}
	for i, port := range routePorts {
		var listener net.Listener
		var err error
		for attempt := 0; attempt < 100; attempt++ {
			listener, err = net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				break
			}
			proxyPort := listener.Addr().(*net.TCPAddr).Port
			if !reserved[proxyPort] {
				break
			}
			_ = listener.Close()
			listener = nil
		}
		if err == nil && listener == nil {
			err = fmt.Errorf("could not allocate route proxy port outside reserved server ports")
		}
		if err != nil {
			for _, opened := range m.proxies {
				_ = opened.listener.Close()
			}
			return nil, err
		}
		p := &routeProxy{mesh: m, node: i, listener: listener, target: fmt.Sprintf("127.0.0.1:%d", port), done: make(chan struct{})}
		m.proxies = append(m.proxies, p)
	}
	for _, p := range m.proxies {
		go p.accept()
	}
	return m, nil
}

func (m *RouteMesh) address(node int) string { return m.proxies[node].listener.Addr().String() }

// PartitionNode isolates node from every other node; clients remain attached.
func (m *RouteMesh) PartitionNode(node int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || node < 0 || node >= len(m.proxies) {
		return fmt.Errorf("invalid route partition node %d", node)
	}
	m.blocked = node
	m.blockedPair = [2]int{-1, -1}
	for s := range m.sessions {
		if s.from < 0 || s.from == node || s.to == node {
			closeRouteSession(s)
		}
	}
	return nil
}

// PartitionNodes severs the direct route between two nodes. Routes through
// other nodes remain available; clients stay connected to their pinned nodes.
func (m *RouteMesh) PartitionNodes(a, b int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || a < 0 || b < 0 || a >= len(m.proxies) || b >= len(m.proxies) || a == b {
		return fmt.Errorf("invalid route partition pair %d,%d", a, b)
	}
	m.blocked = -1
	m.blockedPair = [2]int{a, b}
	for s := range m.sessions {
		if m.pairBlocked(s.from, s.to) {
			closeRouteSession(s)
		}
	}
	return nil
}

// pairBlocked is called with m.mu held.
func (m *RouteMesh) pairBlocked(a, b int) bool {
	return a == m.blockedPair[0] && b == m.blockedPair[1] || a == m.blockedPair[1] && b == m.blockedPair[0]
}

func (m *RouteMesh) Heal() {
	m.mu.Lock()
	m.blocked = -1
	m.blockedPair = [2]int{-1, -1}
	m.mu.Unlock()
}

func (m *RouteMesh) Close() {
	m.mu.Lock()
	m.closed = true
	for _, p := range m.proxies {
		_ = p.listener.Close()
	}
	for s := range m.sessions {
		closeRouteSession(s)
	}
	m.mu.Unlock()
	for _, p := range m.proxies {
		<-p.done
	}
}

func closeRouteSession(s *routeSession) {
	_ = s.client.Close()
	_ = s.upstream.Close()
}

func (p *routeProxy) accept() {
	defer close(p.done)
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		p.mesh.mu.Lock()
		blocked := p.mesh.closed || p.mesh.blocked == p.node
		p.mesh.mu.Unlock()
		if blocked {
			_ = client.Close()
			continue
		}
		upstream, err := net.DialTimeout("tcp", p.target, time.Second)
		if err != nil {
			_ = client.Close()
			continue
		}
		s := &routeSession{from: -1, to: p.node, client: client, upstream: upstream}
		p.mesh.mu.Lock()
		if p.mesh.closed || p.mesh.blocked == p.node {
			p.mesh.mu.Unlock()
			closeRouteSession(s)
			continue
		}
		p.mesh.sessions[s] = struct{}{}
		p.mesh.mu.Unlock()
		go p.bridge(s)
	}
}

func (p *routeProxy) bridge(s *routeSession) {
	defer func() {
		closeRouteSession(s)
		p.mesh.mu.Lock()
		delete(p.mesh.sessions, s)
		p.mesh.mu.Unlock()
	}()
	done := make(chan struct{})
	go func() {
		_, _ = io.Copy(s.client, s.upstream)
		close(done)
		_ = s.client.Close()
	}()
	reader := bufio.NewReader(s.client)
	_ = s.client.SetReadDeadline(time.Now().Add(5 * time.Second))
	from := -1
	for i := 0; i < 8; i++ {
		line, err := reader.ReadBytes('\n')
		if err != nil || len(line) > 16*1024 {
			return
		}
		if _, err := s.upstream.Write(line); err != nil {
			return
		}
		if !bytes.HasPrefix(line, []byte("INFO ")) {
			continue
		}
		var info struct {
			Name string `json:"server_name"`
		}
		if err := json.Unmarshal(bytes.TrimSpace(line[5:]), &info); err != nil {
			return
		}
		name := info.Name
		if strings.HasPrefix(name, "wf-test-") {
			name = strings.TrimPrefix(name, "wf-test-")
		} else if strings.HasPrefix(name, "wf-process-") {
			name = strings.TrimPrefix(name, "wf-process-")
		} else {
			return
		}
		from, err = strconv.Atoi(name)
		if err != nil || from < 0 || from >= len(p.mesh.proxies) || from == p.node {
			return
		}
		break
	}
	if from < 0 {
		return
	}
	p.mesh.mu.Lock()
	s.from = from
	blocked := p.mesh.closed || p.mesh.blocked == from || p.mesh.blocked == p.node || p.mesh.pairBlocked(from, p.node)
	p.mesh.mu.Unlock()
	if blocked {
		return
	}
	_ = s.client.SetReadDeadline(time.Time{})
	_, _ = io.Copy(s.upstream, reader)
	closeRouteSession(s)
	<-done
}
