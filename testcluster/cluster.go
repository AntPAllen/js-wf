// Package testcluster boots real in-process NATS JetStream nodes for tests.
package testcluster

import (
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

type Cluster struct {
	Servers    []*server.Server
	Clients    []*nats.Conn
	root       string
	ports      []int
	routes     []int
	routeMesh  *RouteMesh
	serverTags map[int][]string
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// Start boots count nodes with separate file stores and pinned clients.
func Start(root string, count int) (*Cluster, error) {
	return start(root, count, false, nil)
}

// StartWithServerTags permits documented JetStream placement constraints in
// native tests. Tags are copied before starting the servers.
func StartWithServerTags(root string, count int, tags map[int][]string) (*Cluster, error) {
	return start(root, count, false, tags)
}

// StartPartitionable routes every inter-server connection through a RouteMesh.
// It is intended for route-partition tests, not throughput measurements.
func StartPartitionable(root string, count int) (*Cluster, error) {
	return start(root, count, true, nil)
}

func start(root string, count int, partitionable bool, tags map[int][]string) (*Cluster, error) {
	if count < 1 || count > 3 {
		return nil, fmt.Errorf("count must be 1..3")
	}
	c := &Cluster{root: root, serverTags: make(map[int][]string, len(tags))}
	for node, values := range tags {
		if node < 0 || node >= count {
			return nil, fmt.Errorf("server tag node %d out of range", node)
		}
		for _, value := range values {
			if value == "" {
				return nil, fmt.Errorf("empty server tag")
			}
		}
		c.serverTags[node] = append([]string(nil), values...)
	}
	ports := make([]int, count)
	routes := make([]int, count)
	usedPorts := make(map[int]struct{}, 2*count)
	uniquePort := func() (int, error) {
		for attempt := 0; attempt < 20; attempt++ {
			port, err := freePort()
			if err != nil {
				return 0, err
			}
			if _, used := usedPorts[port]; !used {
				usedPorts[port] = struct{}{}
				return port, nil
			}
		}
		return 0, fmt.Errorf("could not allocate distinct NATS test ports")
	}
	for i := range ports {
		var err error
		ports[i], err = uniquePort()
		if err != nil {
			return nil, err
		}
		if count > 1 {
			routes[i], err = uniquePort()
			if err != nil {
				return nil, err
			}
		}
	}
	c.ports, c.routes = ports, routes
	if partitionable && count > 1 {
		mesh, err := newRouteMesh(routes, append(append([]int(nil), ports...), routes...))
		if err != nil {
			return nil, err
		}
		c.routeMesh = mesh
	}
	for i := 0; i < count; i++ {
		s, err := server.NewServer(c.options(i))
		if err != nil {
			c.Close()
			return nil, err
		}
		c.Servers = append(c.Servers, s)
		go s.Start()
	}
	for i, s := range c.Servers {
		if !s.ReadyForConnections(5 * time.Second) {
			c.Close()
			return nil, fmt.Errorf("node %d not ready (running=%v, url=%s)", i, s.Running(), s.ClientURL())
		}
		nc, err := nats.Connect(s.ClientURL(), nats.NoReconnect())
		if err != nil {
			c.Close()
			return nil, err
		}
		c.Clients = append(c.Clients, nc)
	}
	return c, nil
}

func (c *Cluster) RouteMesh() *RouteMesh { return c.routeMesh }

// ApplyFault connects a recorded schedule to the faults supported by this
// in-process fixture. Pause and disk-delay faults need a separate mechanism.
func (c *Cluster) ApplyFault(event FaultEvent) error {
	if event.A < 0 || event.A >= len(c.Servers) {
		return fmt.Errorf("fault node %d out of range", event.A)
	}
	switch event.Op {
	case KillNode:
		c.KillNode(event.A)
		return nil
	case PartitionNodes:
		if c.routeMesh == nil {
			return fmt.Errorf("route partition requires StartPartitionable")
		}
		return c.routeMesh.PartitionNodes(event.A, event.B)
	default:
		return fmt.Errorf("fault verb %s is not supported by this fixture", event.Op)
	}
}

func (c *Cluster) options(i int) *server.Options {
	opts := &server.Options{Host: "127.0.0.1", Port: c.ports[i], JetStream: true, StoreDir: filepath.Join(c.root, fmt.Sprintf("node-%d", i)), NoLog: true, NoSigs: true, ServerName: fmt.Sprintf("wf-test-%d", i)}
	opts.Tags.Add(c.serverTags[i]...)
	if len(c.ports) > 1 {
		opts.Accounts = []*server.Account{server.NewAccount("$SYS"), server.NewAccount("$G")}
		opts.SystemAccount = "$SYS"
		opts.Cluster = server.ClusterOpts{Name: "wf-test", Host: "127.0.0.1", Port: c.routes[i]}
		if c.routeMesh != nil {
			opts.Cluster.Advertise = c.routeMesh.address(i)
			opts.Cluster.PoolSize = -1
		}
		peer := 0
		if i == 0 {
			peer = 1
		}
		peerAddress := fmt.Sprintf("127.0.0.1:%d", c.routes[peer])
		if c.routeMesh != nil {
			peerAddress = c.routeMesh.address(peer)
		}
		u, _ := url.Parse("nats://" + peerAddress)
		opts.Routes = []*url.URL{u}
	}
	return opts
}

func (c *Cluster) KillNode(i int) {
	if i >= 0 && i < len(c.Servers) && c.Servers[i] != nil {
		c.Servers[i].Shutdown()
		c.Servers[i].WaitForShutdown()
	}
}

// RestartNode reopens a stopped node on its original ports and file store.
// The old pinned client is replaced with a new connection to that node.
func (c *Cluster) RestartNode(i int) error {
	if i < 0 || i >= len(c.Servers) || c.Servers[i] == nil || c.Servers[i].Running() {
		return fmt.Errorf("node %d is not stopped", i)
	}
	s, err := server.NewServer(c.options(i))
	if err != nil {
		return err
	}
	go s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		s.Shutdown()
		s.WaitForShutdown()
		return fmt.Errorf("restarted node %d not ready", i)
	}
	nc, err := nats.Connect(s.ClientURL(), nats.NoReconnect())
	if err != nil {
		s.Shutdown()
		s.WaitForShutdown()
		return err
	}
	c.Clients[i].Close()
	c.Servers[i], c.Clients[i] = s, nc
	return nil
}

func (c *Cluster) Close() {
	if c.routeMesh != nil {
		c.routeMesh.Close()
	}
	for _, nc := range c.Clients {
		nc.Close()
	}
	for _, s := range c.Servers {
		s.Shutdown()
	}
	for _, s := range c.Servers {
		s.WaitForShutdown()
	}
}
