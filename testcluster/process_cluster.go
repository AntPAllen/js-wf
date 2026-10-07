//go:build !windows

package testcluster

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

// ProcessCluster runs real NATS server processes so one node can be stopped
// with SIGSTOP without pausing the Go test process or its other nodes.
type ProcessCluster struct {
	Commands  []*exec.Cmd
	Clients   []*nats.Conn
	root      string
	ports     []int
	routes    []int
	monitors  []int
	profiles  []int
	leafPorts []int
	logs      []string
	paused    []bool
	slowDisk  []*exec.Cmd
	routeMesh *RouteMesh
}

func (c *ProcessCluster) ClientURL(i int) string {
	return fmt.Sprintf("nats://127.0.0.1:%d", c.ports[i])
}

func (c *ProcessCluster) LogPath(i int) string { return c.logs[i] }

func (c *ProcessCluster) RouteMesh() *RouteMesh { return c.routeMesh }

// ApplyFault applies the process and route faults supported by this fixture.
func (c *ProcessCluster) ApplyFault(event FaultEvent) error {
	switch event.Op {
	case KillNode:
		return c.KillNode(event.A)
	case PauseNode:
		return c.PauseNode(event.A)
	case PartitionNodes:
		if c.routeMesh == nil {
			return fmt.Errorf("route partition requires StartPartitionableProcesses")
		}
		return c.routeMesh.PartitionNodes(event.A, event.B)
	case SlowDisk:
		return c.SlowDisk(event.A, time.Duration(event.LatencyMillis)*time.Millisecond)
	default:
		return fmt.Errorf("fault verb %s is not supported by the process fixture", event.Op)
	}
}

// StartProcesses builds the version of nats-server required by this module
// and boots up to three file-backed nodes with separate OS process IDs.
func StartProcesses(root string, count int) (_ *ProcessCluster, err error) {
	return startProcesses(root, count, false)
}

// StartProcessesWithDomain configures a real domain on every native server.
// RestartNode reuses its original configuration, ports and file stores.
func StartProcessesWithDomain(root string, count int, domain string) (*ProcessCluster, error) {
	if domain == "" || strings.ContainsAny(domain, "\r\n") {
		return nil, fmt.Errorf("invalid process JetStream domain")
	}
	return startProcessesWithDomain(root, count, false, nil, false, domain)
}

// StartLeafHubProcesses starts domain hubs with fixed loopback leaf listeners.
// Every restart retains the original listeners, routes, client ports and stores.
func StartLeafHubProcesses(root string, count int, domain string) (*ProcessCluster, error) {
	if domain == "" || strings.ContainsAny(domain, "\r\n") {
		return nil, fmt.Errorf("invalid leaf hub domain")
	}
	return startProcessesWithDiagnosticsAndLeaves(root, count, false, nil, false, domain, false, true)
}

func (c *ProcessCluster) LeafURLs() []*url.URL {
	urls := make([]*url.URL, len(c.leafPorts))
	for i, port := range c.leafPorts {
		urls[i] = &url.URL{Scheme: "nats", Host: fmt.Sprintf("127.0.0.1:%d", port)}
	}
	return urls
}

// StartLeafProcess starts a domain leaf connected to the supplied hub listeners.
// KillNode and RestartNode retain its client port, config and local store.
func StartLeafProcess(root, domain string, remotes []*url.URL) (*ProcessCluster, error) {
	if domain == "" || strings.ContainsAny(domain, "\r\n") || len(remotes) == 0 {
		return nil, fmt.Errorf("invalid leaf domain or remotes")
	}
	values := make([]string, len(remotes))
	for i, remote := range remotes {
		if remote == nil || remote.Scheme != "nats" || remote.Host == "" || remote.User != nil {
			return nil, fmt.Errorf("invalid leaf remote")
		}
		values[i] = strconv.Quote(remote.String())
	}
	return startProcessesWithDiagnostics(root, 1, false, nil, false, domain, false, "leafnodes { reconnect: 25ms, remotes: [{ urls: ["+strings.Join(values, ",")+"] }] }\n")
}

// StartProfiledProcesses enables NATS' loopback HTTP profiler for diagnostic
// stack capture. Sampling remains disabled; profiles are collected on demand.
func StartProfiledProcesses(root string, count int) (*ProcessCluster, error) {
	return startProcessesWithBinaries(root, count, false, nil, true)
}

// StartMixedVersionProcesses starts a three-node file-backed cluster with a
// chosen binary for each node. Empty paths use the server pinned by this
// module. It is intended for rolling-upgrade contract tests.
func StartMixedVersionProcesses(root string, binaries []string) (*ProcessCluster, error) {
	if len(binaries) != 3 {
		return nil, fmt.Errorf("mixed-version cluster needs three binaries")
	}
	return startProcessesWithBinaries(root, 3, false, binaries, false)
}

// StartMixedVersionProcessesWithDomain retains chosen server binaries and the
// same domain configuration across every RestartNode, including legacy peers.
func StartMixedVersionProcessesWithDomain(root string, binaries []string, domain string) (*ProcessCluster, error) {
	if len(binaries) != 3 || domain == "" || strings.ContainsAny(domain, "\r\n") {
		return nil, fmt.Errorf("mixed-version domain cluster needs three binaries and a valid domain")
	}
	return startProcessesWithDomain(root, 3, false, binaries, false, domain)
}

// StartClockSkewProcesses shifts one actual NATS process's Go wall clock.
// Callers must verify the running clocks through ServerNow before workloads.
func StartClockSkewProcesses(root string, count, node int, offset time.Duration) (*ProcessCluster, error) {
	if count < 1 || count > 3 || node < 0 || node >= count {
		return nil, fmt.Errorf("invalid clock-skew process node/count")
	}
	overlay, err := WriteClockOverlay(filepath.Join(root, "clock"), offset)
	if err != nil {
		return nil, err
	}
	binary := filepath.Join(root, "nats-server-skewed")
	build := exec.Command("go", "build", "-overlay="+overlay, "-o", binary, "github.com/nats-io/nats-server/v2")
	if output, err := build.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("build skewed process server: %w: %s", err, output)
	}
	binaries := make([]string, count)
	binaries[node] = binary
	return startProcessesWithBinaries(root, count, false, binaries, false)
}

// StartPartitionableProcesses routes all server links through a controllable
// relay while retaining separate server process IDs and file stores.
func StartPartitionableProcesses(root string, count int) (*ProcessCluster, error) {
	return startProcesses(root, count, true)
}

// StartPartitionableDebugProcesses enables the documented NATS -D option.
// It retains the same executable, stores, route relay and startup admission.
func StartPartitionableDebugProcesses(root string, count int) (*ProcessCluster, error) {
	return startProcessesWithDiagnostics(root, count, true, nil, false, "", true)
}

// StartPartitionableProcessesWithBinaries retains the route relay and startup
// admission while using an explicitly supplied executable for each of three
// peers. Callers capture the source and actual running executable identities.
func StartPartitionableProcessesWithBinaries(root string, binaries []string, debug bool) (*ProcessCluster, error) {
	if len(binaries) != 3 {
		return nil, fmt.Errorf("partitionable binary cluster needs three binaries")
	}
	return startProcessesWithDiagnostics(root, 3, true, binaries, false, "", debug)
}

func startProcesses(root string, count int, partitionable bool) (_ *ProcessCluster, err error) {
	return startProcessesWithBinaries(root, count, partitionable, nil, false)
}

func startProcessesWithBinaries(root string, count int, partitionable bool, binaries []string, profiling bool) (_ *ProcessCluster, err error) {
	return startProcessesWithDomain(root, count, partitionable, binaries, profiling, "")
}

func startProcessesWithDomain(root string, count int, partitionable bool, binaries []string, profiling bool, domain string) (*ProcessCluster, error) {
	return startProcessesWithDiagnostics(root, count, partitionable, binaries, profiling, domain, false)
}

func startProcessesWithDiagnostics(root string, count int, partitionable bool, binaries []string, profiling bool, domain string, debug bool, extraConfig ...string) (_ *ProcessCluster, err error) {
	return startProcessesWithDiagnosticsAndLeaves(root, count, partitionable, binaries, profiling, domain, debug, false, extraConfig...)
}

func startProcessesWithDiagnosticsAndLeaves(root string, count int, partitionable bool, binaries []string, profiling bool, domain string, debug, leafListeners bool, extraConfig ...string) (_ *ProcessCluster, err error) {
	if count < 1 || count > 3 {
		return nil, fmt.Errorf("count must be 1..3")
	}
	if err := os.MkdirAll(root, 0755); err != nil {
		return nil, err
	}
	binary := filepath.Join(root, "nats-server")
	build := exec.Command("go", "build", "-o", binary, "github.com/nats-io/nats-server/v2")
	if output, buildErr := build.CombinedOutput(); buildErr != nil {
		return nil, fmt.Errorf("build nats-server: %w: %s", buildErr, output)
	}
	c := &ProcessCluster{root: root, ports: make([]int, count), routes: make([]int, count), monitors: make([]int, count), profiles: make([]int, count), paused: make([]bool, count), slowDisk: make([]*exec.Cmd, count)}
	if leafListeners {
		c.leafPorts = make([]int, count)
	}
	defer func() {
		if err != nil {
			c.Close()
		}
	}()
	used := map[int]bool{}
	uniquePort := func() (int, error) {
		for attempt := 0; attempt < 20; attempt++ {
			port, portErr := freePort()
			if portErr != nil {
				return 0, portErr
			}
			if !used[port] {
				used[port] = true
				return port, nil
			}
		}
		return 0, fmt.Errorf("could not allocate distinct NATS process ports")
	}
	for i := 0; i < count; i++ {
		if leafListeners {
			c.leafPorts[i], err = uniquePort()
			if err != nil {
				return nil, err
			}
		}
		c.ports[i], err = uniquePort()
		if err != nil {
			return nil, err
		}
		c.monitors[i], err = uniquePort()
		if err != nil {
			return nil, err
		}
		if profiling {
			c.profiles[i], err = uniquePort()
			if err != nil {
				return nil, err
			}
		}
		if count > 1 {
			c.routes[i], err = uniquePort()
			if err != nil {
				return nil, err
			}
		}
	}
	if partitionable && count > 1 {
		reserved := append(append(append([]int(nil), c.ports...), c.monitors...), c.routes...)
		c.routeMesh, err = newRouteMesh(c.routes, reserved)
		if err != nil {
			return nil, err
		}
	}
	for i := 0; i < count; i++ {
		args := []string{"-a", "127.0.0.1", "-p", strconv.Itoa(c.ports[i]), "-m", strconv.Itoa(c.monitors[i]), "-n", fmt.Sprintf("wf-process-%d", i), "-js", "-sd", filepath.Join(root, fmt.Sprintf("node-%d", i))}
		if debug {
			args = append(args, "-D")
		}
		if domain != "" {
			config := filepath.Join(root, fmt.Sprintf("node-%d-domain.conf", i))
			contents := "jetstream { domain: " + strconv.Quote(domain) + " }\n" + strings.Join(extraConfig, "")
			if leafListeners {
				contents += fmt.Sprintf("leafnodes { listen: \"127.0.0.1:%d\" }\n", c.leafPorts[i])
			}
			if err := os.WriteFile(config, []byte(contents), 0600); err != nil {
				return nil, err
			}
			args = append(args, "-c", config)
		}
		if profiling {
			args = append(args, "--profile", strconv.Itoa(c.profiles[i]))
		}
		if count > 1 {
			peer := 0
			if i == 0 {
				peer = 1
			}
			peerAddress := fmt.Sprintf("127.0.0.1:%d", c.routes[peer])
			if c.routeMesh != nil {
				peerAddress = c.routeMesh.address(peer)
				args = append(args, "--cluster_advertise", c.routeMesh.address(i))
			}
			args = append(args, "--cluster", fmt.Sprintf("nats://127.0.0.1:%d", c.routes[i]), "--cluster_name", "wf-process", "--routes", "nats://"+peerAddress)
		}
		logPath := filepath.Join(root, fmt.Sprintf("node-%d.log", i))
		logFile, openErr := os.Create(logPath)
		if openErr != nil {
			return nil, openErr
		}
		nodeBinary := binary
		if len(binaries) > i && binaries[i] != "" {
			nodeBinary = binaries[i]
		}
		cmd := exec.Command(nodeBinary, args...)
		cmd.Stdout, cmd.Stderr = logFile, logFile
		startErr := cmd.Start()
		_ = logFile.Close()
		if startErr != nil {
			return nil, startErr
		}
		c.Commands = append(c.Commands, cmd)
		c.logs = append(c.logs, logPath)
	}
	for i := 0; i < count; i++ {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			conn, connectErr := nats.Connect(c.ClientURL(i), nats.Timeout(200*time.Millisecond), nats.NoReconnect())
			if connectErr == nil {
				c.Clients = append(c.Clients, conn)
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if len(c.Clients) <= i {
			logs, _ := os.ReadFile(c.logs[i])
			return nil, fmt.Errorf("process node %d did not accept clients: %s", i, strings.TrimSpace(string(logs)))
		}
	}
	return c, nil
}

// Diagnostic reads a bounded server stack or connection-state snapshot over
// HTTP, independently of the NATS client connection being investigated.
func (c *ProcessCluster) Diagnostic(ctx context.Context, node int, kind string) ([]byte, error) {
	if node < 0 || node >= len(c.monitors) {
		return nil, fmt.Errorf("invalid diagnostic node %d", node)
	}
	port, path := c.monitors[node], "/connz?subs=detail"
	switch kind {
	case "connections":
	case "leaf":
		path = "/leafz"
	case "jetstream":
		path = "/jsz?accounts=true&streams=true&consumers=true&raft=true"
	case "clock":
		path = "/varz"
	case "goroutines":
		if node >= len(c.profiles) || c.profiles[node] == 0 {
			return nil, fmt.Errorf("server profiling is not enabled")
		}
		port, path = c.profiles[node], "/debug/pprof/goroutine?debug=2"
	default:
		return nil, fmt.Errorf("unsupported diagnostic %q", kind)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d%s", port, path), nil)
	if err != nil {
		return nil, err
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server %s diagnostic: HTTP %d", kind, response.StatusCode)
	}
	const limit = 16 << 20
	data, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if len(data) > limit {
		return nil, fmt.Errorf("server %s diagnostic exceeds %d bytes", kind, limit)
	}
	return data, err
}

func (c *ProcessCluster) ServerNow(ctx context.Context, node int) (time.Time, error) {
	data, err := c.Diagnostic(ctx, node, "clock")
	if err != nil {
		return time.Time{}, err
	}
	var clock struct {
		Now time.Time `json:"now"`
	}
	if err := json.Unmarshal(data, &clock); err != nil {
		return time.Time{}, err
	}
	if clock.Now.IsZero() {
		return time.Time{}, fmt.Errorf("server clock omitted now")
	}
	return clock.Now, nil
}

func (c *ProcessCluster) PauseNode(i int) error {
	if i < 0 || i >= len(c.Commands) || c.Commands[i] == nil || c.paused[i] {
		return fmt.Errorf("process node %d cannot be paused", i)
	}
	if err := c.Commands[i].Process.Signal(syscall.SIGSTOP); err != nil {
		return err
	}
	var status syscall.WaitStatus
	for {
		pid, err := syscall.Wait4(c.Commands[i].Process.Pid, &status, syscall.WUNTRACED, nil)
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			return fmt.Errorf("wait for process node %d to stop: %w", i, err)
		}
		if pid == c.Commands[i].Process.Pid && status.Stopped() {
			break
		}
		if status.Exited() || status.Signaled() {
			return fmt.Errorf("process node %d exited before pause: %v", i, status)
		}
	}
	c.paused[i] = true
	return nil
}

func (c *ProcessCluster) ResumeNode(i int) error {
	if i < 0 || i >= len(c.Commands) || c.Commands[i] == nil || !c.paused[i] {
		return fmt.Errorf("process node %d is not paused", i)
	}
	if err := c.Commands[i].Process.Signal(syscall.SIGCONT); err != nil {
		return err
	}
	c.paused[i] = false
	return nil
}

// KillNode terminates one server without removing its file store. RestartNode
// can then reopen that store on the same client and route ports.
func (c *ProcessCluster) KillNode(i int) error {
	if i < 0 || i >= len(c.Commands) || c.Commands[i] == nil || c.Commands[i].ProcessState != nil {
		return fmt.Errorf("process node %d cannot be killed", i)
	}
	cmd := c.Commands[i]
	if c.slowDisk[i] != nil {
		if err := c.StopSlowDisk(i); err != nil {
			return err
		}
	}
	if c.paused[i] {
		if err := cmd.Process.Signal(syscall.SIGCONT); err != nil {
			return err
		}
		c.paused[i] = false
	}
	if err := cmd.Process.Kill(); err != nil {
		return err
	}
	_ = cmd.Wait()
	c.Clients[i].Close()
	return nil
}

func (c *ProcessCluster) RestartNode(i int) error {
	return c.restartNodeWithBinary(i, "")
}

// UpgradeNode restarts a stopped node on the same ports and file store using
// the module-pinned server binary built when this fixture started.
func (c *ProcessCluster) UpgradeNode(i int) error {
	return c.restartNodeWithBinary(i, filepath.Join(c.root, "nats-server"))
}

func (c *ProcessCluster) restartNodeWithBinary(i int, binary string) error {
	if i < 0 || i >= len(c.Commands) || c.Commands[i] == nil || c.Commands[i].ProcessState == nil {
		return fmt.Errorf("process node %d is not stopped", i)
	}
	old := c.Commands[i]
	logFile, err := os.OpenFile(c.logs[i], os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	if binary == "" {
		binary = old.Args[0]
	}
	cmd := exec.Command(binary, old.Args[1:]...)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	err = cmd.Start()
	_ = logFile.Close()
	if err != nil {
		return err
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, dialErr := c.Dial(i)
		if dialErr == nil {
			c.Commands[i] = cmd
			c.Clients[i] = conn
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	return fmt.Errorf("restarted process node %d did not accept clients: log=%s", i, c.logs[i])
}

func (c *ProcessCluster) Close() {
	for i := range c.slowDisk {
		if c.slowDisk[i] != nil {
			_ = c.StopSlowDisk(i)
		}
	}
	if c.routeMesh != nil {
		c.routeMesh.Close()
		c.routeMesh = nil
	}
	for _, client := range c.Clients {
		client.Close()
	}
	for i, cmd := range c.Commands {
		if cmd == nil || cmd.Process == nil || cmd.ProcessState != nil {
			continue
		}
		if c.paused[i] {
			_ = cmd.Process.Signal(syscall.SIGCONT)
			c.paused[i] = false
		}
		_ = cmd.Process.Signal(syscall.SIGTERM)
	}
	for _, cmd := range c.Commands {
		if cmd == nil || cmd.Process == nil || cmd.ProcessState != nil {
			continue
		}
		done := make(chan struct{})
		go func() { _ = cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
	}
}

// Dial returns a fresh pinned connection after a paused node resumes.
func (c *ProcessCluster) Dial(i int) (*nats.Conn, error) {
	if i < 0 || i >= len(c.ports) {
		return nil, fmt.Errorf("process node %d out of range", i)
	}
	return nats.Connect(c.ClientURL(i), nats.Timeout(2*time.Second), nats.NoReconnect())
}

// RouteCount reads the pinned server's public NATS monitoring endpoint.
func (c *ProcessCluster) RouteCount(ctx context.Context, node int) (int, error) {
	if node < 0 || node >= len(c.monitors) {
		return 0, fmt.Errorf("invalid monitor node %d", node)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/routez", c.monitors[node]), nil)
	if err != nil {
		return 0, err
	}
	response, err := (&http.Client{Timeout: time.Second}).Do(req)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("routez status %d", response.StatusCode)
	}
	var info struct {
		NumRoutes int `json:"num_routes"`
	}
	if err := json.NewDecoder(response.Body).Decode(&info); err != nil {
		return 0, err
	}
	return info.NumRoutes, nil
}
