//go:build !windows

package testcluster

import (
	"fmt"
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

// StartMixedVersionProcesses starts a three-node file-backed cluster with a
// chosen binary for each node. Empty paths use the server pinned by this
// module. It is intended for rolling-upgrade contract tests.
func StartMixedVersionProcesses(root string, binaries []string) (*ProcessCluster, error) {
	if len(binaries) != 3 {
		return nil, fmt.Errorf("mixed-version cluster needs three binaries")
	}
	return startProcessesWithBinaries(root, 3, false, binaries)
}

// StartPartitionableProcesses routes all server links through a controllable
// relay while retaining separate server process IDs and file stores.
func StartPartitionableProcesses(root string, count int) (*ProcessCluster, error) {
	return startProcesses(root, count, true)
}

func startProcesses(root string, count int, partitionable bool) (_ *ProcessCluster, err error) {
	return startProcessesWithBinaries(root, count, partitionable, nil)
}

func startProcessesWithBinaries(root string, count int, partitionable bool, binaries []string) (_ *ProcessCluster, err error) {
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
	c := &ProcessCluster{root: root, ports: make([]int, count), routes: make([]int, count), monitors: make([]int, count), paused: make([]bool, count), slowDisk: make([]*exec.Cmd, count)}
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
		c.ports[i], err = uniquePort()
		if err != nil {
			return nil, err
		}
		c.monitors[i], err = uniquePort()
		if err != nil {
			return nil, err
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
