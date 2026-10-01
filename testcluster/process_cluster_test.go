//go:build !windows

package testcluster

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func waitProcessRoutes(t *testing.T, c *ProcessCluster, want [3]int) {
	t.Helper()
	client := &http.Client{Timeout: 500 * time.Millisecond}
	deadline := time.Now().Add(10 * time.Second)
	var got [3]int
	for time.Now().Before(deadline) {
		ready := true
		for i, port := range c.monitors {
			response, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/routez", port))
			if err != nil {
				ready = false
				break
			}
			var routez struct {
				NumRoutes int `json:"num_routes"`
			}
			err = json.NewDecoder(response.Body).Decode(&routez)
			_ = response.Body.Close()
			if err != nil || response.StatusCode != http.StatusOK {
				ready = false
				break
			}
			got[i] = routez.NumRoutes
		}
		if ready && got == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("process routes=%v want=%v", got, want)
}

func TestProcessClusterIndependentDiagnostics(t *testing.T) {
	c, err := StartProfiledProcesses(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	// Diagnostics must remain available after the fixture's NATS client closes.
	c.Clients[0].Close()
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	stacks, err := c.Diagnostic(ctx, 0, "goroutines")
	if err != nil || !strings.Contains(string(stacks), "goroutine ") || !strings.Contains(string(stacks), "nats-server") {
		t.Fatalf("server stacks: bytes=%d err=%v", len(stacks), err)
	}
	connections, err := c.Diagnostic(ctx, 0, "connections")
	var state struct {
		ServerID string `json:"server_id"`
	}
	if err != nil || json.Unmarshal(connections, &state) != nil || state.ServerID == "" {
		t.Fatalf("connection snapshot: %s err=%v", connections, err)
	}
	state.ServerID = ""
	jetstreamState, err := c.Diagnostic(ctx, 0, "jetstream")
	if err != nil || json.Unmarshal(jetstreamState, &state) != nil || state.ServerID == "" {
		t.Fatalf("JetStream snapshot: %s err=%v", jetstreamState, err)
	}
	if err := c.KillNode(0); err != nil {
		t.Fatal(err)
	}
	if err := c.RestartNode(0); err != nil {
		t.Fatal(err)
	}
	if stacks, err := c.Diagnostic(ctx, 0, "goroutines"); err != nil || !strings.Contains(string(stacks), "goroutine ") {
		t.Fatalf("restarted profiler: bytes=%d err=%v", len(stacks), err)
	}
}

func TestProcessClusterPauseLeaderAndRecoverReplica(t *testing.T) {
	root := t.TempDir()
	c, err := StartPartitionableProcesses(root, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	waitProcessRoutes(t, c, [3]int{8, 8, 8})
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	js := make([]jetstream.JetStream, 3)
	for i, conn := range c.Clients {
		js[i], err = jetstream.New(conn)
		if err != nil {
			t.Fatal(err)
		}
	}
	var stream jetstream.Stream
	for ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, time.Second)
		stream, err = js[0].CreateStream(attempt, jetstream.StreamConfig{Name: "PAUSE_TEST", Subjects: []string{"pause.test"}, Replicas: 3, Storage: jetstream.FileStorage})
		stop()
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("create three-replica stream: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil || info.Cluster == nil || !strings.HasPrefix(info.Cluster.Leader, "wf-process-") {
		t.Fatalf("stream leader: info=%+v err=%v", info, err)
	}
	leader, err := strconv.Atoi(strings.TrimPrefix(info.Cluster.Leader, "wf-process-"))
	if err != nil || leader < 0 || leader >= 3 {
		t.Fatalf("invalid stream leader %q: %v", info.Cluster.Leader, err)
	}
	first, err := js[leader].Publish(ctx, "pause.test", []byte("before"))
	if err != nil || first == nil || first.Sequence != 1 {
		t.Fatalf("first publish=%+v err=%v", first, err)
	}
	partitionPath := filepath.Join(root, "partition-fault.json")
	if err := (FaultSchedule{Seed: 42, Events: []FaultEvent{{Op: PartitionNodes, A: 0, B: 1}}}).Save(partitionPath); err != nil {
		t.Fatal(err)
	}
	partition, err := LoadFaultSchedule(partitionPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := partition.Run(ctx, c.ApplyFault); err != nil {
		t.Fatal(err)
	}
	waitProcessRoutes(t, c, [3]int{4, 4, 8})
	c.RouteMesh().Heal()
	waitProcessRoutes(t, c, [3]int{8, 8, 8})
	pausePath := filepath.Join(root, "pause-fault.json")
	if err := (FaultSchedule{Seed: 42, Events: []FaultEvent{{Op: PauseNode, A: leader}}}).Save(pausePath); err != nil {
		t.Fatal(err)
	}
	pause, err := LoadFaultSchedule(pausePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := pause.Run(ctx, c.ApplyFault); err != nil {
		t.Fatal(err)
	}
	if err := c.Clients[leader].FlushTimeout(200 * time.Millisecond); err == nil {
		t.Fatal("paused server still responded to a client flush")
	}
	majority := (leader + 1) % 3
	var second *jetstream.PubAck
	for ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, 2*time.Second)
		second, err = js[majority].Publish(attempt, "pause.test", []byte("during"))
		stop()
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil || second == nil || second.Sequence != 2 {
		t.Fatalf("majority publish while node %d paused: ack=%+v err=%v", leader, second, err)
	}
	if err := c.ResumeNode(leader); err != nil {
		t.Fatal(err)
	}
	for ctx.Err() == nil {
		conn, dialErr := c.Dial(leader)
		if dialErr == nil {
			resumed, jsErr := jetstream.New(conn)
			if jsErr == nil {
				attempt, stop := context.WithTimeout(ctx, time.Second)
				stream, jsErr := resumed.Stream(attempt, "PAUSE_TEST")
				if jsErr == nil {
					var message *jetstream.RawStreamMsg
					message, jsErr = stream.GetMsg(attempt, 2)
					if jsErr == nil && string(message.Data) == "during" {
						stop()
						conn.Close()
						return
					}
				}
				stop()
			}
			conn.Close()
			err = jsErr
		} else {
			err = dialErr
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("resumed node %d did not recover acknowledged publish: %s (log=%s)", leader, fmt.Sprint(err), c.LogPath(leader))
}

func TestProcessClusterKillLeaderAndRestart(t *testing.T) {
	root := t.TempDir()
	c, err := StartProcesses(root, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	js := make([]jetstream.JetStream, 3)
	for i, conn := range c.Clients {
		js[i], err = jetstream.New(conn)
		if err != nil {
			t.Fatal(err)
		}
	}
	var stream jetstream.Stream
	for ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, time.Second)
		stream, err = js[0].CreateStream(attempt, jetstream.StreamConfig{Name: "KILL_TEST", Subjects: []string{"kill.test"}, Replicas: 3, Storage: jetstream.FileStorage})
		stop()
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("create three-replica stream: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil || info.Cluster == nil {
		t.Fatalf("stream leader: info=%+v err=%v", info, err)
	}
	leader, err := strconv.Atoi(strings.TrimPrefix(info.Cluster.Leader, "wf-process-"))
	if err != nil || leader < 0 || leader >= 3 {
		t.Fatalf("invalid stream leader %q: %v", info.Cluster.Leader, err)
	}
	first, err := js[leader].Publish(ctx, "kill.test", []byte("before"))
	if err != nil || first == nil || first.Sequence != 1 {
		t.Fatalf("first publish=%+v err=%v", first, err)
	}
	faultPath := filepath.Join(root, "kill-fault.json")
	if err := (FaultSchedule{Seed: 42, Events: []FaultEvent{{Op: KillNode, A: leader}}}).Save(faultPath); err != nil {
		t.Fatal(err)
	}
	fault, err := LoadFaultSchedule(faultPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := fault.Run(ctx, c.ApplyFault); err != nil {
		t.Fatal(err)
	}
	survivor := (leader + 1) % 3
	var second *jetstream.PubAck
	for ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, 2*time.Second)
		second, err = js[survivor].Publish(attempt, "kill.test", []byte("during"))
		stop()
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil || second == nil || second.Sequence != 2 {
		t.Fatalf("majority publish while node %d killed: ack=%+v err=%v", leader, second, err)
	}
	if err := c.RestartNode(leader); err != nil {
		t.Fatal(err)
	}
	resumed, err := jetstream.New(c.Clients[leader])
	if err != nil {
		t.Fatal(err)
	}
	for ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, time.Second)
		recovered, getErr := resumed.Stream(attempt, "KILL_TEST")
		if getErr == nil {
			var message *jetstream.RawStreamMsg
			message, getErr = recovered.GetMsg(attempt, 2)
			if getErr == nil && string(message.Data) == "during" {
				stop()
				return
			}
		}
		stop()
		err = getErr
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("restarted node %d did not recover acknowledged publish: %s (log=%s)", leader, fmt.Sprint(err), c.LogPath(leader))
}

func TestProcessClusterCombinedRecordedFaultsRecover(t *testing.T) {
	runProcessClusterCombinedFaultsRecover(t, false)
}

func TestProcessClusterCombinedFourFaultsRecover(t *testing.T) {
	if os.Getenv("WF_DISK_FAULT") != "1" {
		t.Skip("set WF_DISK_FAULT=1 to run the Linux disk-delay fixture")
	}
	if _, err := exec.LookPath("strace"); err != nil {
		t.Skip("strace is required for the Linux disk-delay fixture")
	}
	runProcessClusterCombinedFaultsRecover(t, true)
}

func runProcessClusterCombinedFaultsRecover(t *testing.T, withDiskDelay bool) {
	t.Helper()
	seed, err := SeedFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(seed))
	root := t.TempDir()
	c, err := StartPartitionableProcesses(root, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()
	waitProcessRoutes(t, c, [3]int{8, 8, 8})
	js := make([]jetstream.JetStream, 3)
	for i, conn := range c.Clients {
		js[i], err = jetstream.New(conn)
		if err != nil {
			t.Fatal(err)
		}
	}
	var stream jetstream.Stream
	for ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, time.Second)
		stream, err = js[0].CreateStream(attempt, jetstream.StreamConfig{Name: "COMBINED_TEST", Subjects: []string{"combined.test"}, Replicas: 3, Storage: jetstream.FileStorage})
		stop()
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("create three-replica stream: %v", err)
	}
	// Stream creation can return before the initial replica election finishes.
	// Require an elected fixture node before constructing the fault schedule.
	leader, err := awaitProcessStreamLeader(ctx, stream.Info, len(c.Clients))
	if err != nil {
		t.Fatalf("initial stream leader: %v", err)
	}
	first, err := js[leader].Publish(ctx, "combined.test", []byte("before"))
	if err != nil || first == nil || first.Sequence != 1 {
		t.Fatalf("first publish=%+v err=%v", first, err)
	}
	killed, other := (leader+1)%3, (leader+2)%3
	if rng.Intn(2) == 1 {
		killed, other = other, killed
	}
	path := filepath.Join(root, "combined-faults.json")
	if output := os.Getenv("FAULT_SCHEDULE_OUT"); output != "" {
		path = output
	}
	pauseAt := int64(10 + rng.Intn(30))
	events := []FaultEvent{
		{AtMillis: 0, Op: PartitionNodes, A: killed, B: other},
		{AtMillis: pauseAt, Op: PauseNode, A: leader},
		{AtMillis: pauseAt + int64(10+rng.Intn(30)), Op: KillNode, A: killed},
	}
	if withDiskDelay {
		events = append([]FaultEvent{{AtMillis: 0, Op: SlowDisk, A: other, LatencyMillis: 50}}, events...)
	}
	schedule := FaultSchedule{Seed: seed, Events: events}
	if err := schedule.Save(path); err != nil {
		t.Fatal(err)
	}
	t.Logf("FAULT_SEED=%d FAULT_SCHEDULE=%s", seed, path)
	replayed, err := LoadFaultSchedule(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := replayed.Run(ctx, c.ApplyFault); err != nil {
		t.Fatal(err)
	}
	c.RouteMesh().Heal()
	if err := c.ResumeNode(leader); err != nil {
		t.Fatal(err)
	}
	var second *jetstream.PubAck
	for ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, 2*time.Second)
		second, err = js[other].Publish(attempt, "combined.test", []byte("after-heal"))
		stop()
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil || second == nil || second.Sequence != 2 {
		t.Fatalf("majority publish after heal: ack=%+v err=%v", second, err)
	}
	if withDiskDelay {
		if err := c.StopSlowDisk(other); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(c.DiskTracePath(other))
		if err != nil || !strings.Contains(string(data), "(DELAYED)") {
			t.Fatalf("no delayed store syscall in trace %s: %v: %s", c.DiskTracePath(other), err, data)
		}
	}
	if err := c.RestartNode(killed); err != nil {
		t.Fatal(err)
	}
	waitProcessRoutes(t, c, [3]int{8, 8, 8})
	resumed, err := jetstream.New(c.Clients[killed])
	if err != nil {
		t.Fatal(err)
	}
	for ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, time.Second)
		recovered, getErr := resumed.Stream(attempt, "COMBINED_TEST")
		if getErr == nil {
			var message *jetstream.RawStreamMsg
			message, getErr = recovered.GetMsg(attempt, 2)
			if getErr == nil && string(message.Data) == "after-heal" {
				stop()
				return
			}
		}
		stop()
		err = getErr
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("restarted node %d did not recover acknowledged publish: %s (log=%s)", killed, fmt.Sprint(err), c.LogPath(killed))
}

func TestProcessClusterSlowDiskDelaysStoreWrites(t *testing.T) {
	if os.Getenv("WF_DISK_FAULT") != "1" {
		t.Skip("set WF_DISK_FAULT=1 to run the Linux disk-delay fixture")
	}
	if _, err := exec.LookPath("strace"); err != nil {
		t.Skip("strace is required for the Linux disk-delay fixture")
	}
	c, err := StartProcesses(t.TempDir(), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	js := make([]jetstream.JetStream, 3)
	for i, conn := range c.Clients {
		js[i], err = jetstream.New(conn)
		if err != nil {
			t.Fatal(err)
		}
	}
	var stream jetstream.Stream
	for ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, time.Second)
		stream, err = js[0].CreateStream(attempt, jetstream.StreamConfig{Name: "DISK_TEST", Subjects: []string{"disk.test"}, Replicas: 3, Storage: jetstream.FileStorage})
		stop()
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx)
	if err != nil || info.Cluster == nil {
		t.Fatalf("stream leader: info=%+v err=%v", info, err)
	}
	leader, err := strconv.Atoi(strings.TrimPrefix(info.Cluster.Leader, "wf-process-"))
	if err != nil || leader < 0 || leader >= 3 {
		t.Fatalf("invalid stream leader %q: %v", info.Cluster.Leader, err)
	}
	if _, err := js[leader].Publish(ctx, "disk.test", []byte("before")); err != nil {
		t.Fatal(err)
	}
	if err := (FaultSchedule{Seed: 42, Events: []FaultEvent{{Op: SlowDisk, A: leader, LatencyMillis: 50}}}).Run(ctx, c.ApplyFault); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if _, err := js[leader].Publish(ctx, "disk.test", []byte("during")); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.StopSlowDisk(leader); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(c.DiskTracePath(leader))
	if err != nil || !strings.Contains(string(data), "(DELAYED)") {
		t.Fatalf("no delayed store syscall in trace %s: %v: %s", c.DiskTracePath(leader), err, data)
	}
	if !strings.Contains(string(data), "<"+c.root+"/") || !strings.Contains(string(data), " (DELAYED) <") {
		t.Fatalf("disk trace lacks decoded paths or syscall durations: %s", data)
	}
	info, err = stream.Info(ctx)
	if err != nil || info.State.Msgs != 11 {
		t.Fatalf("stream after disk delay: info=%+v err=%v", info, err)
	}
}

// awaitProcessStreamLeader retries absent election metadata and info errors. Invalid
// nonempty leader names fail immediately instead of selecting the wrong node.
func awaitProcessStreamLeader(ctx context.Context, info func(context.Context, ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error), nodes int) (int, error) {
	ready, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var lastErr error
	for {
		attempt, stop := context.WithTimeout(ready, time.Second)
		state, err := info(attempt)
		stop()
		if err == nil && state != nil && state.Cluster != nil && state.Cluster.Leader != "" {
			name := state.Cluster.Leader
			leader, parseErr := strconv.Atoi(strings.TrimPrefix(name, "wf-process-"))
			if !strings.HasPrefix(name, "wf-process-") || parseErr != nil || leader < 0 || leader >= nodes {
				return 0, fmt.Errorf("invalid stream leader %q", name)
			}
			return leader, nil
		}
		lastErr = err
		timer := time.NewTimer(25 * time.Millisecond)
		select {
		case <-ready.Done():
			timer.Stop()
			return 0, fmt.Errorf("stream leader not ready: %w (last info error: %v)", ready.Err(), lastErr)
		case <-timer.C:
		}
	}
}

func TestAwaitProcessStreamLeader(t *testing.T) {
	t.Run("initial election", func(t *testing.T) {
		calls := 0
		leader, err := awaitProcessStreamLeader(context.Background(), func(context.Context, ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
			calls++
			if calls == 1 {
				return &jetstream.StreamInfo{}, nil
			}
			name := ""
			if calls == 3 {
				name = "wf-process-2"
			}
			return &jetstream.StreamInfo{Cluster: &jetstream.ClusterInfo{Leader: name}}, nil
		}, 3)
		if err != nil || leader != 2 || calls != 3 {
			t.Fatalf("leader=%d calls=%d err=%v", leader, calls, err)
		}
	})
	t.Run("invalid leader", func(t *testing.T) {
		_, err := awaitProcessStreamLeader(context.Background(), func(context.Context, ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
			return &jetstream.StreamInfo{Cluster: &jetstream.ClusterInfo{Leader: "other-2"}}, nil
		}, 3)
		if err == nil {
			t.Fatal("accepted a leader outside the fixture")
		}
	})
	t.Run("election deadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_, err := awaitProcessStreamLeader(ctx, func(context.Context, ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
			return &jetstream.StreamInfo{Cluster: &jetstream.ClusterInfo{}}, nil
		}, 3)
		if err == nil || !strings.Contains(err.Error(), "context deadline exceeded") {
			t.Fatalf("missing bounded election failure: %v", err)
		}
	})
}
