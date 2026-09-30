package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"js-wf/assignment"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/testcluster"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// WF_REBALANCE_SCALE=1 runs 200 workflows with 50 journaled increments each
// while four busy partitions move between six workers every five seconds.
func TestTwoHundredWorkflowsFiftyStepsWithRepeatedRebalance(t *testing.T) {
	if os.Getenv("WF_REBALANCE_SCALE") == "" {
		t.Skip("set WF_REBALANCE_SCALE=1 for the 200-workflow rebalance proof")
	}
	runRebalanceScale(t, false, false, false, false, false, false)
}

// WF_REBALANCE_KV_KILL=1 adds a KV leader kill to the same 200-workflow run.
// All six workers stay connected to the two surviving nodes.
func TestTwoHundredWorkflowsRebalanceDuringKVLeaderKill(t *testing.T) {
	if os.Getenv("WF_REBALANCE_KV_KILL") == "" {
		t.Skip("set WF_REBALANCE_KV_KILL=1 for the 200-workflow rebalance and leader-kill proof")
	}
	runRebalanceScale(t, true, false, false, false, false, false)
}

// WF_REBALANCE_WORKER_PARTITION=1 isolates one worker for 45 seconds while
// assignments keep moving and the other five workers finish the backlog.
func TestTwoHundredWorkflowsRebalanceDuringWorkerPartition(t *testing.T) {
	if os.Getenv("WF_REBALANCE_WORKER_PARTITION") == "" {
		t.Skip("set WF_REBALANCE_WORKER_PARTITION=1 for the 200-workflow network-partition proof")
	}
	runRebalanceScale(t, false, true, false, false, false, false)
}

// WF_REBALANCE_WORKER_KILL=1 closes an active worker connection, waits for its
// loop to exit, and starts a replacement with the same ID during the backlog.
func TestTwoHundredWorkflowsRebalanceDuringWorkerKill(t *testing.T) {
	if os.Getenv("WF_REBALANCE_WORKER_KILL") == "" {
		t.Skip("set WF_REBALANCE_WORKER_KILL=1 for the 200-workflow worker-kill proof")
	}
	runRebalanceScale(t, false, false, true, false, false, false)
}

// WF_REBALANCE_PROCESS_KILL=1 runs owner 0 in a child process and sends
// SIGKILL after it begins an invocation; the surviving workers rebalance it.
func TestTwoHundredWorkflowsRebalanceDuringProcessKill(t *testing.T) {
	if os.Getenv("WF_REBALANCE_PROCESS_KILL") == "" {
		t.Skip("set WF_REBALANCE_PROCESS_KILL=1 for the 200-workflow process-kill proof")
	}
	runRebalanceScale(t, false, false, false, true, false, false)
}

// WF_REBALANCE_ROUTE_PARTITION=1 isolates one NATS server from both peers for
// 45 seconds while all six workers and the assignment controller remain live.
func TestTwoHundredWorkflowsRebalanceDuringRoutePartition(t *testing.T) {
	if os.Getenv("WF_REBALANCE_ROUTE_PARTITION") == "" {
		t.Skip("set WF_REBALANCE_ROUTE_PARTITION=1 for the 200-workflow server-route partition proof")
	}
	runRebalanceScale(t, false, false, false, false, true, false)
}

// WF_REBALANCE_COMBINED_CHAOS=1 overlaps a worker process kill, a 45-second
// process pause, a 45-second worker connection cut, and a 45-second server
// route partition while the busy partition assignments keep moving.
func TestTwoHundredWorkflowsRebalanceDuringCombinedFaults(t *testing.T) {
	if os.Getenv("WF_REBALANCE_COMBINED_CHAOS") == "" {
		t.Skip("set WF_REBALANCE_COMBINED_CHAOS=1 for the combined fault proof")
	}
	runRebalanceScale(t, false, true, false, true, true, true)
}

// TestRebalanceWorkerChild runs only as a subprocess of the process-fault tests.
func TestRebalanceWorkerChild(t *testing.T) {
	if os.Getenv("WF_REBALANCE_CHILD") != "1" {
		t.Skip("worker subprocess helper")
	}
	url, marker := os.Getenv("WF_REBALANCE_CHILD_URL"), os.Getenv("WF_REBALANCE_CHILD_MARKER")
	if url == "" || marker == "" {
		t.Fatal("missing worker subprocess configuration")
	}
	nc, err := nats.Connect(url, nats.NoReconnect(), nats.IgnoreDiscoveredServers())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	workerID := os.Getenv("WF_REBALANCE_CHILD_WORKER")
	if workerID == "" {
		workerID = "rebalance-0"
	}
	pauseRelease := os.Getenv("WF_REBALANCE_CHILD_RELEASE")
	pauseResult := os.Getenv("WF_REBALANCE_CHILD_RESULT")
	if pauseRelease != "" && pauseResult == "" {
		t.Fatal("missing pause-child result path")
	}
	w, err := worker.New(context.Background(), js, workerID, map[string]worker.Handler{"rebalance": func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
		var id string
		if err := json.Unmarshal(input, &id); err != nil {
			return nil, err
		}
		var records []journal.Record
		var tail uint64
		if pauseRelease != "" {
			var err error
			records, tail, err = journal.New(js).Read(c.Context(), "rebalance", id)
			if err != nil || len(records) != 1 || records[0].Kind != journal.Started {
				return nil, fmt.Errorf("pause child initial journal=%+v err=%v", records, err)
			}
		}
		if err := os.WriteFile(marker, []byte(id), 0644); err != nil {
			return nil, err
		}
		if pauseRelease == "" {
			select {}
		}
		for {
			if _, err := os.Stat(pauseRelease); err == nil {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		probeCtx, stopProbe := context.WithTimeout(context.Background(), 45*time.Second)
		defer stopProbe()
		var appendErr error
		probeURLs := strings.Split(os.Getenv("WF_REBALANCE_CHILD_PROBE_URLS"), ",")
		probeAttempt := 0
		for probeCtx.Err() == nil {
			probeJS := js
			var probeConn *nats.Conn
			if probeAttempt > 0 && len(probeURLs) > 0 && probeURLs[0] != "" {
				probeURL := probeURLs[(probeAttempt-1)%len(probeURLs)]
				probeConn, err = nats.Connect(probeURL, nats.NoReconnect(), nats.IgnoreDiscoveredServers(), nats.Timeout(time.Second))
				if err == nil {
					probeJS, err = jetstream.New(probeConn)
				}
				if err != nil {
					if probeConn != nil {
						probeConn.Close()
					}
					appendErr = fmt.Errorf("%w: connect probe peer %s: %v", journal.ErrUnknown, probeURL, err)
					probeAttempt++
					select {
					case <-probeCtx.Done():
					case <-time.After(100 * time.Millisecond):
					}
					continue
				}
			}
			attempt, done := context.WithTimeout(probeCtx, 5*time.Second)
			_, appendErr = journal.New(probeJS).Append(attempt, "rebalance", id, journal.Entry{Epoch: records[0].Epoch, Index: 1, Kind: journal.Completed, Payload: json.RawMessage(`0`), WorkerID: workerID}, tail)
			done()
			if probeConn != nil {
				probeConn.Close()
			}
			probeAttempt++
			if !errors.Is(appendErr, journal.ErrUnknown) && !errors.Is(appendErr, context.DeadlineExceeded) {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		outcome := "success"
		if errors.Is(appendErr, journal.ErrStale) {
			outcome = "stale"
		} else if appendErr != nil {
			outcome = fmt.Sprintf("error after %d attempts: %v", probeAttempt, appendErr)
		}
		if err := os.WriteFile(pauseResult, []byte(outcome), 0644); err != nil {
			return nil, err
		}
		return nil, appendErr
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.RunKVAssignments(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func runRebalanceScale(t *testing.T, killKVLeader, isolateWorker, killWorker, killProcess, routePartition, pauseProcess bool) {
	t.Helper()
	var all []jetstream.JetStream
	var cluster *testcluster.Cluster
	if routePartition {
		partitionable, err := testcluster.StartPartitionable(t.TempDir(), 3)
		if err != nil {
			t.Fatal(err)
		}
		all, cluster = setupCluster(t, partitionable)
		defer cluster.RouteMesh().Heal()
	} else {
		all, cluster = setup(t)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	workerNodes := []int{0, 1, 2}
	kvLeader := -1
	var kvStream jetstream.Stream
	var readyKV func() *jetstream.StreamInfo
	if killKVLeader {
		var err error
		kvStream, err = all[0].Stream(ctx, "KV_WF_ASSIGN")
		if err != nil {
			t.Fatal(err)
		}
		readyKV = func() *jetstream.StreamInfo {
			until := time.Now().Add(15 * time.Second)
			for {
				attempt, done := context.WithTimeout(ctx, time.Second)
				info, infoErr := kvStream.Info(attempt)
				done()
				ready := infoErr == nil && info.Cluster != nil && info.Cluster.Leader != "" && len(info.Cluster.Replicas) == 2
				if ready {
					for _, replica := range info.Cluster.Replicas {
						ready = ready && replica.Current && !replica.Offline
					}
				}
				for node, server := range cluster.Servers {
					routes, routeErr := server.Routez(nil)
					if routeErr != nil {
						ready = false
						continue
					}
					peers := map[string]bool{}
					for _, route := range routes.Routes {
						peers[route.RemoteName] = true
					}
					for other, peer := range cluster.Servers {
						if other != node {
							ready = ready && peers[peer.Name()]
						}
					}
				}
				if ready {
					return info
				}
				if time.Now().After(until) || ctx.Err() != nil {
					t.Fatalf("KV assignment replicas not ready: info=%+v err=%v", info, infoErr)
				}
				time.Sleep(50 * time.Millisecond)
			}
		}
		info := readyKV()
		workerNodes = nil
		for index, server := range cluster.Servers {
			if server.Name() == info.Cluster.Leader {
				kvLeader = index
			} else {
				workerNodes = append(workerNodes, index)
			}
		}
		if kvLeader < 0 || len(workerNodes) != 2 {
			t.Fatalf("unknown KV assignment leader %q", info.Cluster.Leader)
		}
		kvStream, err = all[workerNodes[0]].Stream(ctx, "KV_WF_ASSIGN")
		if err != nil {
			t.Fatal(err)
		}
	}
	var proxy *testcluster.ClientProxy
	var isolatedConn *nats.Conn
	var isolatedJS jetstream.JetStream
	var killConn *nats.Conn
	var killJS jetstream.JetStream
	isolatedWorkerIndex := 0
	if killProcess {
		isolatedWorkerIndex = 2
	}
	var dedicatedConns []*nats.Conn
	defer func() {
		for _, conn := range dedicatedConns {
			conn.Close()
		}
	}()
	if isolateWorker {
		var err error
		proxy, err = testcluster.NewClientProxy(cluster.Servers[workerNodes[0]].ClientURL())
		if err != nil {
			t.Fatal(err)
		}
		defer proxy.Close()
		isolatedConn, err = proxy.Connect()
		if err != nil {
			t.Fatal(err)
		}
		defer isolatedConn.Close()
		isolatedJS, err = jetstream.New(isolatedConn)
		if err != nil {
			t.Fatal(err)
		}
	}
	if killWorker {
		var err error
		killConn, err = nats.Connect(cluster.Servers[workerNodes[0]].ClientURL(), nats.NoReconnect(), nats.IgnoreDiscoveredServers())
		if err != nil {
			t.Fatal(err)
		}
		dedicatedConns = append(dedicatedConns, killConn)
		killJS, err = jetstream.New(killConn)
		if err != nil {
			t.Fatal(err)
		}
	}
	assignments, err := assignment.New(ctx, all[workerNodes[0]])
	if err != nil {
		t.Fatal(err)
	}
	owners := []string{"rebalance-0", "rebalance-1", "rebalance-2", "rebalance-3", "rebalance-4", "rebalance-5"}
	if err := assignments.InitializeStatic(ctx, owners); err != nil {
		t.Fatal(err)
	}
	var childCmd *exec.Cmd
	var childMarker string
	childWaited := false
	if killProcess {
		executable, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		childRoot := t.TempDir()
		childMarker = filepath.Join(childRoot, "active-invocation")
		childLog, err := os.Create(filepath.Join(childRoot, "worker.log"))
		if err != nil {
			t.Fatal(err)
		}
		defer childLog.Close()
		childCmd = exec.Command(executable, "-test.run=^TestRebalanceWorkerChild$")
		childCmd.Env = append(os.Environ(), "WF_REBALANCE_CHILD=1", "WF_REBALANCE_CHILD_URL="+cluster.Servers[workerNodes[0]].ClientURL(), "WF_REBALANCE_CHILD_MARKER="+childMarker)
		childCmd.Stdout, childCmd.Stderr = childLog, childLog
		if err := childCmd.Start(); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if !childWaited {
				_ = childCmd.Process.Kill()
				_ = childCmd.Wait()
			}
		}()
	}
	var pauseCmd *exec.Cmd
	var pauseMarker, pauseRelease, pauseResult string
	pauseWaited := false
	if pauseProcess {
		executable, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		childRoot := t.TempDir()
		pauseMarker = filepath.Join(childRoot, "active-invocation")
		pauseRelease = filepath.Join(childRoot, "release")
		pauseResult = filepath.Join(childRoot, "result")
		childLog, err := os.Create(filepath.Join(childRoot, "worker.log"))
		if err != nil {
			t.Fatal(err)
		}
		defer childLog.Close()
		pauseCmd = exec.Command(executable, "-test.run=^TestRebalanceWorkerChild$")
		pauseCmd.Env = append(os.Environ(), "WF_REBALANCE_CHILD=1", "WF_REBALANCE_CHILD_URL="+cluster.Servers[workerNodes[1%len(workerNodes)]].ClientURL(), "WF_REBALANCE_CHILD_PROBE_URLS="+cluster.Servers[workerNodes[0]].ClientURL()+","+cluster.Servers[workerNodes[2%len(workerNodes)]].ClientURL(), "WF_REBALANCE_CHILD_WORKER=rebalance-1", "WF_REBALANCE_CHILD_MARKER="+pauseMarker, "WF_REBALANCE_CHILD_RELEASE="+pauseRelease, "WF_REBALANCE_CHILD_RESULT="+pauseResult)
		pauseCmd.Stdout, pauseCmd.Stderr = childLog, childLog
		if err := pauseCmd.Start(); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if !pauseWaited {
				_ = pauseCmd.Process.Kill()
				_ = pauseCmd.Wait()
			}
		}()
	}
	const typ = "rebalance"
	var completed, collisions atomic.Int64
	var activeByPartition [4]atomic.Int64
	var active sync.Map
	handler := func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
		var id string
		if err := json.Unmarshal(input, &id); err != nil {
			return nil, err
		}
		if _, loaded := active.LoadOrStore(id, true); loaded {
			collisions.Add(1)
			t.Logf("simultaneous handler execution for %s", id)
			return nil, fmt.Errorf("simultaneous handler execution for %s", id)
		}
		defer active.Delete(id)
		partition := identity.Partition(typ, id, provision.Partitions)
		activeByPartition[partition].Add(1)
		defer activeByPartition[partition].Add(-1)
		count := 0
		for step := 0; step < 50; step++ {
			nextCount := count + 1
			var err error
			count, err = wf.Run(c, "increment", count, func(stepCtx context.Context) (int, error) {
				timer := time.NewTimer(10 * time.Millisecond)
				defer timer.Stop()
				select {
				case <-stepCtx.Done():
					return 0, stepCtx.Err()
				case <-timer.C:
					return nextCount, nil
				}
			})
			if err != nil {
				return nil, err
			}
		}
		completed.Add(1)
		return json.Marshal(count)
	}
	workerCtx, stopWorkers := context.WithCancel(ctx)
	defer stopWorkers()
	type workerExit struct {
		index int
		err   error
	}
	done := make(chan workerExit, len(owners)+1)
	activeWorkers := 0
	startWorker := func(index int, workerJS jetstream.JetStream) {
		w, err := worker.New(ctx, workerJS, owners[index], map[string]worker.Handler{typ: handler})
		if err != nil {
			t.Fatal(err)
		}
		activeWorkers++
		go func() { done <- workerExit{index: index, err: w.RunKVAssignments(workerCtx)} }()
	}
	defer func() {
		stopWorkers()
		for activeWorkers > 0 {
			result := <-done
			activeWorkers--
			if result.err != nil {
				t.Error(result.err)
			}
		}
	}()
	for index := range owners {
		if killProcess && index == 0 || pauseProcess && index == 1 {
			continue
		}
		workerJS := all[workerNodes[index%len(workerNodes)]]
		if isolateWorker && index == isolatedWorkerIndex {
			workerJS = isolatedJS
		}
		if killWorker && index == 0 {
			workerJS = killJS
		}
		startWorker(index, workerJS)
	}
	run, err := all[workerNodes[0]].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	consumersUntil := time.Now().Add(30 * time.Second)
	for {
		info, err := run.Info(ctx)
		if err == nil && info.State.Consumers == int(provision.Partitions) {
			break
		}
		if ctx.Err() != nil || time.Now().After(consumersUntil) {
			t.Fatalf("partition consumers not ready: info=%+v err=%v", info, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	ids := make([]string, 0, 200)
	counts := [4]int{}
	for candidate := 0; len(ids) < 200; candidate++ {
		id := fmt.Sprintf("job-%05d", candidate)
		partition := identity.Partition(typ, id, provision.Partitions)
		if partition < 4 && counts[partition] < 50 {
			ids = append(ids, id)
			counts[partition]++
		}
	}
	clients := make([]*client.Client, len(workerNodes))
	for index, node := range workerNodes {
		clients[index] = client.New(all[node])
	}
	for index, id := range ids {
		payload, _ := json.Marshal(id)
		if _, err := clients[index%len(clients)].Start(ctx, typ, id, payload); err != nil {
			t.Fatalf("start %s: %v", id, err)
		}
	}
	moves, inFlightMoves := 0, 0
	leaderKilled := false
	workerIsolated, workerHealed, isolatedInFlight := false, false, false
	workerKilled, workerRestarted, killedInFlight := false, false, false
	processKilled, processKilledInFlight := false, false
	processPaused, processResumed, pauseStale := false, false, false
	var pausedAt time.Time
	var pausedID string
	routesIsolated, routesHealed, routeFaultInFlight := false, false, false
	var isolatedAt time.Time
	var routesIsolatedAt time.Time
	healWorker := func() {
		if !workerIsolated || workerHealed {
			return
		}
		if remaining := 45*time.Second - time.Since(isolatedAt); remaining > 0 {
			select {
			case <-time.After(remaining):
			case <-ctx.Done():
				t.Fatal("worker partition did not last 45 seconds")
			}
		}
		proxy.Heal()
		for !isolatedConn.IsConnected() && ctx.Err() == nil {
			time.Sleep(10 * time.Millisecond)
		}
		if ctx.Err() != nil {
			t.Fatal("isolated worker did not reconnect")
		}
		workerHealed = true
	}
	healRoutes := func() {
		if !routesIsolated || routesHealed {
			return
		}
		if remaining := 45*time.Second - time.Since(routesIsolatedAt); remaining > 0 {
			select {
			case <-time.After(remaining):
			case <-ctx.Done():
				t.Fatal("server route partition did not last 45 seconds")
			}
		}
		cluster.RouteMesh().Heal()
		until := time.Now().Add(10 * time.Second)
		for cluster.Servers[0].NumRoutes() < 2 || cluster.Servers[1].NumRoutes() < 2 || cluster.Servers[2].NumRoutes() < 2 {
			if time.Now().After(until) || ctx.Err() != nil {
				t.Fatalf("server routes did not heal: %d/%d/%d", cluster.Servers[0].NumRoutes(), cluster.Servers[1].NumRoutes(), cluster.Servers[2].NumRoutes())
			}
			time.Sleep(20 * time.Millisecond)
		}
		routesHealed = true
	}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for completed.Load() < int64(len(ids)) {
		select {
		case <-ticker.C:
			if routePartition && !routesIsolated {
				until := time.Now().Add(2 * time.Second)
				for activeByPartition[2].Load() == 0 && time.Now().Before(until) {
					time.Sleep(10 * time.Millisecond)
				}
				routeFaultInFlight = activeByPartition[2].Load() > 0
				if err := cluster.RouteMesh().PartitionNode(2); err != nil {
					t.Fatal(err)
				}
				routesIsolatedAt = time.Now()
				routesIsolated = true
				until = time.Now().Add(2 * time.Second)
				for cluster.Servers[2].NumRoutes() != 0 || cluster.Servers[0].NumRoutes() < 1 || cluster.Servers[1].NumRoutes() < 1 {
					if time.Now().After(until) || ctx.Err() != nil {
						t.Fatalf("server route partition did not take effect: %d/%d/%d", cluster.Servers[0].NumRoutes(), cluster.Servers[1].NumRoutes(), cluster.Servers[2].NumRoutes())
					}
					time.Sleep(10 * time.Millisecond)
				}
			} else if routePartition && routesIsolated && !routesHealed && time.Since(routesIsolatedAt) >= 45*time.Second {
				healRoutes()
			}
			if killProcess && !processKilled {
				until := time.Now().Add(2 * time.Second)
				for {
					activeID, readErr := os.ReadFile(childMarker)
					if readErr == nil && len(activeID) > 0 {
						if identity.Partition(typ, string(activeID), provision.Partitions) != 0 {
							t.Fatalf("child held unexpected invocation %q", activeID)
						}
						processKilledInFlight = true
						break
					}
					if time.Now().After(until) || ctx.Err() != nil {
						t.Fatalf("child did not enter a workflow: marker_err=%v", readErr)
					}
					time.Sleep(10 * time.Millisecond)
				}
				if err := childCmd.Process.Kill(); err != nil {
					t.Fatalf("kill worker process: %v", err)
				}
				waitErr := childCmd.Wait()
				childWaited = true
				status, ok := childCmd.ProcessState.Sys().(syscall.WaitStatus)
				if waitErr == nil || !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
					t.Fatalf("worker process did not die from SIGKILL: state=%v err=%v", childCmd.ProcessState, waitErr)
				}
				processKilled = true
			}
			if pauseProcess && !processPaused {
				until := time.Now().Add(2 * time.Second)
				for {
					activeID, readErr := os.ReadFile(pauseMarker)
					if readErr == nil && len(activeID) > 0 {
						if identity.Partition(typ, string(activeID), provision.Partitions) != 1 {
							t.Fatalf("paused child held unexpected invocation %q", activeID)
						}
						pausedID = string(activeID)
						break
					}
					if time.Now().After(until) || ctx.Err() != nil {
						t.Fatalf("paused child did not enter a workflow: marker_err=%v", readErr)
					}
					time.Sleep(10 * time.Millisecond)
				}
				if err := pauseCmd.Process.Signal(syscall.SIGSTOP); err != nil {
					t.Fatalf("pause worker process: %v", err)
				}
				pausedAt = time.Now()
				processPaused = true
			}
			if killWorker && !workerKilled {
				until := time.Now().Add(2 * time.Second)
				for activeByPartition[0].Load() == 0 && time.Now().Before(until) {
					time.Sleep(10 * time.Millisecond)
				}
				killedInFlight = activeByPartition[0].Load() > 0
				killConn.Close()
				workerKilled = true
				select {
				case result := <-done:
					activeWorkers--
					if result.index != 0 {
						t.Fatalf("worker %d exited during worker 0 kill: %v", result.index, result.err)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("killed worker did not exit")
				}
				replacementConn, err := nats.Connect(cluster.Servers[workerNodes[0]].ClientURL(), nats.NoReconnect(), nats.IgnoreDiscoveredServers())
				if err != nil {
					t.Fatal(err)
				}
				dedicatedConns = append(dedicatedConns, replacementConn)
				replacementJS, err := jetstream.New(replacementConn)
				if err != nil {
					t.Fatal(err)
				}
				startWorker(0, replacementJS)
				workerRestarted = true
			}
			if isolateWorker && !workerIsolated {
				until := time.Now().Add(2 * time.Second)
				for activeByPartition[isolatedWorkerIndex].Load() == 0 && time.Now().Before(until) {
					time.Sleep(10 * time.Millisecond)
				}
				isolatedInFlight = activeByPartition[isolatedWorkerIndex].Load() > 0
				proxy.Block()
				isolatedAt = time.Now()
				workerIsolated = true
			} else if isolateWorker && workerIsolated && !workerHealed && time.Since(isolatedAt) >= 45*time.Second {
				healWorker()
			}
			for partition := uint32(0); partition < 4; partition++ {
				moveTimeout := 5 * time.Second
				if routePartition {
					moveTimeout = 65 * time.Second
				}
				until := time.Now().Add(moveTimeout)
				for {
					if routePartition && !routesHealed && time.Since(routesIsolatedAt) >= 45*time.Second {
						healRoutes()
					}
					attempt, done := context.WithTimeout(ctx, time.Second)
					owner, revision, getErr := assignments.Get(attempt, partition)
					done()
					if getErr == nil {
						index := -1
						for i, candidate := range owners {
							if candidate == owner {
								index = i
								break
							}
						}
						if index < 0 {
							t.Fatalf("unexpected owner %q for partition %d", owner, partition)
						}
						attempt, done = context.WithTimeout(ctx, time.Second)
						_, moveErr := assignments.Assign(attempt, partition, owners[(index+1)%len(owners)], revision)
						done()
						if moveErr == nil {
							break
						}
						if !routePartition && !errors.Is(moveErr, assignment.ErrConflict) {
							t.Fatalf("move partition %d: %v", partition, moveErr)
						}
						getErr = moveErr
					}
					if time.Now().After(until) || ctx.Err() != nil {
						t.Fatalf("move partition %d did not converge: %v", partition, getErr)
					}
					time.Sleep(50 * time.Millisecond)
				}
				moves++
				if activeByPartition[partition].Load() > 0 {
					inFlightMoves++
				}
			}
			if killKVLeader && !leaderKilled {
				current := readyKV()
				if current.Cluster.Leader != cluster.Servers[kvLeader].Name() {
					t.Fatalf("KV assignment leader changed before injected kill: %s", current.Cluster.Leader)
				}
				cluster.KillNode(kvLeader)
				leaderKilled = true
				until := time.Now().Add(20 * time.Second)
				for {
					attempt, done := context.WithTimeout(ctx, time.Second)
					info, infoErr := kvStream.Info(attempt)
					done()
					if infoErr == nil && info.Cluster != nil && info.Cluster.Leader != "" && info.Cluster.Leader != cluster.Servers[kvLeader].Name() {
						break
					}
					if time.Now().After(until) || ctx.Err() != nil {
						t.Fatalf("KV assignment leader did not move: info=%+v err=%v", info, infoErr)
					}
					time.Sleep(50 * time.Millisecond)
				}
			}
		case result := <-done:
			done <- result
			t.Fatalf("worker %d exited during rebalance: %v", result.index, result.err)
		case <-ctx.Done():
			t.Fatalf("workflows did not finish: completed=%d collisions=%d moves=%d", completed.Load(), collisions.Load(), moves)
		case <-time.After(100 * time.Millisecond):
		}
	}
	if isolateWorker && !workerHealed {
		healWorker()
	}
	if routePartition && !routesHealed {
		healRoutes()
	}
	if pauseProcess {
		if !processPaused || pausedID == "" {
			t.Fatal("worker process was not paused in flight")
		}
		if remaining := 45*time.Second - time.Since(pausedAt); remaining > 0 {
			select {
			case <-time.After(remaining):
			case <-ctx.Done():
				t.Fatal("worker process was not stopped for 45 seconds")
			}
		}
		value, err := clients[0].Await(ctx, typ, pausedID)
		if err != nil || string(value) != "50" {
			t.Fatalf("paused worker invocation result=%s err=%v", value, err)
		}
		if err := pauseCmd.Process.Signal(syscall.SIGCONT); err != nil {
			t.Fatalf("resume worker process: %v", err)
		}
		if err := os.WriteFile(pauseRelease, []byte("resume"), 0644); err != nil {
			t.Fatal(err)
		}
		for {
			outcome, err := os.ReadFile(pauseResult)
			if err == nil {
				if string(outcome) != "stale" {
					t.Fatalf("resumed worker append outcome=%q", outcome)
				}
				pauseStale = true
				break
			}
			if ctx.Err() != nil {
				t.Fatalf("resumed worker did not report stale append: %v", err)
			}
			time.Sleep(10 * time.Millisecond)
		}
		if err := pauseCmd.Process.Kill(); err != nil {
			t.Fatal(err)
		}
		waitErr := pauseCmd.Wait()
		pauseWaited = true
		status, ok := pauseCmd.ProcessState.Sys().(syscall.WaitStatus)
		if waitErr == nil || !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
			t.Fatalf("resumed worker process did not die from SIGKILL: state=%v err=%v", pauseCmd.ProcessState, waitErr)
		}
		processResumed = true
	}
	if moves < 4 || inFlightMoves == 0 || collisions.Load() != 0 || (killKVLeader && !leaderKilled) || (isolateWorker && (!workerIsolated || !workerHealed || !isolatedInFlight)) || (killWorker && (!workerKilled || !workerRestarted || !killedInFlight)) || (killProcess && (!processKilled || !processKilledInFlight)) || (routePartition && (!routesIsolated || !routesHealed || !routeFaultInFlight)) || (pauseProcess && (!processPaused || !processResumed || !pauseStale)) {
		t.Fatalf("rebalance proof: moves=%d in_flight=%d collisions=%d leader_killed=%t worker_isolated=%t worker_healed=%t isolated_in_flight=%t worker_killed=%t worker_restarted=%t killed_in_flight=%t process_killed=%t process_killed_in_flight=%t routes_isolated=%t routes_healed=%t route_fault_in_flight=%t process_paused=%t process_resumed=%t pause_stale=%t", moves, inFlightMoves, collisions.Load(), leaderKilled, workerIsolated, workerHealed, isolatedInFlight, workerKilled, workerRestarted, killedInFlight, processKilled, processKilledInFlight, routesIsolated, routesHealed, routeFaultInFlight, processPaused, processResumed, pauseStale)
	}
	for index, id := range ids {
		result, err := clients[index%len(clients)].Await(ctx, typ, id)
		var count int
		if err == nil {
			err = json.Unmarshal(result, &count)
		}
		if err != nil || count != 50 {
			t.Fatalf("result %s=%d err=%v", id, count, err)
		}
	}
	for {
		info, err := run.Info(ctx)
		if err == nil && info.State.Msgs == 0 {
			break
		}
		if ctx.Err() != nil {
			inspectCtx, stopInspect := context.WithTimeout(context.Background(), 10*time.Second)
			last, lastErr := run.Info(inspectCtx)
			var consumers []string
			for partition := uint32(0); partition < 4; partition++ {
				name := fmt.Sprintf("WF_P_%02d", partition)
				consumer, consumerErr := run.Consumer(inspectCtx, name)
				if consumerErr != nil {
					consumers = append(consumers, fmt.Sprintf("%s lookup=%v", name, consumerErr))
					continue
				}
				state, stateErr := consumer.Info(inspectCtx)
				owner, _, ownerErr := assignments.Get(inspectCtx, partition)
				if stateErr != nil {
					consumers = append(consumers, fmt.Sprintf("%s info=%v owner=%s owner_err=%v", name, stateErr, owner, ownerErr))
					continue
				}
				consumers = append(consumers, fmt.Sprintf("%s pending=%d ack_pending=%d redelivered=%d owner=%s owner_err=%v", name, state.NumPending, state.NumAckPending, state.NumRedelivered, owner, ownerErr))
			}
			stopInspect()
			t.Fatalf("run queue not drained: last_info=%+v last_err=%v timed_out_info=%+v timed_out_err=%v consumers=%v", last, lastErr, info, err, consumers)
		}
		time.Sleep(50 * time.Millisecond)
	}
	report, err := integrity.Check(ctx, all[workerNodes[len(workerNodes)-1]])
	if err != nil || report.Terminal != len(ids) || report.Invocations != len(ids) {
		t.Fatalf("integrity=%+v err=%v", report, err)
	}
	t.Logf("completed=%d moves=%d in_flight_moves=%d kv_leader_killed=%t worker_isolated_45s=%t worker_killed_and_restarted=%t process_killed=%t server_routes_isolated_45s=%t process_paused_45s=%t pause_stale_append=%t journal_entries=%d", completed.Load(), moves, inFlightMoves, leaderKilled, workerHealed, workerRestarted, processKilled, routesHealed, processResumed, pauseStale, report.Entries)
}
