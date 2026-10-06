//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/history"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/internal/natsutil"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/reconcile"
	"js-wf/testcluster"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/anishathalye/porcupine"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type matrixPausedLease struct {
	Key      string    `json:"key"`
	Worker   string    `json:"worker_id"`
	Epoch    uint64    `json:"epoch"`
	Revision uint64    `json:"revision"`
	Created  time.Time `json:"created_at"`
	Observed time.Time `json:"observed_at"`
}

type matrixLeaderFault struct {
	StartGap              *fiveStartGapObservation                      `json:"start_gap,omitempty"`
	WorkerPingAt          time.Time                                     `json:"worker_ping_at,omitempty"`
	PausedLeases          []matrixPausedLease                           `json:"paused_leases,omitempty"`
	WorkerSelection       string                                        `json:"worker_selection,omitempty"`
	WorkerTarget          *matrixIsolationTarget                        `json:"worker_target,omitempty"`
	WorkerKillConfirmed   bool                                          `json:"worker_sigkill_confirmed,omitempty"`
	IsolationTarget       *matrixIsolationTarget                        `json:"isolation_target,omitempty"`
	UpgradeShutdownMode   string                                        `json:"upgrade_shutdown_mode,omitempty"`
	GracefulUpgrade       *testcluster.DockerGracefulUpgradeObservation `json:"graceful_upgrade,omitempty"`
	VersionsBefore        []string                                      `json:"versions_before,omitempty"`
	VersionsAfter         []string                                      `json:"versions_after,omitempty"`
	BlockDelay            *testcluster.BlockDelayProof                  `json:"block_delay,omitempty"`
	BlockStall            *testcluster.BlockStallProof                  `json:"block_stall,omitempty"`
	FanoutParent          string                                        `json:"fanout_parent,omitempty"`
	FanoutTail            uint64                                        `json:"fanout_tail,omitempty"`
	FanoutChildren        []string                                      `json:"fanout_children,omitempty"`
	FanoutPendingChildren []string                                      `json:"fanout_pending_children,omitempty"`
	Scheduled             time.Time                                     `json:"scheduled"`
	Killed                time.Time                                     `json:"killed"`
	Healed                time.Time                                     `json:"healed"`
	Node                  int                                           `json:"node"`
	Nodes                 []int                                         `json:"nodes,omitempty"`
	Routes                [3]int                                        `json:"partition_routes,omitempty"`
	MajoritySequence      uint64                                        `json:"majority_sequence,omitempty"`
	Consumer              string                                        `json:"consumer,omitempty"`
	Pending               uint64                                        `json:"pending,omitempty"`
	AckPending            int                                           `json:"ack_pending,omitempty"`
	Worker                string                                        `json:"worker,omitempty"`
	WorkerSlot            *int                                          `json:"worker_slot,omitempty"`
	PID                   int                                           `json:"pid,omitempty"`
	ActiveLeases          int                                           `json:"active_leases,omitempty"`
	Paused                time.Time                                     `json:"paused,omitempty"`
	Resumed               time.Time                                     `json:"resumed,omitempty"`
	FencingEvents         int                                           `json:"fencing_events,omitempty"`
	ProxyBefore           *testcluster.ClientProxyStats                 `json:"proxy_before,omitempty"`
	ProxyBlocked          *testcluster.ClientProxyStats                 `json:"proxy_blocked,omitempty"`
	ProxyHealed           *testcluster.ClientProxyStats                 `json:"proxy_healed,omitempty"`
	ServerClocks          []matrixServerClockSample                     `json:"server_clocks,omitempty"`
	ClockSamples          []matrixWorkerClockSample                     `json:"clock_samples,omitempty"`
	Error                 string                                        `json:"error,omitempty"`
}

type matrixLatencySample struct {
	ServerClockOffset time.Duration `json:"server_clock_offset_ns,omitempty"`
	ObservedLower     *time.Time    `json:"observed_lower,omitempty"`
	Type              string        `json:"type"`
	ID                string        `json:"id"`
	Event             string        `json:"event"`
	Enabled           time.Time     `json:"enabled"`
	Observed          time.Time     `json:"observed"`
	Delay             time.Duration `json:"delay_ns"`
}

// One sustained row of the release matrix. Shortened runs are smoke evidence;
// the default exercises ten minutes on the same stores and worker fleet.
func TestMixedMatrixJournalLeaderEveryThirtySeconds(t *testing.T) {
	runMixedMatrixLeader(t, "journal_leader")
}

func TestMixedMatrixConsumerLeaderEveryThirtySeconds(t *testing.T) {
	runMixedMatrixLeader(t, "consumer_leader")
}

func TestMixedMatrixAllServersKilledEveryThirtySeconds(t *testing.T) {
	runMixedMatrixLeader(t, "all_servers")
}

func TestMixedMatrixServerPartitionEveryThirtySeconds(t *testing.T) {
	runMixedMatrixLeader(t, "server_partition")
}

func TestMixedMatrixRandomWorkerKilledEveryFiveSeconds(t *testing.T) {
	runMixedMatrixLeader(t, "worker_kill")
}

func TestMixedMatrixWorkerPausedFortyFiveSeconds(t *testing.T) {
	runMixedMatrixLeader(t, "worker_pause")
}

func TestMixedMatrixWorkerReplyIsolationFortyFiveSeconds(t *testing.T) {
	runMixedMatrixLeader(t, "worker_isolation")
}

func TestMixedMatrixWorkerClockSkew(t *testing.T) { runMixedMatrixLeader(t, "worker_clock") }

func TestMixedMatrixRollingServerUpgrade(t *testing.T) { runMixedMatrixLeader(t, "rolling_upgrade") }

func TestMixedMatrixBlockDiskStallEveryThirtySeconds(t *testing.T) {
	runMixedMatrixLeader(t, "block_disk")
}

func TestMixedMatrixFanoutRestartEveryThirtySeconds(t *testing.T) {
	runMixedMatrixLeader(t, "fanout_restart")
}

func TestMixedMatrixServerClockSkewPositive(t *testing.T) {
	runMixedMatrixLeader(t, "server_clock_plus")
}
func TestMixedMatrixServerClockSkewNegative(t *testing.T) {
	runMixedMatrixLeader(t, "server_clock_minus")
}

func runMixedMatrixLeader(t *testing.T, row string) {
	runMixedMatrixLeaderWithChallenge(t, row, "")
}

func runMixedMatrixLeaderWithChallenge(t *testing.T, row, mutationMode string) {
	t.Helper()
	if mutationMode != "" && row != "journal_leader" {
		t.Fatal("sustained mutation challenge requires the journal-leader row")
	}
	if os.Getenv("WF_MATRIX_CHAOS") != "1" {
		t.Skip("set WF_MATRIX_CHAOS=1 for sustained mixed matrix chaos")
	}
	duration := 10 * time.Minute
	if raw := os.Getenv("WF_MATRIX_DURATION"); raw != "" {
		var err error
		duration, err = time.ParseDuration(raw)
		if err != nil || duration < 35*time.Second || duration > 10*time.Minute {
			t.Fatalf("WF_MATRIX_DURATION=%q: want 35s..10m", raw)
		}
	}
	// The matrix allows five minutes after the final heal and retains artifacts
	// during cleanup. Fail before starting processes if Go would kill the test
	// before those gates can finish.
	if deadline, ok := t.Deadline(); ok && time.Until(deadline) < duration+6*time.Minute {
		t.Fatalf("test timeout cannot cover matrix duration and recovery; use -timeout=%s or longer", duration+6*time.Minute)
	}
	seed, err := testcluster.SeedFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	var clockBinaries []string
	if row == "worker_clock" {
		clockRoot := t.TempDir()
		if base := os.Getenv("WF_MATRIX_PROCESS_ROOT"); base != "" {
			if err := os.MkdirAll(base, 0700); err != nil {
				t.Fatal(err)
			}
			clockRoot = filepath.Join(base, strings.ReplaceAll(t.Name(), "/", "_")+"-worker-clocks")
			if err := os.Mkdir(clockRoot, 0700); err != nil {
				t.Fatal(err)
			}
			t.Logf("retained worker clock builds=%s", clockRoot)
		}
		clockBinaries, err = buildMatrixClockWorkers(clockRoot)
		if err != nil {
			t.Fatal(err)
		}
	}
	serverOffset := time.Duration(0)
	if row == "server_clock_plus" {
		serverOffset = 60 * time.Second
	}
	if row == "server_clock_minus" {
		serverOffset = -60 * time.Second
	}
	var blockDisk *testcluster.BlockDisk
	defer func() {
		if blockDisk != nil {
			if err := blockDisk.Close(); err != nil {
				t.Errorf("block disk cleanup: %v", err)
			}
		}
	}()
	startCluster := testcluster.StartProcesses
	if row == "rolling_upgrade" {
		oldBinary := os.Getenv("WF_NATS_SERVER_BIN")
		if oldBinary == "" {
			t.Fatal("rolling upgrade row requires WF_NATS_SERVER_BIN pointing to NATS 2.11.17")
		}
		startCluster = func(root string, count int) (*testcluster.ProcessCluster, error) {
			return testcluster.StartMixedVersionProcesses(root, []string{oldBinary, oldBinary, oldBinary})
		}
	} else if row == "block_disk" {
		startCluster = func(root string, count int) (*testcluster.ProcessCluster, error) {
			var err error
			blockDisk, err = testcluster.NewBlockDisk(root)
			if err != nil {
				return nil, err
			}
			if os.Getenv("WF_MATRIX_PROCESS_ROOT") != "" {
				blockDisk.RetainMediaOnClose()
				t.Logf("retained block filesystem image=%s", blockDisk.ImagePath())
			}
			if err := os.Symlink(blockDisk.StoreDir, filepath.Join(root, "node-2")); err != nil {
				return nil, err
			}
			return testcluster.StartProcesses(root, count)
		}
	} else if serverOffset != 0 {
		startCluster = func(root string, count int) (*testcluster.ProcessCluster, error) {
			return testcluster.StartClockSkewProcesses(root, count, 2, serverOffset)
		}
	} else if row == "server_partition" {
		startCluster = testcluster.StartPartitionableProcesses
	} else if row == "worker_isolation" {
		startCluster = testcluster.StartProfiledProcesses
	}
	clusterRoot := t.TempDir()
	if base := os.Getenv("WF_MATRIX_PROCESS_ROOT"); base != "" {
		// Retain original process stores for a focused campaign. Refuse an
		// existing case directory so a replay cannot silently reuse old state.
		if err := os.MkdirAll(base, 0700); err != nil {
			t.Fatal(err)
		}
		clusterRoot = filepath.Join(base, strings.ReplaceAll(t.Name(), "/", "_"))
		if err := os.Mkdir(clusterRoot, 0700); err != nil {
			t.Fatal(err)
		}
		t.Logf("retained matrix process stores=%s", clusterRoot)
	}
	cluster, err := startCluster(clusterRoot, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	defer func() {
		if !t.Failed() {
			return
		}
		for node := range cluster.Commands {
			data, err := os.ReadFile(cluster.LogPath(node))
			if err != nil {
				t.Logf("server %d log: %v", node, err)
				continue
			}
			if len(data) > 4096 {
				data = data[len(data)-4096:]
			}
			t.Logf("server %d log tail:\n%s", node, data)
		}
	}()
	if row == "rolling_upgrade" {
		for node, version := range matrixClusterVersions(cluster) {
			if version != "2.11.17" {
				t.Fatalf("initial node %d version=%s want=2.11.17", node, version)
			}
		}
	}
	urls := make([]string, 3)
	for i := range urls {
		urls[i] = cluster.ClientURL(i)
	}
	clientURLs := urls
	if row == "server_partition" {
		clientURLs = urls[:2]
	}
	connectOptions := []nats.Option{nats.MaxReconnects(-1), nats.ReconnectWait(100 * time.Millisecond), nats.Timeout(time.Second)}
	if row == "server_partition" {
		connectOptions = append(connectOptions, nats.IgnoreDiscoveredServers())
	}
	nc, err := nats.Connect(strings.Join(clientURLs, ","), connectOptions...)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), duration+6*time.Minute)
	defer cancel()
	ready, stopReady := context.WithTimeout(ctx, 30*time.Second)
	for ready.Err() == nil {
		attempt, stop := context.WithTimeout(ready, 3*time.Second)
		if row == "rolling_upgrade" {
			var backend provision.TimerBackend
			backend, err = provision.EnsureAuto(attempt, js, 3)
			if err == nil && backend != provision.FallbackTimers {
				err = fmt.Errorf("upgrade requires fallback timer backend: %s", backend)
			}
		} else {
			err = provision.Ensure(attempt, js, 3)
		}
		stop()
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	stopReady()
	if err != nil {
		t.Fatal(err)
	}
	if row == "server_partition" {
		if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "MATRIX_ROUTE_PROBE", Subjects: []string{"matrix.route.probe"}, Storage: jetstream.FileStorage, Replicas: 3}); err != nil {
			t.Fatal(err)
		}
	}
	if row == "worker_clock" {
		if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "MATRIX_CLOCK", Subjects: []string{"matrix.clock.*"}, Storage: jetstream.FileStorage, Replicas: 3, MaxMsgsPerSubject: 16}); err != nil {
			t.Fatal(err)
		}
	}
	if serverOffset != 0 {
		if err := preferMatrixNodeTwoLeaders(ctx, nc, js); err != nil {
			t.Fatal(err)
		}
		if proof, err := verifyMatrixServerClocks(ctx, js, cluster, serverOffset, time.Now()); err != nil {
			t.Fatal(err)
		} else {
			t.Logf("initial server clock proof=%+v", proof.ServerClocks)
		}
	}
	var fanoutBarrier matrixFanoutBarrier
	var recorder history.Recorder
	var dispatchMu sync.Mutex
	var dispatch []worker.DispatchEvent
	var operationMu sync.Mutex
	var operations []worker.OperationEvent
	var latencySamples []matrixLatencySample
	var workerRoot string
	var processWorkers []*matrixProcessWorker
	var workerProxies []*testcluster.ClientProxy
	c := client.NewObserved(js, &recorder)
	var faults []matrixLeaderFault
	var faultsMu sync.Mutex
	defer func() {
		prefix := os.Getenv("MATRIX_ARTIFACT_PREFIX")
		if prefix == "" {
			prefix = filepath.Join(t.TempDir(), "matrix-artifacts")
			t.Logf("matrix artifacts=%s", prefix)
		}
		file, err := os.Create(prefix + "-history.jsonl")
		if err != nil {
			t.Errorf("history artifact: %v", err)
			return
		}
		if err := recorder.WriteJSONL(file); err != nil {
			t.Errorf("history artifact: %v", err)
		}
		_ = file.Close()
		file, err = os.Create(prefix + "-dispatch.jsonl")
		if err != nil {
			t.Errorf("dispatch artifact: %v", err)
		} else {
			dispatchMu.Lock()
			encoder := json.NewEncoder(file)
			for _, event := range dispatch {
				if err := encoder.Encode(event); err != nil {
					t.Errorf("dispatch artifact: %v", err)
					break
				}
			}
			dispatchMu.Unlock()
			_ = file.Close()
		}
		if os.Getenv("WF_MATRIX_OPERATION_TIMINGS") == "1" {
			file, err = os.Create(prefix + "-operations.jsonl")
			if err != nil {
				t.Errorf("operation artifact: %v", err)
			} else {
				operationMu.Lock()
				encoder := json.NewEncoder(file)
				for _, event := range operations {
					if err := encoder.Encode(event); err != nil {
						t.Errorf("operation artifact: %v", err)
						break
					}
				}
				operationMu.Unlock()
				_ = file.Close()
			}
		}
		faultsMu.Lock()
		data, err := json.MarshalIndent(struct {
			Seed     int64               `json:"seed"`
			Duration string              `json:"duration"`
			Faults   []matrixLeaderFault `json:"faults"`
		}{seed, duration.String(), faults}, "", "  ")
		faultsMu.Unlock()
		if err == nil {
			err = os.WriteFile(prefix+"-faults.json", append(data, '\n'), 0644)
		}
		if err != nil {
			t.Errorf("fault artifact: %v", err)
		}
		data, err = json.MarshalIndent(latencySamples, "", "  ")
		if err == nil {
			err = os.WriteFile(prefix+"-latencies.json", append(data, '\n'), 0644)
		}
		if err != nil {
			t.Errorf("latency artifact: %v", err)
		}
		if workerRoot != "" {
			files, readErr := os.ReadDir(workerRoot)
			if readErr != nil {
				t.Errorf("worker artifacts: %v", readErr)
			} else {
				for _, file := range files {
					if !file.IsDir() {
						data, err := os.ReadFile(filepath.Join(workerRoot, file.Name()))
						if err == nil && strings.Contains(string(data), "--- FAIL: TestMixedMatrixWorkerProcessChild") {
							t.Errorf("worker subprocess failure in %s", file.Name())
						}
						if err == nil && strings.Contains(string(data), "WARNING: DATA RACE") {
							t.Errorf("worker race detector failure in %s", file.Name())
						}
						if err == nil {
							err = os.WriteFile(prefix+"-"+file.Name(), data, 0644)
						}
						if err != nil {
							t.Errorf("worker artifact %s: %v", file.Name(), err)
						}
					}
				}
			}
		}
		if t.Failed() {
			for i := range urls {
				if data, err := os.ReadFile(cluster.LogPath(i)); err == nil {
					_ = os.WriteFile(fmt.Sprintf("%s-server-%d.log", prefix, i), data, 0644)
				}
			}
		}
	}()
	workCtx, stopWork := context.WithCancel(ctx)
	var fleet sync.WaitGroup
	defer func() { stopWork(); fleet.Wait() }()
	fleetErrors := make(chan error, provision.Partitions+3)
	launch := func(run func() error) {
		fleet.Add(1)
		go func() {
			defer fleet.Done()
			if err := run(); err != nil && workCtx.Err() == nil {
				fleetErrors <- err
				cancel()
			}
		}()
	}
	if row == "worker_kill" || row == "worker_pause" || row == "worker_isolation" || row == "worker_clock" {
		workerRoot = t.TempDir()
		processWorkers = make([]*matrixProcessWorker, 3)
		if row == "worker_isolation" {
			workerProxies = make([]*testcluster.ClientProxy, 3)
			defer func() {
				for _, proxy := range workerProxies {
					if proxy != nil {
						proxy.Close()
					}
				}
			}()
			for index := range workerProxies {
				workerProxies[index], err = testcluster.NewClientProxy(urls[index])
				if err != nil {
					t.Fatal(err)
				}
				if err := workerProxies[index].EnableTrafficTrace(4 << 20); err != nil {
					t.Fatal(err)
				}
			}
		}
		defer func() {
			for _, process := range processWorkers {
				stopMatrixProcessWorker(process)
			}
		}()
		for index := range processWorkers {
			workerURLs := urls
			if row == "worker_isolation" {
				workerURLs = []string{workerProxies[index].URL()}
			}
			if row == "worker_clock" {
				processWorkers[index], err = startMatrixProcessWorkerExecutable(ctx, workerRoot, workerURLs, index, 0, clockBinaries[index], true)
			} else {
				processWorkers[index], err = startMatrixProcessWorker(ctx, workerRoot, workerURLs, index, 0)
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		if row == "worker_clock" {
			if proof, err := verifyMatrixWorkerClocks(processWorkers, time.Now()); err != nil {
				t.Fatal(err)
			} else {
				t.Logf("initial worker clock proofs=%+v", proof.ClockSamples)
			}
		}
	} else {
		handlers := matrixLeaderHandlers()
		if row == "fanout_restart" {
			handlers = fanoutBarrier.handlers()
		}
		options := []worker.Option{worker.WithPartitionConcurrency(4), worker.WithDispatchObserver(func(event worker.DispatchEvent) {
			dispatchMu.Lock()
			dispatch = append(dispatch, event)
			dispatchMu.Unlock()
		})}
		if os.Getenv("WF_MATRIX_OPERATION_TIMINGS") == "1" {
			options = append(options, worker.WithOperationObserver(func(event worker.OperationEvent) {
				operationMu.Lock()
				operations = append(operations, event)
				operationMu.Unlock()
			}))
		}
		w, err := worker.New(ctx, js, "matrix-worker", handlers, options...)
		if err != nil {
			t.Fatal(err)
		}
		for partition := uint32(0); partition < provision.Partitions; partition++ {
			launch(func() error { return w.RunPartition(workCtx, partition) })
		}
	}
	launch(func() error { return reconcile.RunStartLoop(workCtx, js, "matrix-start", time.Second, 32) })
	launch(func() error { return reconcile.RunSignalLoop(workCtx, js, "matrix-signal", time.Second, 32) })
	launch(func() error { return reconcile.RunSuspendedLoop(workCtx, js, "matrix-suspended", time.Second, 8) })
	if row == "rolling_upgrade" {
		launch(func() error {
			return reconcile.RunFallbackTimerLoop(workCtx, js, "matrix-upgrade-timers", 100*time.Millisecond, 100)
		})
	}
	start := time.Now()
	end := start.Add(duration)
	faultInterval := 30 * time.Second
	if row == "worker_kill" {
		faultInterval = 5 * time.Second
	}
	firstFault := faultInterval
	if row == "worker_pause" || row == "worker_isolation" {
		faultInterval = 60 * time.Second
		firstFault = 5 * time.Second
	}
	if row == "rolling_upgrade" && duration > 2*firstFault {
		faultInterval = (duration - 2*firstFault) / 2
		if faultInterval < 30*time.Second {
			faultInterval = 30 * time.Second
		}
	}
	faultCtx, stopFault := context.WithCancel(ctx)
	faultDone := make(chan error, 1)
	faultExited := make(chan struct{})
	faultRNG := rand.New(rand.NewSource(seed ^ 0x6c6561646572))
	upgradeOrder := rand.New(rand.NewSource(seed ^ 0x75706772616465)).Perm(3)
	go func() {
		defer close(faultExited)
		defer close(faultDone)
		upgradeIndex := 0
		for scheduled := start.Add(firstFault); scheduled.Before(end); scheduled = scheduled.Add(faultInterval) {
			if row == "rolling_upgrade" && upgradeIndex == 3 {
				break
			}
			timer := time.NewTimer(time.Until(scheduled))
			select {
			case <-faultCtx.Done():
				timer.Stop()
				faultDone <- faultCtx.Err()
				return
			case <-timer.C:
			}
			var event matrixLeaderFault
			var err error
			if row == "rolling_upgrade" {
				event, err = upgradeMatrixServer(faultCtx, js, cluster, upgradeOrder[upgradeIndex], scheduled)
				upgradeIndex++
			} else if row == "block_disk" {
				event, err = stallMatrixBlockDisk(faultCtx, nc, js, blockDisk, scheduled)
			} else if serverOffset != 0 {
				event, err = verifyMatrixServerClocks(faultCtx, js, cluster, serverOffset, scheduled)
			} else if row == "worker_clock" {
				event, err = verifyMatrixWorkerClocks(processWorkers, scheduled)
			} else if row == "worker_isolation" {
				event, err = isolateMatrixWorkerReplies(faultCtx, processWorkers, workerProxies, faultRNG.Intn(len(processWorkers)), scheduled)
			} else if row == "worker_pause" {
				event, err = pauseMatrixProcessWorker(faultCtx, js, processWorkers, faultRNG.Intn(len(processWorkers)), scheduled)
			} else if row == "worker_kill" {
				event, err = killMatrixProcessWorker(faultCtx, workerRoot, urls, processWorkers, faultRNG.Intn(len(processWorkers)), scheduled)
			} else if row == "server_partition" {
				event, err = partitionMatrixServer(faultCtx, js, cluster, scheduled)
			} else if row == "fanout_restart" {
				event, err = fanoutBarrier.restart(faultCtx, js, cluster, scheduled)
			} else if row == "all_servers" {
				event, err = killMatrixAllServers(faultCtx, js, cluster, scheduled)
			} else if row == "consumer_leader" {
				event, err = killMatrixConsumerLeader(faultCtx, js, cluster, scheduled, faultRNG)
			} else {
				event, err = killMatrixJournalLeader(faultCtx, js, cluster, scheduled)
			}
			if err != nil {
				event.Error = err.Error()
			}
			faultsMu.Lock()
			faults = append(faults, event)
			faultsMu.Unlock()
			if err != nil {
				t.Logf("%s fault failed: %v", row, err)
				if row == "worker_isolation" {
					captureMatrixIsolationDiagnostics(t, cluster, workerProxies, workerRoot)
				}
				faultDone <- err
				cancel()
				return
			}
			if row == "worker_pause" {
				t.Logf("worker pause worker=%s confirmed=%s resumed=%s pause_duration=%s fencing_events=%d", event.Worker, event.Paused.Sub(start), event.Resumed.Sub(start), event.Resumed.Sub(event.Paused), event.FencingEvents)
			}
			if serverOffset != 0 {
				t.Logf("server clock verification scheduled=%s samples=%+v", event.Scheduled.Sub(start), event.ServerClocks)
				continue
			}
			if row == "worker_clock" {
				t.Logf("worker clock verification scheduled=%s samples=%+v", event.Scheduled.Sub(start), event.ClockSamples)
				continue
			}
			t.Logf("%s fault node=%d nodes=%v worker=%s pid=%d active_leases=%d routes=%v majority_seq=%d consumer=%s pending=%d ack_pending=%d scheduled=%s killed=%s healed=%s", row, event.Node, event.Nodes, event.Worker, event.PID, event.ActiveLeases, event.Routes, event.MajoritySequence, event.Consumer, event.Pending, event.AckPending, event.Scheduled.Sub(start), event.Killed.Sub(start), event.Healed.Sub(start))
		}
	}()
	defer func() { stopFault(); <-faultExited }()
	// Audit completed cohorts on one independent reader. The workload keeps
	// producing active executions while the controller schedules pause faults.
	type checkpoint struct {
		batch  int
		cutoff uint64
	}
	checkpoints := make(chan checkpoint, 128)
	checkpointErrors := make(chan error, 1)
	checkpointCtx, stopCheckpoints := context.WithCancel(ctx)
	checkpointDone := make(chan struct{})
	go func() {
		defer close(checkpointDone)
		for {
			select {
			case <-checkpointCtx.Done():
				return
			case cut, ok := <-checkpoints:
				if !ok {
					return
				}
				t.Logf("checkpoint audit batch=%d invocation_cutoff=%d started elapsed=%s", cut.batch, cut.cutoff, time.Since(start))
				report, err := matrixRetainedAuditUsing(checkpointCtx, func(attempt context.Context) (integrity.Report, error) {
					return integrity.CheckThroughInvocationSequence(attempt, js, cut.cutoff)
				})
				if checkpointCtx.Err() != nil {
					return
				}
				if err != nil || report.Invocations != cut.batch*28 || report.Journals != cut.batch*28 || report.Terminal != cut.batch*28 {
					checkpointErrors <- fmt.Errorf("intermediate retained audit batch=%d cutoff=%d report=%+v err=%v", cut.batch, cut.cutoff, report, err)
					cancel()
					return
				}
				t.Logf("checkpoint audit batch=%d complete report=%+v elapsed=%s", cut.batch, report, time.Since(start))
			}
		}
	}()
	defer func() { stopCheckpoints(); <-checkpointDone }()
	rng := rand.New(rand.NewSource(seed))
	var latencies []time.Duration
	latenciesByType := map[string][]time.Duration{}
	progressByType := map[string][]time.Duration{}
	batches := 0
	for time.Now().Before(end) {
		select {
		case err := <-faultDone:
			if err != nil {
				t.Fatalf("leader fault: %v", err)
			}
		case err := <-fleetErrors:
			t.Fatalf("fleet: %v", err)
		case err := <-checkpointErrors:
			t.Fatal(err)
		default:
		}
		kinds := []string{"matrixshort", "matrixshort", "matrixshort", "matrixshort", "matrixtimer", "matrixtimer", "matrixtimer", "matrixsignal", "matrixsignal", "matrixfanout"}
		rng.Shuffle(len(kinds), func(i, j int) { kinds[i], kinds[j] = kinds[j], kinds[i] })
		var batch sync.WaitGroup
		batchCtx, stopBatch := context.WithTimeout(ctx, 5*time.Minute)
		batchErrors := make(chan error, len(kinds))
		var cohort []matrixStallTarget
		for i, typ := range kinds {
			id := fmt.Sprintf("seed-%d-batch-%d-%d", seed, batches, i)
			cohort = append(cohort, matrixStallTarget{Type: typ, ID: id})
			if row == "fanout_restart" && typ == "matrixfanout" {
				fanoutBarrier.noteParent(id)
			}
			batch.Add(1)
			go func() {
				defer batch.Done()
				if err := matrixRetryClient(batchCtx, func(attempt context.Context) error {
					_, err := c.Start(attempt, typ, id, []byte(`null`))
					if errors.Is(err, client.ErrAlreadyStarted) {
						return nil
					}
					return err
				}); err != nil {
					batchErrors <- fmt.Errorf("start %s/%s: %w", typ, id, err)
					return
				}
				if typ == "matrixsignal" {
					for n := 0; n < 8; n++ {
						if err := matrixRetryClient(batchCtx, func(attempt context.Context) error {
							_, err := c.Signal(attempt, typ, id, "go", []byte(strconv.Itoa(n)), fmt.Sprintf("signal-%d", n))
							return err
						}); err != nil {
							batchErrors <- err
							return
						}
					}
				}
				value, err := c.Await(batchCtx, typ, id)
				if err != nil || string(value) != "42" {
					batchErrors <- fmt.Errorf("result %s/%s=%s err=%v", typ, id, value, err)
				}
			}()
		}
		batch.Wait()
		stopBatch()
		if ctx.Err() != nil {
			select {
			case err := <-checkpointErrors:
				t.Fatal(err)
			default:
			}
			select {
			case err := <-fleetErrors:
				t.Fatalf("fleet: %v", err)
			default:
			}
			select {
			case err := <-faultDone:
				if err != nil {
					t.Fatalf("leader fault: %v", err)
				}
			default:
			}
		}
		close(batchErrors)
		for err := range batchErrors {
			dispatchMu.Lock()
			observed := append([]worker.DispatchEvent(nil), dispatch...)
			dispatchMu.Unlock()
			captureMatrixStallDiagnostics(t, js, observed, cohort, err)
			t.Fatal(err)
		}
		batches++
		if batches%10 == 0 {
			stream, err := matrixReadMetadata(ctx, func(attempt context.Context) (jetstream.Stream, error) { return js.Stream(attempt, "WF_INV") })
			if err != nil {
				t.Fatal(err)
			}
			info, err := matrixReadMetadata(ctx, func(attempt context.Context) (*jetstream.StreamInfo, error) { return stream.Info(attempt) })
			if err != nil {
				t.Fatal(err)
			}
			select {
			case checkpoints <- checkpoint{batch: batches, cutoff: info.State.LastSeq}:
			case <-ctx.Done():
				t.Fatalf("enqueue checkpoint: %v", ctx.Err())
			}
		}

		t.Logf("mixed batch=%d elapsed=%s", batches, time.Since(start))
	}
	close(checkpoints)
	<-checkpointDone
	select {
	case err := <-checkpointErrors:
		t.Fatal(err)
	default:
	}
	if err := <-faultDone; err != nil {
		t.Fatalf("leader fault: %v", err)
	}
	wantFaults := 0
	if duration > firstFault {
		wantFaults = 1 + int((duration-firstFault-time.Nanosecond)/faultInterval)
	}
	if row == "rolling_upgrade" && wantFaults > 3 {
		wantFaults = 3
	}
	if len(faults) != wantFaults {
		t.Fatalf("faults=%d want=%d", len(faults), wantFaults)
	}
	if row == "worker_kill" || row == "worker_pause" || row == "worker_isolation" {
		activeKills := 0
		for _, fault := range faults {
			if fault.ActiveLeases > 0 {
				activeKills++
			}
		}
		t.Logf("worker faults=%d active_worker_faults=%d row=%s", len(faults), activeKills, row)
		if activeKills == 0 {
			t.Fatal("worker fault row did not fault any worker with an active lease")
		}
	}
	if serverOffset != 0 {
		if _, err := verifyMatrixServerClocks(ctx, js, cluster, serverOffset, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	if row == "fanout_restart" {
		if err := verifyMatrixRestartFanouts(ctx, js, faults); err != nil {
			t.Fatal(err)
		}
	}
	completionDeadline := faults[len(faults)-1].Healed.Add(5 * time.Minute)
	// Terminal audits include all children and grandchildren, not just parents.
	inv, err := js.Stream(ctx, "WF_INV")
	if err != nil {
		t.Fatal(err)
	}
	info, err := inv.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	invocationCutoff := info.State.LastSeq
	for sequence := info.State.FirstSeq; sequence <= info.State.LastSeq; sequence++ {
		msg, err := inv.GetMsg(ctx, sequence)
		if err != nil {
			t.Fatal(err)
		}
		parts := strings.Split(msg.Subject, ".")
		attempt, done := context.WithTimeout(ctx, 20*time.Second)
		samples, err := matrixInvocationLatenciesWithClock(attempt, js, parts[2], parts[3], msg.Time, completionDeadline, serverOffset)
		done()
		if err != nil {
			t.Fatal(err)
		}
		latencySamples = append(latencySamples, samples...)
		for _, sample := range samples {
			if sample.Event == "terminal" {
				latencies = append(latencies, sample.Delay)
				latenciesByType[parts[2]] = append(latenciesByType[parts[2]], sample.Delay)
			} else {
				progressByType[parts[2]] = append(progressByType[parts[2]], sample.Delay)
			}
		}
	}
	report, err := matrixRetainedAudit(ctx, js)
	want := batches * 28
	if err != nil || report.Invocations != want || report.Journals != want || report.Terminal != want {
		t.Fatalf("mixed retained state=%+v want=%d err=%v", report, want, err)
	}
	t.Logf("MATRIX_RETAINED row=%s report=%+v expected_invocations=%d", row, report, want)
	for _, check := range []func([]client.Operation, time.Duration) (porcupine.CheckResult, error){history.CheckStarts, history.CheckSignals, history.CheckResults} {
		result, err := check(recorder.Snapshot(), 30*time.Second)
		if err != nil || result != porcupine.Ok {
			t.Fatalf("history=%v err=%v", result, err)
		}
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p99 := latencies[(len(latencies)*99+99)/100-1]
	t.Logf("MATRIX_RESULT row=%s seed=%d duration=%s release_duration=%t batches=%d invocations=%d faults=%d terminal_p99=%s", row, seed, duration, duration == 10*time.Minute, batches, want, len(faults), p99)
	if p99 >= 30*time.Second {
		t.Fatalf("terminal p99=%s, want <30s", p99)
	}
	for _, typ := range []string{"matrixshort", "matrixtimer", "matrixsignal", "matrixfanout", "matrixchild", "matrixgrandchild"} {
		values := latenciesByType[typ]
		if len(values) == 0 {
			t.Fatalf("missing workload %s", typ)
		}
		sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
		p99 := values[(len(values)*99+99)/100-1]
		t.Logf("MATRIX_CELL type=%s invocations=%d terminal_p99=%s", typ, len(values), p99)
		if p99 >= 30*time.Second {
			t.Errorf("%s terminal p99=%s, want <30s", typ, p99)
		}
		progress := progressByType[typ]
		sort.Slice(progress, func(i, j int) bool { return progress[i] < progress[j] })
		progressP99 := progress[(len(progress)*99+99)/100-1]
		var aboveThirty int
		for _, delay := range progress {
			if delay >= 30*time.Second {
				aboveThirty++
			}
		}
		t.Logf("MATRIX_PROGRESS type=%s enabled_events=%d progress_p99=%s progress_max=%s above_30s=%d", typ, len(progress), progressP99, progress[len(progress)-1], aboveThirty)
		if progressP99 >= 30*time.Second {
			t.Errorf("%s progress p99=%s, want <30s", typ, progressP99)
		}
	}
	run, err := js.Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	drainCtx, stopDrain := context.WithTimeout(ctx, 30*time.Second)
	defer stopDrain()
	for {
		attempt, done := context.WithTimeout(drainCtx, 2*time.Second)
		info, err := run.Info(attempt)
		done()
		if err == nil && info.State.Msgs == 0 {
			break
		}
		if drainCtx.Err() != nil {
			if row == "rolling_upgrade" {
				if err := recordMatrixPeerQueues(cluster, "drain-failed"); err != nil {
					t.Logf("physical peer queue diagnostic: %v", err)
				}
			}
			captureMatrixQueueDiagnostics(t, run)
			t.Fatalf("run queue did not drain: info=%+v err=%v", info, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if row == "rolling_upgrade" {
		if err := recordMatrixPeerQueues(cluster, "drained"); err != nil {
			t.Errorf("physical peer queue diagnostic: %v", err)
		}
	}
	stopWork()
	fleet.Wait()
	if mutationMode != "" {
		if t.Failed() {
			t.Fatal("sustained row failed a release gate before mutation admission")
		}
		challengeSustainedMixedMutation(t, ctx, cluster, js, mutationMode, seed, duration, time.Since(start), batches, len(faults), invocationCutoff, report)
	}
}

// Lost read replies during a leader kill must not consume the entire workload
// duration. Every attempt still checks the whole retained state; only named
// transient transport failures get a fresh context, never invariant errors.
func matrixRetainedAudit(ctx context.Context, js jetstream.JetStream) (integrity.Report, error) {
	return matrixRetainedAuditUsing(ctx, func(attempt context.Context) (integrity.Report, error) {
		return matrixRetainedCheck(attempt, js, nil)
	})
}

// Select the same explicit reader for checkpoint and final full audits.
func matrixRetainedCheck(ctx context.Context, js jetstream.JetStream, cutoff *uint64) (integrity.Report, error) {
	streaming := os.Getenv("WF_TIER3_STREAMING_STATE_RETAINED_AUDIT") == "1"
	batched := os.Getenv("WF_TIER3_BATCHED_RETAINED_AUDIT") == "1"
	concurrent := os.Getenv("WF_TIER3_CONCURRENT_STATE_RETAINED_AUDIT") == "1"
	chunked := os.Getenv("WF_TIER3_CHUNKED_STATE_RETAINED_AUDIT") == "1"
	if (streaming && batched) || (concurrent && (streaming || batched)) || (chunked && (streaming || batched || concurrent)) {
		return integrity.Report{}, errors.New("conflicting retained audit modes")
	}
	if cutoff != nil {
		if chunked {
			return integrity.CheckThroughInvocationSequenceWithChunkedConcurrentStateReads(ctx, js, *cutoff)
		}
		if concurrent {
			return integrity.CheckThroughInvocationSequenceWithConcurrentStreamingStateReads(ctx, js, *cutoff)
		}
		if streaming {
			return integrity.CheckThroughInvocationSequenceWithStreamingStateReads(ctx, js, *cutoff)
		}
		if batched {
			return integrity.CheckThroughInvocationSequenceWithBatchedReads(ctx, js, *cutoff)
		}
		return integrity.CheckThroughInvocationSequence(ctx, js, *cutoff)
	}
	if chunked {
		return integrity.CheckWithChunkedConcurrentStateReads(ctx, js)
	}
	if concurrent {
		return integrity.CheckWithConcurrentStreamingStateReads(ctx, js)
	}
	if streaming {
		return integrity.CheckWithStreamingStateReads(ctx, js)
	}
	if batched {
		return integrity.CheckWithBatchedReads(ctx, js)
	}
	return integrity.Check(ctx, js)
}

func matrixRetainedAuditUsing(ctx context.Context, check func(context.Context) (integrity.Report, error)) (integrity.Report, error) {
	return matrixRetainedAuditWithClock(ctx, check, time.Now, func(ctx context.Context, d time.Duration) error {
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return nil
		}
	})
}

// Keep the original three twenty-second attempts inside their sixty-second
// maximum. Quick 503 replies must not exhaust all attempts before fault heal.
// The clock seam makes this harness recovery decision deterministic in tests.
func matrixRetainedAuditWithClock(ctx context.Context, check func(context.Context) (integrity.Report, error), now func() time.Time, wait func(context.Context, time.Duration) error) (integrity.Report, error) {
	bound, stop := context.WithTimeout(ctx, 60*time.Second)
	defer stop()
	var report integrity.Report
	var err error
	for i := 0; i < 3; i++ {
		started := now()
		attempt, done := context.WithTimeout(bound, 20*time.Second)
		report, err = check(attempt)
		done()
		if err == nil {
			return report, nil
		}
		if bound.Err() != nil || !matrixTransientTransport(err) {
			break
		}
		if i < 2 {
			if pause := 10*time.Second - now().Sub(started); pause > 0 {
				if waitErr := wait(bound, pause); waitErr != nil {
					return report, fmt.Errorf("retained audit retry wait: %w", waitErr)
				}
			}
		}
	}
	return report, fmt.Errorf("retained audit: %w", err)
}

func matrixRetryClient(ctx context.Context, call func(context.Context) error) error {
	bound, stop := context.WithTimeout(ctx, 30*time.Second)
	defer stop()
	for {
		attempt, done := context.WithTimeout(bound, 5*time.Second)
		err := call(attempt)
		done()
		if err == nil {
			return nil
		}
		if !errors.Is(err, client.ErrStartUnknown) && !errors.Is(err, client.ErrSignalUnknown) && !errors.Is(err, client.ErrEnqueueUnknown) && !matrixTransientTransport(err) {
			return err
		}
		select {
		case <-bound.Done():
			return fmt.Errorf("client retry: %w (last reply: %v)", bound.Err(), err)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// Fault selection is read-only. Retry named transport failures under the
// controller's existing deadline without changing the seeded candidate order.
// Missing resources and configuration errors remain immediate failures.
func matrixReadMetadata[T any](ctx context.Context, lookup func(context.Context) (T, error)) (T, error) {
	var zero T
	for i := 0; ; i++ {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		attempt, done := context.WithTimeout(ctx, 2*time.Second)
		value, err := lookup(attempt)
		done()
		if err == nil {
			return value, nil
		}
		if i == 2 || !matrixTransientTransport(err) {
			return zero, err
		}
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func matrixLeaderHandlers() map[string]worker.Handler {
	return map[string]worker.Handler{
		"matrixshort": func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
			value, err := wf.Run(c, "effect", 0, func(context.Context) (int, error) { return 42, nil })
			return json.RawMessage(strconv.Itoa(value)), err
		},
		"matrixtimer": func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
			for i := 0; i < 8; i++ {
				if err := wf.Sleep(c, fmt.Sprintf("timer-%d", i), 250*time.Millisecond); err != nil {
					return nil, err
				}
			}
			return json.RawMessage(`42`), nil
		},
		"matrixsignal": func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
			for i := 0; i < 8; i++ {
				value, err := wf.AwaitSignal(c, "go")
				if err != nil {
					return nil, err
				}
				if string(value) != strconv.Itoa(i) {
					return nil, fmt.Errorf("signal %d=%s", i, value)
				}
			}
			return json.RawMessage(`42`), nil
		},
		"matrixfanout": matrixCallChildren("matrixchild", 6),
		"matrixchild":  matrixCallChildren("matrixgrandchild", 2),
		"matrixgrandchild": func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
			value, err := wf.Run(c, "effect", 0, func(context.Context) (int, error) { return 3, nil })
			// Two grandchildren produce seven per child, six children produce 42.
			return json.RawMessage(strconv.Itoa(value)), err
		},
	}
}

func matrixCallChildren(typ string, count int) worker.Handler {
	return func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		promises := make([]wf.Promise, count)
		for i := range promises {
			p, err := wf.CallAsync(c, typ, json.RawMessage(`null`))
			if err != nil {
				return nil, err
			}
			promises[i] = p
		}
		sum := 0
		if typ == "matrixgrandchild" {
			sum = 1
		}
		for _, promise := range promises {
			value, err := wf.AwaitPromise(c, promise)
			if err != nil {
				return nil, err
			}
			var n int
			if err := json.Unmarshal(value, &n); err != nil {
				return nil, err
			}
			sum += n
		}
		return json.Marshal(sum)
	}
}

func killMatrixJournalLeader(ctx context.Context, js jetstream.JetStream, cluster *testcluster.ProcessCluster, scheduled time.Time) (matrixLeaderFault, error) {
	event := matrixLeaderFault{Scheduled: scheduled, Node: -1}
	attempt, stop := context.WithTimeout(ctx, 20*time.Second)
	defer stop()
	stream, err := matrixReadMetadata(attempt, func(ctx context.Context) (jetstream.Stream, error) {
		return js.Stream(ctx, "WF_JRN")
	})
	if err != nil {
		return event, err
	}
	info, err := matrixReadMetadata(attempt, func(ctx context.Context) (*jetstream.StreamInfo, error) {
		return stream.Info(ctx)
	})
	if err != nil || info.Cluster == nil {
		return event, fmt.Errorf("journal leader: info=%+v err=%v", info, err)
	}
	node, err := strconv.Atoi(strings.TrimPrefix(info.Cluster.Leader, "wf-process-"))
	if err != nil || node < 0 || node >= 3 {
		return event, fmt.Errorf("unknown leader %q", info.Cluster.Leader)
	}
	event.Node, event.Killed = node, time.Now()
	if err := cluster.KillNode(node); err != nil {
		return event, err
	}
	if err := cluster.RestartNode(node); err != nil {
		return event, err
	}
	for attempt.Err() == nil {
		lookup, done := context.WithTimeout(attempt, time.Second)
		info, err := stream.Info(lookup)
		done()
		ready := err == nil && info.Cluster != nil && info.Cluster.Leader != "" && len(info.Cluster.Replicas) == 2
		if ready {
			for _, peer := range info.Cluster.Replicas {
				ready = ready && peer.Current && !peer.Offline
			}
		}
		if ready {
			event.Healed = time.Now()
			return event, nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return event, fmt.Errorf("journal leader recovery: %w", attempt.Err())
}

func killMatrixConsumerLeader(ctx context.Context, js jetstream.JetStream, cluster *testcluster.ProcessCluster, scheduled time.Time, rng *rand.Rand) (matrixLeaderFault, error) {
	event := matrixLeaderFault{Scheduled: scheduled, Node: -1}
	bound, stop := context.WithTimeout(ctx, 20*time.Second)
	defer stop()
	stream, err := matrixReadMetadata(bound, func(ctx context.Context) (jetstream.Stream, error) {
		return js.Stream(ctx, "WF_RUN")
	})
	if err != nil {
		return event, err
	}
	var selected jetstream.Consumer
	var selectedInfo *jetstream.ConsumerInfo
	// Prefer a consumer with live deliveries; if all are idle, record the
	// selected durable explicitly so that an idle fault is visible in evidence.
	for _, partition := range rng.Perm(int(provision.Partitions)) {
		consumer, err := matrixReadMetadata(bound, func(ctx context.Context) (jetstream.Consumer, error) {
			return stream.Consumer(ctx, fmt.Sprintf("WF_P_%02d", partition))
		})
		if errors.Is(err, jetstream.ErrConsumerNotFound) {
			continue
		}
		if err != nil {
			return event, err
		}
		info := consumer.CachedInfo()
		if info == nil || info.Cluster == nil || info.Cluster.Leader == "" {
			return event, fmt.Errorf("consumer has no confirmed leader: %+v", info)
		}
		if selected == nil || info.NumPending > 0 || info.NumAckPending > 0 {
			selected, selectedInfo = consumer, info
		}
		if info.NumPending > 0 || info.NumAckPending > 0 {
			break
		}
	}
	if selected == nil {
		return event, fmt.Errorf("no durable consumer found")
	}
	node, err := strconv.Atoi(strings.TrimPrefix(selectedInfo.Cluster.Leader, "wf-process-"))
	if err != nil || node < 0 || node >= 3 {
		return event, fmt.Errorf("unknown consumer leader %q", selectedInfo.Cluster.Leader)
	}
	event.Node, event.Consumer = node, selectedInfo.Name
	event.Pending, event.AckPending = selectedInfo.NumPending, selectedInfo.NumAckPending
	event.Killed = time.Now()
	if err := cluster.KillNode(node); err != nil {
		return event, err
	}
	if err := cluster.RestartNode(node); err != nil {
		return event, err
	}
	for bound.Err() == nil {
		attempt, done := context.WithTimeout(bound, time.Second)
		info, err := selected.Info(attempt)
		done()
		ready := err == nil && info.Cluster != nil && info.Cluster.Leader != "" && len(info.Cluster.Replicas) == 2
		if ready {
			for _, peer := range info.Cluster.Replicas {
				ready = ready && peer.Current && !peer.Offline
			}
		}
		if ready {
			event.Healed = time.Now()
			return event, nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return event, fmt.Errorf("consumer leader recovery: %w", bound.Err())
}

func matrixInvocationLatencies(ctx context.Context, js jetstream.JetStream, typ, id string, enabled, completionDeadline time.Time) ([]matrixLatencySample, error) {
	return matrixInvocationLatenciesWithClock(ctx, js, typ, id, enabled, completionDeadline, 0)
}

func matrixInvocationLatenciesWithClock(ctx context.Context, js jetstream.JetStream, typ, id string, enabled, completionDeadline time.Time, offset time.Duration) ([]matrixLatencySample, error) {
	records, _, err := journal.New(js).Read(ctx, typ, id)
	if err != nil {
		return nil, err
	}
	stream, err := js.Stream(ctx, "WF_JRN")
	if err != nil {
		return nil, err
	}
	times := make([]time.Time, len(records))
	for i, record := range records {
		msg, err := stream.GetMsg(ctx, record.Sequence)
		if err != nil {
			return nil, err
		}
		times[i] = msg.Time
	}
	return matrixReduceInvocationLatencies(ctx, typ, id, enabled, completionDeadline, offset, records, times,
		func(childType, childID string) (time.Time, error) {
			msg, err := stream.GetLastMsgForSubject(ctx, identity.JournalSubject(childType, childID))
			if err != nil {
				return time.Time{}, err
			}
			return msg.Time, nil
		},
		func(sequence uint64) (time.Time, error) {
			signals, err := js.Stream(ctx, "WF_SIG")
			if err != nil {
				return time.Time{}, err
			}
			msg, err := signals.GetMsg(ctx, sequence)
			if err != nil {
				return time.Time{}, err
			}
			return msg.Time, nil
		},
		func() (time.Time, error) {
			msg, err := stream.GetLastMsgForSubject(ctx, identity.JournalSubject(typ, id))
			if err != nil {
				return time.Time{}, err
			}
			return msg.Time, nil
		})
}

// Kill every server before restarting any of them. No graceful flush occurs;
// each process returns on its original ports and persistent file store.
func killMatrixAllServers(ctx context.Context, js jetstream.JetStream, cluster *testcluster.ProcessCluster, scheduled time.Time) (matrixLeaderFault, error) {
	event := matrixLeaderFault{Scheduled: scheduled, Node: -1, Killed: time.Now()}
	bound, stop := context.WithTimeout(ctx, 25*time.Second)
	defer stop()
	for node := 0; node < 3; node++ {
		if err := cluster.KillNode(node); err != nil {
			return event, err
		}
		event.Nodes = append(event.Nodes, node)
	}
	for node := 0; node < 3; node++ {
		if err := cluster.RestartNode(node); err != nil {
			return event, err
		}
	}
	if err := waitMatrixWorkflowReplicas(bound, js); err != nil {
		return event, err
	}
	event.Healed = time.Now()
	return event, nil
}

func matrixRouteCounts(ctx context.Context, cluster *testcluster.ProcessCluster, want [3]int) ([3]int, error) {
	var got [3]int
	for ctx.Err() == nil {
		ready := true
		for node := range got {
			count, err := cluster.RouteCount(ctx, node)
			if err != nil {
				ready = false
				break
			}
			got[node] = count
		}
		if ready && got == want {
			return got, nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return got, fmt.Errorf("route counts=%v want=%v: %w", got, want, ctx.Err())
}

func partitionMatrixServer(ctx context.Context, js jetstream.JetStream, cluster *testcluster.ProcessCluster, scheduled time.Time) (event matrixLeaderFault, resultErr error) {
	event = matrixLeaderFault{Scheduled: scheduled, Node: 2, Killed: time.Now()}
	bound, stop := context.WithTimeout(ctx, 35*time.Second)
	defer stop()
	finishDiagnostics, diagnosticErr := startMatrixPartitionDiagnostics(bound, cluster, scheduled)
	if diagnosticErr != nil {
		return event, diagnosticErr
	}
	defer func() {
		if err := finishDiagnostics(); resultErr == nil {
			resultErr = err
		}
	}()

	if err := cluster.RouteMesh().PartitionNode(2); err != nil {
		return event, err
	}
	defer cluster.RouteMesh().Heal()
	cut, done := context.WithTimeout(bound, 4*time.Second)
	var err error
	event.Routes, err = matrixRouteCounts(cut, cluster, [3]int{4, 4, 0})
	done()
	if err != nil {
		return event, err
	}
	// The workload connection is restricted to the two majority nodes. Require
	// an acknowledged replicated write while the minority is still isolated.
	publish, done := context.WithDeadline(bound, event.Killed.Add(10*time.Second))
	for publish.Err() == nil {
		attempt, stopAttempt := context.WithTimeout(publish, time.Second)
		ack, pubErr := js.Publish(attempt, "matrix.route.probe", []byte(scheduled.Format(time.RFC3339Nano)), jetstream.WithMsgID(scheduled.Format(time.RFC3339Nano)))
		stopAttempt()
		if pubErr == nil {
			event.MajoritySequence = ack.Sequence
			break
		}
		var api *jetstream.APIError
		if !matrixTransientTransport(pubErr) && !(errors.As(pubErr, &api) && api.ErrorCode == 10158) {
			done()
			return event, pubErr
		}
		time.Sleep(50 * time.Millisecond)
	}
	done()
	if event.MajoritySequence == 0 {
		return event, fmt.Errorf("majority publish did not commit during isolation")
	}
	timer := time.NewTimer(time.Until(event.Killed.Add(10 * time.Second)))
	defer timer.Stop()
	select {
	case <-bound.Done():
		return event, bound.Err()
	case <-timer.C:
	}
	cluster.RouteMesh().Heal()
	if _, err := matrixRouteCounts(bound, cluster, [3]int{8, 8, 8}); err != nil {
		return event, err
	}
	// Confirm the formerly isolated peer has the majority's retained write,
	// then require full workflow-store replica catch-up before recording heal.
	minority, err := jetstream.New(cluster.Clients[2])
	if err != nil {
		return event, err
	}
	for bound.Err() == nil {
		attempt, done := context.WithTimeout(bound, time.Second)
		stream, readErr := minority.Stream(attempt, "MATRIX_ROUTE_PROBE")
		var msg *jetstream.RawStreamMsg
		if readErr == nil {
			msg, readErr = stream.GetMsg(attempt, event.MajoritySequence)
		}
		done()
		if readErr == nil {
			if string(msg.Data) != scheduled.Format(time.RFC3339Nano) {
				return event, fmt.Errorf("majority probe payload changed")
			}
			break
		}
		if !matrixTransientTransport(readErr) && !errors.Is(readErr, jetstream.ErrMsgNotFound) {
			return event, readErr
		}
		time.Sleep(50 * time.Millisecond)
	}
	if bound.Err() != nil {
		return event, bound.Err()
	}
	if err := waitMatrixWorkflowReplicas(bound, js); err != nil {
		return event, err
	}
	event.Healed = time.Now()
	return event, nil
}

func waitMatrixWorkflowReplicas(ctx context.Context, js jetstream.JetStream) error {
	var lastName string
	var lastCluster *jetstream.ClusterInfo
	var lastErr error
	for ctx.Err() == nil {
		ready := true
		for _, name := range []string{"WF_INV", "WF_JRN", "WF_RUN", "WF_SIG", "KV_WF_LEASE", "KV_WF_STATE", "WF_PURGE", "KV_WF_VIEW", "KV_WF_ASSIGN", "OBJ_WF_BLOB"} {
			attempt, done := context.WithTimeout(ctx, time.Second)
			stream, err := js.Stream(attempt, name)
			var info *jetstream.StreamInfo
			if err == nil {
				info, err = stream.Info(attempt)
			}
			done()
			lastName, lastErr = name, err
			lastCluster = nil
			if info != nil {
				lastCluster = info.Cluster
			}
			if err != nil || info == nil || info.Cluster == nil || info.Cluster.Leader == "" || len(info.Cluster.Replicas) != 2 {
				ready = false
				break
			}
			for _, peer := range info.Cluster.Replicas {
				ready = ready && peer.Current && !peer.Offline
			}
			if !ready {
				break
			}
		}
		if ready {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	clusterJSON, _ := json.Marshal(lastCluster)
	return fmt.Errorf("workflow replica recovery stream=%s cluster=%s last_error=%v: %w", lastName, clusterJSON, lastErr, ctx.Err())
}

func matrixTransientTransport(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrTimeout) || errors.Is(err, nats.ErrNoResponders) || errors.Is(err, jetstream.ErrNoStreamResponse) || natsutil.IsUnavailable(err)
}
