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

	"github.com/anishathalye/porcupine"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/history"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/reconcile"
	"js-wf/testcluster"
	"js-wf/wf"
	"js-wf/worker"
)

// Sustained R5 mixed-workload rows. This is not the full Tier 3 matrix
// or its 24-hour release soak; unsupported rows are not treated as covered.
func TestFiveContainerMixedServerClockAheadWithJournalKills(t *testing.T) {
	runFiveContainerMixedLeader(t, "server_clock_ahead")
}

func TestFiveContainerMixedServerClockBehindWithJournalKills(t *testing.T) {
	runFiveContainerMixedLeader(t, "server_clock_behind")
}

func TestFiveContainerMixedJournalLeaderEveryThirtySeconds(t *testing.T) {
	runFiveContainerMixedLeader(t, "journal")
}

func TestFiveContainerMixedConsumerLeaderEveryThirtySeconds(t *testing.T) {
	runFiveContainerMixedLeader(t, "consumer")
}

func TestFiveContainerMixedAllServersEveryThirtySeconds(t *testing.T) {
	runFiveContainerMixedLeader(t, "restart")
}

func TestFiveContainerMixedFanoutRestartEveryThirtySeconds(t *testing.T) {
	runFiveContainerMixedLeader(t, "fanout_restart")
}

func TestFiveContainerMixedRouteQuorumEveryThirtySeconds(t *testing.T) {
	runFiveContainerMixedLeader(t, "route_quorum")
}

func TestFiveContainerMixedRouteMajorityEveryThirtySeconds(t *testing.T) {
	runFiveContainerMixedLeader(t, "route_majority")
}

func TestFiveContainerMixedWorkerRepliesIsolatedFortyFiveSeconds(t *testing.T) {
	runFiveContainerMixedLeader(t, "worker_isolation")
}

func TestFiveContainerMixedWorkerPausedFortyFiveSeconds(t *testing.T) {
	runFiveContainerMixedLeader(t, "worker_pause")
}

func TestFiveContainerMixedWorkerKilledEveryFiveSeconds(t *testing.T) {
	runFiveContainerMixedLeader(t, "worker_kill")
}

func runFiveContainerMixedLeader(t *testing.T, row string) {
	t.Helper()
	if matrixServerClockOffset(row) == 0 && row != "journal" && row != "consumer" && row != "restart" && row != "fanout_restart" && row != "route_quorum" && row != "route_majority" && row != "worker_kill" && row != "worker_pause" && row != "worker_isolation" {
		t.Fatal("unsupported R5 fault row")
	}
	if os.Getenv("WF_TIER3_MATRIX") != "1" {
		t.Skip("set WF_TIER3_MATRIX=1 for sustained five-container mixed faults")
	}
	duration := 10 * time.Minute
	if value := os.Getenv("WF_TIER3_MATRIX_DURATION"); value != "" {
		var err error
		duration, err = time.ParseDuration(value)
		if err != nil {
			t.Fatal(err)
		}
	}
	if duration != 35*time.Second && duration != 10*time.Minute && duration != 24*time.Hour {
		t.Fatal("duration must be35s smoke,10m row,or24h single-row soak")
	}
	seed, err := testcluster.SeedFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(seed))
	faultRNG := rand.New(rand.NewSource(seed ^ 0x5c0115))
	root := os.Getenv("TIER3_MATRIX_ARTIFACT_ROOT")
	if root == "" {
		root = t.TempDir()
	}
	if err := os.MkdirAll(root, 0755); err != nil {
		t.Fatal(err)
	}
	if row == "route_quorum" || row == "route_majority" {
		// The default route ping can take30s before stale sockets disappear,
		// which exceeds the scheduled cut cadence. Make this fixture detection
		// interval explicit; production write-sync remains unchanged.
		t.Setenv("WF_TIER3_ROUTE_PING_INTERVAL", "1s")
	}
	if offset := matrixServerClockOffset(row); offset != 0 {
		t.Setenv("WF_TIER3_SERVER_SKEW", fmt.Sprintf("4:%s", offset))
	}
	cluster, err := testcluster.StartDockerCluster(filepath.Join(root, "cluster"), 5)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	defer func() {
		for node := 0; node < 5; node++ {
			logs, err := cluster.Logs(node)
			if err != nil {
				t.Errorf("server log%d: %v", node, err)
				continue
			}
			if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("server-%d.log", node)), []byte(logs), 0644); err != nil {
				t.Error(err)
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), duration+6*time.Minute)
	defer cancel()
	urls := make([]string, 5)
	for node := range urls {
		urls[node] = cluster.ClientURL(node)
	}
	nc, err := nats.Connect(strings.Join(urls, ","), nats.MaxReconnects(-1), nats.ReconnectWait(100*time.Millisecond), nats.IgnoreDiscoveredServers(), nats.Timeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ready, stopReady := context.WithTimeout(ctx, 45*time.Second)
	for ready.Err() == nil {
		attempt, stop := context.WithTimeout(ready, 3*time.Second)
		err = provision.Ensure(attempt, js, 5)
		stop()
		if err == nil {
			break
		}
		var api *jetstream.APIError
		// A newly formed five-peer metadata group can answer before all peers
		// become eligible for R5 placement. Retry this named startup error only
		// within the existing whole-provisioning deadline.
		placementPending := errors.As(err, &api) && api.ErrorCode == 10005
		if !matrixTransientTransport(err) && !placementPending {
			break
		}
		t.Logf("tier3 provisioning retry: %v", err)
		time.Sleep(100 * time.Millisecond)
	}
	stopReady()
	if err != nil {
		t.Fatal(err)
	}
	if row == "route_quorum" || row == "route_majority" {
		if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "TIER3_ROUTE_PROBE", Subjects: []string{"tier3.route.probe"}, Replicas: 5, Storage: jetstream.FileStorage}); err != nil {
			t.Fatal(err)
		}
	}
	var recorder history.Recorder
	c := client.NewObserved(js, &recorder)
	var evidenceMu sync.Mutex
	var controllerOperations []worker.OperationEvent
	var controllerTimers = make(map[string]matrixControllerTimerCall)
	var controllerProof matrixControllerAudit
	var controllerReceipts []matrixControllerJournalReceipt
	var controllerAuditOperations []worker.OperationEvent
	var controllerAuditTimers []matrixControllerTimerCall
	var controllerClientCalls []client.Operation
	var receiptObserver *matrixControllerReceiptObserver
	var observeOperations func(worker.OperationEvent)
	if matrixServerClockOffset(row) != 0 {
		observeOperations = func(event worker.OperationEvent) {
			if event.Operation != "journal_append" && event.Operation != "timer_clock" {
				return
			}
			evidenceMu.Lock()
			controllerOperations = append(controllerOperations, event)
			evidenceMu.Unlock()
		}
	}

	var dispatch []worker.DispatchEvent
	var fencing []worker.FencingEvent
	var repairs []reconcile.RepairEvent
	repairObserver := func(event reconcile.RepairEvent) {
		evidenceMu.Lock()
		repairs = append(repairs, event)
		evidenceMu.Unlock()
	}
	var scans []struct {
		At     time.Time
		Cursor uint64
		Result reconcile.ScanResult
		Error  string
	}
	var clockObservations []matrixServerClockObservation
	var clockRoles []matrixClockRoleObservation
	recordRoles := func(records []matrixClockRoleObservation) {
		evidenceMu.Lock()
		clockRoles = append(clockRoles, records...)
		evidenceMu.Unlock()
	}
	var faults []matrixLeaderFault
	var samples []matrixLatencySample
	var fleet sync.WaitGroup
	workCtx, stopWork := context.WithCancel(ctx)
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
	var processes, processSessions []*matrixProcessWorker
	var workerProxies []*testcluster.ClientProxy
	var proxySpecs []tier3ProxySpec
	defer func() {
		for _, proxy := range workerProxies {
			proxy.Close()
		}
	}()
	var workers []*worker.Worker
	defer func() {
		stopWork()
		fleet.Wait()
		for _, w := range workers {
			_ = w.Close()
		}
	}()
	defer func() {
		for _, p := range processes {
			stopMatrixProcessWorker(p)
		}
	}()
	var fanoutBarrier matrixFanoutBarrier
	handlers := matrixLeaderHandlers()
	if row == "fanout_restart" {
		handlers = fanoutBarrier.handlers()
	}
	if matrixServerClockOffset(row) != 0 {
		handlers["matrixtimer"] = func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
			var request struct {
				ID string `json:"fixture_id"`
			}
			if err := json.Unmarshal(input, &request); err != nil || request.ID == "" {
				return nil, fmt.Errorf("missing controller timer fixture identity")
			}
			for i := 0; i < 8; i++ {
				name := fmt.Sprintf("timer-%d", i)
				key := request.ID + "/" + name
				before := time.Now().UTC()
				evidenceMu.Lock()
				timer := controllerTimers[key]
				if timer.FirstCall.IsZero() {
					timer = matrixControllerTimerCall{ID: request.ID, Name: name, FirstCall: before, Duration: 250 * time.Millisecond}
					controllerTimers[key] = timer
				}
				evidenceMu.Unlock()
				err := wf.Sleep(c, name, 250*time.Millisecond)
				if err != nil {
					return nil, err
				}
				after := time.Now().UTC()
				evidenceMu.Lock()
				timer = controllerTimers[key]
				if timer.FirstReturn.IsZero() || after.Before(timer.FirstReturn) {
					timer.FirstReturn = after
					controllerTimers[key] = timer
				}
				evidenceMu.Unlock()
			}
			return json.RawMessage(`42`), nil
		}
	}
	if row != "worker_kill" && row != "worker_pause" && row != "worker_isolation" {
		for node := 0; node < 5; node++ {
			w, err := worker.New(ctx, js, fmt.Sprintf("tier3-mixed-%d", node), handlers, worker.WithPartitionConcurrency(4), worker.WithOperationObserver(observeOperations), worker.WithDispatchObserver(func(event worker.DispatchEvent) {
				evidenceMu.Lock()
				dispatch = append(dispatch, event)
				evidenceMu.Unlock()
			}), worker.WithFencingObserver(func(event worker.FencingEvent) {
				evidenceMu.Lock()
				fencing = append(fencing, event)
				evidenceMu.Unlock()
			}))
			if err != nil {
				t.Fatal(err)
			}
			workers = append(workers, w)
		}
		for part := uint32(0); part < provision.Partitions; part++ {
			w := workers[part%5]
			launch(func() error { return w.RunPartition(workCtx, part) })
		}
	} else {
		for slot := 0; slot < 5; slot++ {
			workerURLs := urls
			if row == "worker_isolation" {
				proxy, err := testcluster.NewClientProxy(urls[slot])
				if err != nil {
					t.Fatal(err)
				}
				workerProxies = append(workerProxies, proxy)
				if err := proxy.EnableTrafficTrace(4 << 20); err != nil {
					t.Fatal(err)
				}
				workerURLs = []string{proxy.URL()}
			}
			process, err := startMatrixProcessWorker(ctx, root, workerURLs, slot, 0)
			if err != nil {
				t.Fatal(err)
			}
			processes = append(processes, process)
			processSessions = append(processSessions, process)
			if row == "worker_isolation" {
				proxySpecs = append(proxySpecs, tier3ProxySpec{Worker: process.id, PID: process.cmd.Process.Pid, ProxyURL: workerURLs[0], ServerURL: urls[slot], Slot: slot})
			}
		}
	}
	launch(func() error {
		return reconcile.RunRepairLoopObserved(workCtx, js, "tier3-mixed-start", "start", time.Second, 32, repairObserver)
	})
	launch(func() error {
		return reconcile.RunRepairLoopObserved(workCtx, js, "tier3-mixed-signal", "signal", time.Second, 32, repairObserver)
	})
	launch(func() error {
		return reconcile.RunSuspendedLoopWithObservers(workCtx, js, "tier3-mixed-suspended", time.Second, 8, func(cursor uint64, result reconcile.ScanResult, err error) {
			evidenceMu.Lock()
			defer evidenceMu.Unlock()
			message := ""
			if err != nil {
				message = err.Error()
			}
			scans = append(scans, struct {
				At     time.Time
				Cursor uint64
				Result reconcile.ScanResult
				Error  string
			}{time.Now().UTC(), cursor, result, message})
		}, repairObserver)
	})
	defer func() {
		stopWork()
		fleet.Wait()
		write := func(name string, value any) {
			data, err := json.MarshalIndent(value, "", "  ")
			if err == nil {
				err = os.WriteFile(filepath.Join(root, name), data, 0644)
			}
			if err != nil {
				t.Error(err)
			}
		}
		if row == "worker_kill" || row == "worker_pause" || row == "worker_isolation" {
			for _, p := range processes {
				stopMatrixProcessWorker(p)
			}
			var sessions []tier3ProcessEvidence
			for _, p := range processSessions {
				proof, steps, fences, err := readTier3ProcessEvidence(p)
				if err != nil {
					t.Error(err)
					continue
				}
				sessions = append(sessions, proof)
				dispatch = append(dispatch, steps...)
				fencing = append(fencing, fences...)
			}
			write("process-evidence.json", sessions)
			if row == "worker_isolation" {
				write("worker-proxy-specs.json", proxySpecs)
				for slot, proxy := range workerProxies {
					write(fmt.Sprintf("proxy-%d-traffic.json", slot), proxy.TrafficTrace())
				}
			}
		}
		evidenceMu.Lock()
		write("dispatch.json", dispatch)
		write("fencing.json", fencing)
		write("repairs.json", repairs)
		write("suspended-scans.json", scans)
		evidenceMu.Unlock()
		evidenceMu.Lock()
		write("server-clock-observations.json", clockObservations)
		write("server-clock-roles.json", clockRoles)
		if matrixServerClockOffset(row) != 0 {
			operations := controllerOperations
			if controllerAuditOperations != nil {
				operations = controllerAuditOperations
			}
			write("controller-operations.json", operations)
			var timers []matrixControllerTimerCall
			for _, timer := range controllerTimers {
				timers = append(timers, timer)
			}
			if controllerAuditTimers != nil {
				timers = controllerAuditTimers
			}
			write("controller-timers.json", timers)
			write("controller-client-calls.json", controllerClientCalls)
			if receiptObserver != nil && controllerReceipts == nil {
				controllerReceipts, _ = receiptObserver.snapshot()
			}
			write("controller-receipts.json", controllerReceipts)
			write("controller-latency-audit.json", controllerProof)
		}

		evidenceMu.Unlock()
		write("faults.json", faults)
		write("latencies.json", samples)
		var metrics []worker.Metrics
		for _, w := range workers {
			metrics = append(metrics, w.Metrics())
		}
		write("worker-metrics.json", metrics)
		var fenceCount uint64
		for _, m := range metrics {
			fenceCount += m.FencingEvents
		}
		if row != "worker_kill" && row != "worker_pause" && row != "worker_isolation" && fenceCount != uint64(len(fencing)) {
			t.Errorf("fencing evidence=%d counter=%d", len(fencing), fenceCount)
		}
		f, err := os.Create(filepath.Join(root, "history.jsonl"))
		if err != nil {
			t.Error(err)
		} else {
			if err := recorder.WriteJSONL(f); err != nil {
				t.Error(err)
			}
			if err := f.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	ready, stopReady = context.WithTimeout(ctx, 60*time.Second)
	err = waitFiveReplicaReadiness(ready, js, 0)
	stopReady()
	if err != nil {
		t.Fatal(err)
	}
	if matrixServerClockOffset(row) != 0 {
		receiptObserver, err = startMatrixControllerReceiptObserver(ctx, js)
		if err != nil {
			t.Fatal(err)
		}
		defer receiptObserver.consume.Stop()
	}
	observeClocks := func(stage string, fault int) error {
		if matrixServerClockOffset(row) == 0 {
			return nil
		}
		observed, err := observeMatrixServerClocks(ctx, cluster, row, stage, fault)
		evidenceMu.Lock()
		clockObservations = append(clockObservations, observed...)
		evidenceMu.Unlock()
		return err
	}
	if matrixServerClockOffset(row) != 0 {
		roles, err := preferMatrixClockLeaders(ctx, nc, js, cluster, "initial", 0)
		recordRoles(roles)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := observeClocks("initial", 0); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	end := started.Add(duration)
	faultDone := make(chan error, 1)
	faultInterval := 30 * time.Second
	firstFault := faultInterval
	if row == "worker_kill" {
		faultInterval = 5 * time.Second
		firstFault = faultInterval
	}
	if row == "worker_pause" || row == "worker_isolation" {
		faultInterval = 60 * time.Second
		firstFault = 5 * time.Second
	}
	go func() {
		for scheduled := started.Add(firstFault); scheduled.Before(end); scheduled = scheduled.Add(faultInterval) {
			delay := time.Until(scheduled)
			if delay < 0 {
				delay = 0
			}
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				faultDone <- ctx.Err()
				return
			case <-timer.C:
			}
			var event matrixLeaderFault
			var err error
			prefix := filepath.Join(root, fmt.Sprintf("fault-%d", len(faults)+1))
			if matrixServerClockOffset(row) != 0 {
				roles, roleErr := preferMatrixClockLeaders(ctx, nc, js, cluster, "before", len(faults)+1)
				recordRoles(roles)
				if roleErr != nil {
					cancel()
					faultDone <- roleErr
					return
				}
			}
			if err := observeClocks("before", len(faults)+1); err != nil {
				cancel()
				faultDone <- err
				return
			}
			if matrixServerClockOffset(row) != 0 {
				event, err = killFiveContainerMixedClockLeader(ctx, js, cluster, scheduled, prefix, len(faults)+1, recordRoles)
			} else if row == "worker_isolation" {
				event, err = isolateMatrixWorkerReplies(ctx, processes, workerProxies, faultRNG.Intn(len(processes)), scheduled, func(c context.Context, fleet []*matrixProcessWorker, first int) (int, matrixIsolationTarget, func() error, error) {
					return armMatrixUnfinishedIsolationTarget(c, js, fleet, first)
				})
			} else if row == "worker_pause" {
				event, err = pauseMatrixProcessWorker(ctx, js, processes, faultRNG.Intn(len(processes)), scheduled)
			} else if row == "worker_kill" {
				// The acquisition handoff proves the selected delivery still holds
				// its lease when SIGKILL occurs. Snapshot the pointers so release
				// disarms the old generation as well as surviving workers.
				snapshot := append([]*matrixProcessWorker(nil), processes...)
				var slot int
				var target matrixIsolationTarget
				var release func() error
				first := faultRNG.Intn(len(snapshot))
				selectionCtx, stopSelection := context.WithTimeout(ctx, 500*time.Millisecond)
				slot, target, release, err = armMatrixIsolationTarget(selectionCtx, snapshot, first)
				stopSelection()
				selection := "held_delivery"
				if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
					// A serial cohort can be waiting for the killed owner's lease
					// to expire, with no new acquisition. Random kill cadence must
					// still continue; retain this as a distinct selection outcome.
					if releaseErr := release(); releaseErr == nil {
						err = nil
						slot = first
						selection = "no_new_acquisition_within_500ms"
					}
				}
				if err == nil {
					event, err = killMatrixProcessWorker(ctx, root, urls, processes, slot, scheduled)
					event.WorkerSelection = selection
					if selection == "held_delivery" {
						event.WorkerTarget = &target
					}
					if processes[slot] != nil {
						processSessions = append(processSessions, processes[slot])
					}
				}
				if release != nil {
					err = errors.Join(err, release())
				}
			} else if row == "consumer" {
				event, err = killFiveContainerMixedConsumerLeader(ctx, js, cluster, scheduled, prefix, faultRNG)
			} else if row == "route_quorum" || row == "route_majority" {
				event, err = partitionFiveContainerMixedRoute(ctx, js, cluster, scheduled, prefix, faultRNG, nc, row == "route_quorum")
			} else if row == "fanout_restart" {
				event, err = fanoutBarrier.restartWith(ctx, js, scheduled, prefix, func() (matrixLeaderFault, error) {
					return killFiveContainerMixedAllServers(ctx, js, cluster, scheduled, prefix)
				})
			} else if row == "restart" {
				event, err = killFiveContainerMixedAllServers(ctx, js, cluster, scheduled, prefix)
			} else {
				event, err = killFiveContainerMixedJournalLeader(ctx, js, cluster, scheduled, prefix)
			}
			if err == nil {
				err = observeClocks("after", len(faults)+1)
			}
			evidenceMu.Lock()
			faults = append(faults, event)
			evidenceMu.Unlock()
			if err != nil {
				t.Logf("tier3 %s fault failed: %v", row, err)
				cancel()
				faultDone <- err
				return
			}
			t.Logf("tier3 %s fault node=%d scheduled=%s killed=%s healed=%s", row, event.Node, event.Scheduled.Sub(started), event.Killed.Sub(started), event.Healed.Sub(started))
			if row == "consumer" {
				t.Logf("TIER3_CONSUMER_FAULT consumer=%s node=%d pending=%d ack_pending=%d", event.Consumer, event.Node, event.Pending, event.AckPending)
			}
		}
		faultDone <- nil
	}()
	faultJoined := false
	defer func() {
		if !faultJoined {
			cancel()
			<-faultDone
		}
	}()
	batches := 0
	for time.Now().Before(end) && ctx.Err() == nil {
		kinds := []string{"matrixshort", "matrixshort", "matrixshort", "matrixshort", "matrixtimer", "matrixtimer", "matrixtimer", "matrixsignal", "matrixsignal", "matrixfanout"}
		rng.Shuffle(len(kinds), func(i, j int) { kinds[i], kinds[j] = kinds[j], kinds[i] })
		batchCtx, stopBatch := context.WithTimeout(ctx, 5*time.Minute)
		var batch sync.WaitGroup
		batchErrors := make(chan error, len(kinds))
		for index, typ := range kinds {
			id := fmt.Sprintf("tier3-%d-batch-%d-%d", seed, batches, index)
			if row == "fanout_restart" && typ == "matrixfanout" {
				fanoutBarrier.noteParent(id)
			}
			batch.Add(1)
			go func() {
				defer batch.Done()
				input := []byte(`null`)
				if matrixServerClockOffset(row) != 0 && typ == "matrixtimer" {
					input, _ = json.Marshal(map[string]string{"fixture_id": id})
				}
				if err := matrixRetryClient(batchCtx, func(attempt context.Context) error {
					_, err := c.Start(attempt, typ, id, input)
					if errors.Is(err, client.ErrAlreadyStarted) {
						return nil
					}
					return err
				}); err != nil {
					batchErrors <- err
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
					batchErrors <- fmt.Errorf("result%s/%s=%s err=%v", typ, id, value, err)
				}
			}()
		}
		batch.Wait()
		stopBatch()
		close(batchErrors)
		for err := range batchErrors {
			t.Fatal(err)
		}
		batches++
		if batches%10 == 0 {
			report, err := matrixRetainedAudit(ctx, js)
			if err != nil || report.Invocations != batches*28 || report.Journals != batches*28 || report.Terminal != batches*28 {
				t.Fatalf("checkpoint batch=%d report=%+v err=%v", batches, report, err)
			}
			t.Logf("tier3 checkpoint batch=%d report=%+v", batches, report)
		}
		t.Logf("tier3 mixed batch=%d elapsed=%s", batches, time.Since(started))
	}
	if err := <-faultDone; err != nil {
		faultJoined = true
		t.Fatal(err)
	}
	faultJoined = true
	if row == "worker_isolation" {
		for index, fault := range faults {
			final, _, err := journal.New(js).Read(ctx, fault.IsolationTarget.Delivery.Type, fault.IsolationTarget.Delivery.ID)
			if err != nil {
				t.Fatal(err)
			}
			prefix := fault.IsolationTarget.JournalPrefix
			if len(final) < len(prefix) {
				t.Fatal("isolated invocation lost original prefix")
			}
			a, _ := json.Marshal(prefix)
			b, _ := json.Marshal(final[:len(prefix)])
			if len(prefix) > 0 && string(a) != string(b) {
				t.Fatal("isolated invocation prefix changed")
			}
			if len(final) == 0 || final[len(final)-1].Kind != journal.Completed {
				t.Fatal("isolated unfinished invocation did not complete")
			}
			data, err := json.MarshalIndent(final, "", "  ")
			if err == nil {
				err = os.WriteFile(filepath.Join(root, fmt.Sprintf("fault-%d-isolation-final-journal.json", index+1)), data, 0644)
			}
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if row == "fanout_restart" {
		if err := verifyMatrixRestartFanoutsWithArtifacts(ctx, js, faults, root); err != nil {
			t.Fatal(err)
		}
	}
	if ctx.Err() != nil {
		select {
		case err := <-fleetErrors:
			t.Fatal(err)
		default:
			t.Fatal(ctx.Err())
		}
	}
	expectedFaults := int((duration-firstFault-time.Nanosecond)/faultInterval) + 1
	if len(faults) != expectedFaults {
		t.Fatalf("faults=%d want=%d", len(faults), expectedFaults)
	}
	if len(faults) == 0 {
		t.Fatal("no journal fault executed")
	}
	if row == "worker_kill" {
		active := 0
		for _, fault := range faults {
			if fault.WorkerTarget != nil {
				active++
			}
		}
		if active == 0 {
			t.Fatal("worker row never killed a confirmed held delivery")
		}
	}
	completionDeadline := faults[len(faults)-1].Healed.Add(5 * time.Minute)
	inv, err := matrixReadMetadata(ctx, func(attempt context.Context) (jetstream.Stream, error) { return js.Stream(attempt, "WF_INV") })
	if err != nil {
		t.Fatal(err)
	}
	info, err := matrixReadMetadata(ctx, func(attempt context.Context) (*jetstream.StreamInfo, error) { return inv.Info(attempt) })
	if err != nil {
		t.Fatal(err)
	}
	if matrixServerClockOffset(row) != 0 {
		controllerReceipts, err = receiptObserver.snapshot()
		if err != nil {
			t.Fatal(err)
		}
		evidenceMu.Lock()
		operations := append([]worker.OperationEvent(nil), controllerOperations...)
		var timers []matrixControllerTimerCall
		for _, timer := range controllerTimers {
			timers = append(timers, timer)
		}
		evidenceMu.Unlock()
		controllerAuditOperations, controllerAuditTimers = operations, timers
		controllerClientCalls = recorder.Snapshot()
		controllerProof, err = auditMatrixControllerLatencies(ctx, js, operations, controllerReceipts, controllerClientCalls, timers, completionDeadline)
		if err != nil {
			t.Fatal(err)
		}
		samples = controllerProof.Samples
	} else {
		for sequence := info.State.FirstSeq; sequence <= info.State.LastSeq; sequence++ {
			attempt, stop := context.WithTimeout(ctx, 20*time.Second)
			msg, err := inv.GetMsg(attempt, sequence)
			if err == nil {
				parts := strings.Split(msg.Subject, ".")
				var next []matrixLatencySample
				next, err = matrixInvocationLatencies(attempt, js, parts[2], parts[3], msg.Time, completionDeadline)
				samples = append(samples, next...)
			}
			stop()
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	report, err := matrixRetainedAudit(ctx, js)
	want := batches * 28
	if err != nil || report.Invocations != want || report.Journals != want || report.Terminal != want {
		t.Fatalf("retained=%+v want=%d err=%v", report, want, err)
	}
	for _, check := range []func([]client.Operation, time.Duration) (porcupine.CheckResult, error){history.CheckStarts, history.CheckSignals, history.CheckResults} {
		result, err := check(recorder.Snapshot(), 30*time.Second)
		if err != nil || result != porcupine.Ok {
			t.Fatalf("history=%v err=%v", result, err)
		}
	}
	var recoverySamples []matrixLatencySample
	if row == "route_quorum" {
		for _, sample := range samples {
			adjusted := sample
			adjusted.Delay = tier3RouteRecoveryDelay(sample, faults)
			recoverySamples = append(recoverySamples, adjusted)
		}
		data, err := json.MarshalIndent(recoverySamples, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(root, "route-recovery-latencies.json"), data, 0644); err != nil {
			t.Fatal(err)
		}
	}
	for _, typ := range []string{"matrixshort", "matrixtimer", "matrixsignal", "matrixfanout", "matrixchild", "matrixgrandchild"} {
		var terminal, progress []time.Duration
		for _, sample := range samples {
			if sample.Type != typ {
				continue
			}
			if sample.Event == "terminal" {
				terminal = append(terminal, sample.Delay)
			} else {
				progress = append(progress, sample.Delay)
			}
		}
		expectedCounts := map[string]int{"matrixshort": 4, "matrixtimer": 3, "matrixsignal": 2, "matrixfanout": 1, "matrixchild": 6, "matrixgrandchild": 12}
		if len(terminal) != batches*expectedCounts[typ] || len(progress) == 0 {
			t.Fatalf("missing%s latency samples", typ)
		}
		percentile := func(values []time.Duration) time.Duration {
			sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
			return values[(99*len(values)+99)/100-1]
		}
		tp, pp := percentile(terminal), percentile(progress)
		if row == "route_quorum" {
			t.Logf("TIER3_ROUTE_RAW_CELL type=%s terminal_p99=%s progress_p99=%s", typ, tp, pp)
			terminal, progress = nil, nil
			for _, sample := range recoverySamples {
				if sample.Type != typ {
					continue
				}
				if sample.Event == "terminal" {
					terminal = append(terminal, sample.Delay)
				} else {
					progress = append(progress, sample.Delay)
				}
			}
			tp, pp = percentile(terminal), percentile(progress)
		}
		t.Logf("TIER3_MIXED_CELL type=%s invocations=%d terminal_p99=%s progress_p99=%s", typ, len(terminal), tp, pp)
		if tp >= 30*time.Second || pp >= 30*time.Second {
			t.Errorf("%s terminal/progress p99=%s/%s want<30s", typ, tp, pp)
		}
	}
	run, err := js.Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	// Preserve server-side placement/queues even if the client metadata path
	// stops answering. These independent HTTP reads do not certify drainage.
	captureDrain := func(phase string) {
		for node := 0; node < 5; node++ {
			for _, kind := range []string{"jetstream", "connections", "routes"} {
				read, stop := context.WithTimeout(context.Background(), 2*time.Second)
				data, err := cluster.Diagnostic(read, node, kind)
				stop()
				name := filepath.Join(root, fmt.Sprintf("drain-%s-node%d-%s.json", phase, node, kind))
				if err != nil {
					data, _ = json.Marshal(map[string]string{"error": err.Error()})
				}
				if err := os.WriteFile(name, data, 0644); err != nil {
					t.Error(err)
				}
			}
		}
	}
	drain, stopDrain := context.WithTimeout(ctx, 30*time.Second)
	defer stopDrain()
	captureDrain("before")
	defer captureDrain("after")
	type drainAttempt struct {
		At    time.Time
		Part  int
		Info  *jetstream.StreamInfo
		Error string
	}
	var drainAttempts []drainAttempt
	var lastDrainState *jetstream.StreamInfo
	defer func() {
		data, err := json.MarshalIndent(drainAttempts, "", "  ")
		if err == nil {
			err = os.WriteFile(filepath.Join(root, "drain-attempts.json"), data, 0644)
		}
		if err != nil {
			t.Error(err)
		}
	}()

	for {
		attempt, stop := context.WithTimeout(drain, 2*time.Second)
		state, err := run.Info(attempt)
		stop()
		if err == nil {
			lastDrainState = state
		}
		message := ""
		if err != nil {
			message = err.Error()
		}
		drainAttempts = append(drainAttempts, drainAttempt{time.Now().UTC(), -1, state, message})
		drained := err == nil && state.State.Msgs == 0
		if drained {
			for part := uint32(0); part < provision.Partitions; part++ {
				attempt, stop := context.WithTimeout(drain, 2*time.Second)
				consumer, err := run.Consumer(attempt, fmt.Sprintf("WF_P_%02d", part))
				var info *jetstream.ConsumerInfo
				if err == nil {
					info, err = consumer.Info(attempt)
				}
				stop()
				if err != nil || info.NumPending != 0 || info.NumAckPending != 0 {
					message := fmt.Sprintf("consumer=%+v error=%v", info, err)
					drainAttempts = append(drainAttempts, drainAttempt{time.Now().UTC(), int(part), state, message})
					drained = false
					break
				}
			}
		}
		if drained {
			break
		}
		if drain.Err() != nil {
			if snapshotErr := captureMatrixRunBacklog(filepath.Join(root, "drain-failure-backlog.json"), run, lastDrainState); snapshotErr != nil {
				t.Errorf("capture failed drain backlog: %v", snapshotErr)
			}
			t.Fatalf("run queue not drained: %+v %v", state, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if row == "consumer" {
		active := 0
		for _, fault := range faults {
			if fault.Pending > 0 || fault.AckPending > 0 {
				active++
			}
		}
		if active == 0 {
			t.Fatal("consumer row never selected a consumer with active deliveries")
		}
	}
	t.Logf("TIER3_MIXED_RESULT row=%s seed=%d duration=%s five_replicas=true batches=%d invocations=%d entries=%d faults=%d full_matrix_release=false", row, seed, duration, batches, want, report.Entries, len(faults))
}

func killFiveContainerMixedJournalLeader(ctx context.Context, js jetstream.JetStream, cluster *testcluster.DockerCluster, scheduled time.Time, artifactPrefix string) (matrixLeaderFault, error) {
	event := matrixLeaderFault{Scheduled: scheduled, Node: -1}
	bound, stop := context.WithTimeout(ctx, 60*time.Second)
	defer stop()
	stream, err := matrixReadMetadata(bound, func(attempt context.Context) (jetstream.Stream, error) { return js.Stream(attempt, "WF_JRN") })
	if err != nil {
		return event, err
	}
	info, err := matrixReadMetadata(bound, func(attempt context.Context) (*jetstream.StreamInfo, error) { return stream.Info(attempt) })
	if err != nil || info.Cluster == nil {
		return event, fmt.Errorf("journal leader info=%+v err=%v", info, err)
	}
	for node := 0; node < 5; node++ {
		if info.Cluster.Leader == cluster.NodeName(node) {
			event.Node = node
			break
		}
	}
	if event.Node < 0 {
		return event, fmt.Errorf("unknown journal leader%q", info.Cluster.Leader)
	}
	clientURL, monitorURL := cluster.ClientURL(event.Node), cluster.MonitorURL(event.Node)
	logs, err := cluster.Logs(event.Node)
	if err != nil {
		return event, err
	}
	if err := os.WriteFile(artifactPrefix+"-server-before.log", []byte(logs), 0644); err != nil {
		return event, err
	}
	data, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		return event, err
	}
	if err := os.WriteFile(artifactPrefix+"-journal-before.json", data, 0644); err != nil {
		return event, err
	}
	event.Killed = time.Now()
	if err := cluster.KillNode(event.Node); err != nil {
		return event, err
	}
	removedAt := time.Now().UTC()
	if err := cluster.RestartNode(event.Node); err != nil {
		return event, err
	}
	restartedAt := time.Now().UTC()
	operations := []struct {
		Node   int       `json:"node"`
		Action string    `json:"action"`
		At     time.Time `json:"at"`
	}{{event.Node, "sigkill_removed", removedAt}, {event.Node, "restarted", restartedAt}}
	data, err = json.MarshalIndent(operations, "", "  ")
	if err == nil {
		err = os.WriteFile(artifactPrefix+"-journal-operations.json", data, 0644)
	}
	if err != nil {
		return event, err
	}
	if cluster.ClientURL(event.Node) != clientURL || cluster.MonitorURL(event.Node) != monitorURL {
		return event, fmt.Errorf("restart changed persistent client/monitor address")
	}
	if err := waitFiveReplicaReadiness(bound, js, 0); err != nil {
		return event, err
	}
	event.Healed = time.Now()
	return event, nil
}

// Prefer live delivery metadata; idle selections remain visible and do not
// establish the entire row's active-consumer coverage on their own.
func killFiveContainerMixedConsumerLeader(ctx context.Context, js jetstream.JetStream, cluster *testcluster.DockerCluster, scheduled time.Time, prefix string, rng *rand.Rand) (matrixLeaderFault, error) {
	event := matrixLeaderFault{Scheduled: scheduled, Node: -1}
	bound, stop := context.WithTimeout(ctx, 60*time.Second)
	defer stop()
	stream, err := matrixReadMetadata(bound, func(attempt context.Context) (jetstream.Stream, error) { return js.Stream(attempt, "WF_RUN") })
	if err != nil {
		return event, err
	}
	var selected *jetstream.ConsumerInfo
	var selectedPart uint32
	for _, part := range rng.Perm(int(provision.Partitions)) {
		consumer, err := matrixReadMetadata(bound, func(attempt context.Context) (jetstream.Consumer, error) {
			return stream.Consumer(attempt, fmt.Sprintf("WF_P_%02d", part))
		})
		if errors.Is(err, jetstream.ErrConsumerNotFound) {
			continue
		}
		if err != nil {
			return event, err
		}
		info := consumer.CachedInfo()
		if info == nil || info.Cluster == nil || info.Cluster.Leader == "" || len(info.Cluster.Replicas) != 4 {
			return event, fmt.Errorf("no confirmed R5 consumer leader: %+v", info)
		}
		if selected == nil || info.NumPending > 0 || info.NumAckPending > 0 {
			selected, selectedPart = info, uint32(part)
		}
		if info.NumPending > 0 || info.NumAckPending > 0 {
			break
		}
	}
	if selected == nil {
		return event, fmt.Errorf("no durable consumer found")
	}
	for node := 0; node < 5; node++ {
		if selected.Cluster.Leader == cluster.NodeName(node) {
			event.Node = node
			break
		}
	}
	if event.Node < 0 {
		return event, fmt.Errorf("unknown consumer leader %q", selected.Cluster.Leader)
	}
	if selected.Cluster.Leader != cluster.NodeName(event.Node) {
		return event, fmt.Errorf("selected fault node is not the observed consumer leader")
	}
	event.Consumer, event.Pending, event.AckPending = selected.Name, selected.NumPending, selected.NumAckPending
	clientURL, monitorURL := cluster.ClientURL(event.Node), cluster.MonitorURL(event.Node)
	logs, err := cluster.Logs(event.Node)
	if err != nil {
		return event, err
	}
	if err = os.WriteFile(prefix+"-server-before.log", []byte(logs), 0644); err != nil {
		return event, err
	}
	data, err := json.MarshalIndent(struct {
		ObservedAt time.Time
		Node       int
		Partition  uint32
		Info       *jetstream.ConsumerInfo
	}{time.Now().UTC(), event.Node, selectedPart, selected}, "", "  ")
	if err != nil {
		return event, err
	}
	if err = os.WriteFile(prefix+"-consumer-before.json", data, 0644); err != nil {
		return event, err
	}
	event.Killed = time.Now()
	if err = cluster.KillNode(event.Node); err != nil {
		return event, err
	}
	if err = cluster.RestartNode(event.Node); err != nil {
		return event, err
	}
	if cluster.ClientURL(event.Node) != clientURL || cluster.MonitorURL(event.Node) != monitorURL {
		return event, fmt.Errorf("restart changed persistent client/monitor address")
	}
	if err = waitFiveReplicaReadiness(bound, js, selectedPart); err != nil {
		return event, err
	}
	event.Healed = time.Now()
	return event, nil
}

// KillNode waits for container removal after literal SIGKILL. Complete every
// kill before any restart so this cannot silently become a rolling restart.
func killFiveContainerMixedAllServers(ctx context.Context, js jetstream.JetStream, cluster *testcluster.DockerCluster, scheduled time.Time, prefix string) (matrixLeaderFault, error) {
	event := matrixLeaderFault{Scheduled: scheduled, Node: -1}
	bound, stop := context.WithTimeout(ctx, 60*time.Second)
	defer stop()
	type operation struct {
		Node   int
		Action string
		At     time.Time
	}
	var operations []operation
	write := func() error {
		data, err := json.MarshalIndent(operations, "", "  ")
		if err != nil {
			return err
		}
		return os.WriteFile(prefix+"-restart-operations.json", data, 0644)
	}
	defer func() { _ = write() }()
	var clients, monitors [5]string
	for node := 0; node < 5; node++ {
		clients[node], monitors[node] = cluster.ClientURL(node), cluster.MonitorURL(node)
		logs, err := cluster.Logs(node)
		if err != nil {
			return event, err
		}
		if err = os.WriteFile(fmt.Sprintf("%s-node%d-before.log", prefix, node), []byte(logs), 0644); err != nil {
			return event, err
		}
	}
	event.Killed = time.Now()
	for node := 0; node < 5; node++ {
		if bound.Err() != nil {
			return event, bound.Err()
		}
		if err := cluster.KillNode(node); err != nil {
			return event, err
		}
		event.Nodes = append(event.Nodes, node)
		operations = append(operations, operation{node, "sigkill_removed", time.Now().UTC()})
	}
	// Persist the all-down boundary before creating any replacement process.
	if err := write(); err != nil {
		return event, err
	}
	for node := 0; node < 5; node++ {
		if bound.Err() != nil {
			return event, bound.Err()
		}
		if err := cluster.RestartNode(node); err != nil {
			return event, err
		}
		if cluster.ClientURL(node) != clients[node] || cluster.MonitorURL(node) != monitors[node] {
			return event, fmt.Errorf("restart changed node%d persistent endpoints", node)
		}
		operations = append(operations, operation{node, "restarted", time.Now().UTC()})
	}
	if err := waitFiveReplicaReadiness(bound, js, 0); err != nil {
		return event, err
	}
	event.Healed = time.Now()
	if err := write(); err != nil {
		return event, err
	}
	return event, nil
}
