//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
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

type matrixLeaderFault struct {
	Scheduled time.Time `json:"scheduled"`
	Killed    time.Time `json:"killed"`
	Healed    time.Time `json:"healed"`
	Node      int       `json:"node"`
}

// One sustained row of the release matrix. Shortened runs are smoke evidence;
// the default exercises ten minutes on the same stores and worker fleet.
func TestMixedMatrixJournalLeaderEveryThirtySeconds(t *testing.T) {
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
	seed, err := testcluster.SeedFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	cluster, err := testcluster.StartProcesses(t.TempDir(), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	urls := make([]string, 3)
	for i := range urls {
		urls[i] = cluster.ClientURL(i)
	}
	nc, err := nats.Connect(strings.Join(urls, ","), nats.MaxReconnects(-1), nats.ReconnectWait(100*time.Millisecond), nats.Timeout(time.Second))
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
		err = provision.Ensure(attempt, js, 3)
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
	var recorder history.Recorder
	var dispatchMu sync.Mutex
	var dispatch []worker.DispatchEvent
	c := client.NewObserved(js, &recorder)
	var faults []matrixLeaderFault
	var faultsMu sync.Mutex
	defer func() {
		prefix := os.Getenv("MATRIX_ARTIFACT_PREFIX")
		if prefix == "" {
			return
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
	w, err := worker.New(ctx, js, "matrix-worker", matrixLeaderHandlers(), worker.WithPartitionConcurrency(4), worker.WithDispatchObserver(func(event worker.DispatchEvent) {
		dispatchMu.Lock()
		dispatch = append(dispatch, event)
		dispatchMu.Unlock()
	}))
	if err != nil {
		t.Fatal(err)
	}
	for partition := uint32(0); partition < provision.Partitions; partition++ {
		launch(func() error { return w.RunPartition(workCtx, partition) })
	}
	launch(func() error { return reconcile.RunStartLoop(workCtx, js, "matrix-start", time.Second, 32) })
	launch(func() error { return reconcile.RunSignalLoop(workCtx, js, "matrix-signal", time.Second, 32) })
	launch(func() error { return reconcile.RunSuspendedLoop(workCtx, js, "matrix-suspended", time.Second, 8) })
	start := time.Now()
	end := start.Add(duration)
	faultCtx, stopFault := context.WithCancel(ctx)
	faultDone := make(chan error, 1)
	faultExited := make(chan struct{})
	go func() {
		defer close(faultExited)
		defer close(faultDone)
		for scheduled := start.Add(30 * time.Second); scheduled.Before(end); scheduled = scheduled.Add(30 * time.Second) {
			timer := time.NewTimer(time.Until(scheduled))
			select {
			case <-faultCtx.Done():
				timer.Stop()
				faultDone <- faultCtx.Err()
				return
			case <-timer.C:
			}
			event, err := killMatrixJournalLeader(faultCtx, js, cluster, scheduled)
			faultsMu.Lock()
			faults = append(faults, event)
			faultsMu.Unlock()
			if err != nil {
				faultDone <- err
				cancel()
				return
			}
			t.Logf("journal leader fault node=%d scheduled=%s killed=%s healed=%s", event.Node, event.Scheduled.Sub(start), event.Killed.Sub(start), event.Healed.Sub(start))
		}
	}()
	defer func() { stopFault(); <-faultExited }()
	rng := rand.New(rand.NewSource(seed))
	var latencies []time.Duration
	latenciesByType := map[string][]time.Duration{}
	batches := 0
	for time.Now().Before(end) {
		select {
		case err := <-faultDone:
			if err != nil {
				t.Fatalf("leader fault: %v", err)
			}
		case err := <-fleetErrors:
			t.Fatalf("fleet: %v", err)
		default:
		}
		kinds := []string{"matrixshort", "matrixshort", "matrixshort", "matrixshort", "matrixtimer", "matrixtimer", "matrixtimer", "matrixsignal", "matrixsignal", "matrixfanout"}
		rng.Shuffle(len(kinds), func(i, j int) { kinds[i], kinds[j] = kinds[j], kinds[i] })
		var batch sync.WaitGroup
		batchCtx, stopBatch := context.WithTimeout(ctx, 5*time.Minute)
		batchErrors := make(chan error, len(kinds))
		for i, typ := range kinds {
			id := fmt.Sprintf("seed-%d-batch-%d-%d", seed, batches, i)
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
			t.Fatal(err)
		}
		batches++
		if batches%10 == 0 {
			t.Logf("checkpoint audit batch=%d started elapsed=%s", batches, time.Since(start))
			report, err := matrixRetainedAudit(ctx, js)
			if err != nil || report.Invocations != batches*28 || report.Journals != batches*28 || report.Terminal != batches*28 {
				t.Fatalf("intermediate retained audit batch=%d report=%+v err=%v", batches, report, err)
			}
		}
		t.Logf("mixed batch=%d elapsed=%s", batches, time.Since(start))
	}
	if err := <-faultDone; err != nil {
		t.Fatalf("leader fault: %v", err)
	}
	wantFaults := int((duration - time.Nanosecond) / (30 * time.Second))
	if len(faults) != wantFaults {
		t.Fatalf("faults=%d want=%d", len(faults), wantFaults)
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
	for sequence := info.State.FirstSeq; sequence <= info.State.LastSeq; sequence++ {
		msg, err := inv.GetMsg(ctx, sequence)
		if err != nil {
			t.Fatal(err)
		}
		parts := strings.Split(msg.Subject, ".")
		attempt, done := context.WithTimeout(ctx, 20*time.Second)
		latency, err := matrixTerminalLatency(attempt, js, parts[2], parts[3], msg.Time, completionDeadline)
		done()
		if err != nil {
			t.Fatal(err)
		}
		latencies = append(latencies, latency)
		latenciesByType[parts[2]] = append(latenciesByType[parts[2]], latency)
	}
	report, err := matrixRetainedAudit(ctx, js)
	want := batches * 28
	if err != nil || report.Invocations != want || report.Journals != want || report.Terminal != want {
		t.Fatalf("mixed retained state=%+v want=%d err=%v", report, want, err)
	}
	for _, check := range []func([]client.Operation, time.Duration) (porcupine.CheckResult, error){history.CheckStarts, history.CheckSignals, history.CheckResults} {
		result, err := check(recorder.Snapshot(), 30*time.Second)
		if err != nil || result != porcupine.Ok {
			t.Fatalf("history=%v err=%v", result, err)
		}
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p99 := latencies[(len(latencies)*99+99)/100-1]
	t.Logf("MATRIX_RESULT row=journal_leader seed=%d duration=%s release_duration=%t batches=%d invocations=%d faults=%d terminal_p99=%s", seed, duration, duration == 10*time.Minute, batches, want, len(faults), p99)
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
	}
	run, err := js.Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	drainCtx, stopDrain := context.WithTimeout(ctx, 30*time.Second)
	defer stopDrain()
	for {
		info, err := run.Info(drainCtx)
		if err == nil && info.State.Msgs == 0 {
			break
		}
		if drainCtx.Err() != nil {
			t.Fatalf("run queue did not drain: info=%+v err=%v", info, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	stopWork()
	fleet.Wait()
}

// Lost read replies during a leader kill must not consume the entire workload
// duration. Every attempt still checks the whole retained state; only named
// transient transport failures get a fresh context, never invariant errors.
func matrixRetainedAudit(ctx context.Context, js jetstream.JetStream) (integrity.Report, error) {
	var report integrity.Report
	var err error
	for i := 0; i < 3; i++ {
		attempt, done := context.WithTimeout(ctx, 20*time.Second)
		report, err = integrity.Check(attempt, js)
		done()
		if err == nil {
			return report, nil
		}
		if ctx.Err() != nil || !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, nats.ErrTimeout) && !errors.Is(err, nats.ErrNoResponders) {
			break
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
		if !errors.Is(err, client.ErrStartUnknown) && !errors.Is(err, client.ErrSignalUnknown) && !errors.Is(err, client.ErrEnqueueUnknown) && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, nats.ErrTimeout) && !errors.Is(err, nats.ErrNoResponders) {
			return err
		}
		select {
		case <-bound.Done():
			return fmt.Errorf("client retry: %w (last reply: %v)", bound.Err(), err)
		case <-time.After(100 * time.Millisecond):
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
	stream, err := js.Stream(attempt, "WF_JRN")
	if err != nil {
		return event, err
	}
	info, err := stream.Info(attempt)
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

func matrixTerminalLatency(ctx context.Context, js jetstream.JetStream, typ, id string, enabled, completionDeadline time.Time) (time.Duration, error) {
	records, _, err := journal.New(js).Read(ctx, typ, id)
	if err != nil {
		return 0, err
	}
	stream, err := js.Stream(ctx, "WF_JRN")
	if err != nil {
		return 0, err
	}
	for _, record := range records {
		if record.Kind == journal.StepRequested {
			var request struct {
				FireAt    time.Time `json:"fire_at"`
				ChildType string    `json:"child_type"`
				ChildID   string    `json:"child_id"`
			}
			if err := json.Unmarshal(record.Payload, &request); err != nil {
				return 0, err
			}
			if request.FireAt.After(enabled) {
				enabled = request.FireAt
			}
			if request.ChildID != "" {
				child, err := stream.GetLastMsgForSubject(ctx, identity.JournalSubject(request.ChildType, request.ChildID))
				if err != nil {
					return 0, err
				}
				if child.Time.After(enabled) {
					enabled = child.Time
				}
			}
		}
		if record.Kind == journal.SignalConsumed {
			var signal struct {
				Sequence uint64 `json:"sig_seq"`
			}
			if err := json.Unmarshal(record.Payload, &signal); err != nil {
				return 0, err
			}
			signals, err := js.Stream(ctx, "WF_SIG")
			if err != nil {
				return 0, err
			}
			msg, err := signals.GetMsg(ctx, signal.Sequence)
			if err != nil {
				return 0, err
			}
			if msg.Time.After(enabled) {
				enabled = msg.Time
			}
		}
	}
	last, err := stream.GetLastMsgForSubject(ctx, identity.JournalSubject(typ, id))
	if err != nil {
		return 0, err
	}
	if last.Time.After(completionDeadline) {
		return 0, fmt.Errorf("%s/%s completed at %s after post-heal deadline %s", typ, id, last.Time, completionDeadline)
	}
	return max(time.Duration(0), last.Time.Sub(enabled)), nil
}
