package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/history"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/provision"
	"js-wf/reconcile"
	"js-wf/testcluster"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/anishathalye/porcupine"
	"github.com/nats-io/nats.go/jetstream"
)

func setup(t *testing.T) ([]jetstream.JetStream, *testcluster.Cluster) {
	t.Helper()
	c, err := testcluster.Start(t.TempDir(), 3)
	if err != nil {
		t.Fatal(err)
	}
	return setupCluster(t, c)
}

func setupCluster(t *testing.T, c *testcluster.Cluster) ([]jetstream.JetStream, *testcluster.Cluster) {
	t.Helper()
	t.Cleanup(c.Close)
	var all []jetstream.JetStream
	for _, nc := range c.Clients {
		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, js)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for {
		attempt, done := context.WithTimeout(ctx, 2*time.Second)
		_, readyErr := all[0].AccountInfo(attempt)
		done()
		if readyErr != nil {
			if ctx.Err() != nil {
				t.Fatalf("jetstream ready: %v", readyErr)
			}
			time.Sleep(50 * time.Millisecond)
			continue
		}
		attempt, done = context.WithTimeout(ctx, 3*time.Second)
		err := provision.Ensure(attempt, all[0], 3)
		done()
		if err == nil {
			break
		} else if ctx.Err() != nil {
			t.Fatalf("provision: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	return all, c
}

func TestConcurrentStart(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	recorder := &history.Recorder{}
	var wg sync.WaitGroup
	var started, already, other atomic.Int64
	unexpected := make(chan string, 500)
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			h, err := client.NewObserved(all[i%3], recorder).Start(ctx, "test", "same", []byte(`{"n":1}`))
			if h.InvSeq == 0 {
				other.Add(1)
				unexpected <- fmt.Sprintf("inv_seq=0: %v", err)
				return
			}
			switch {
			case err == nil:
				started.Add(1)
			case errors.Is(err, client.ErrAlreadyStarted):
				already.Add(1)
			default:
				other.Add(1)
				unexpected <- fmt.Sprintf("inv_seq=%d: %v", h.InvSeq, err)
			}
		}(i)
	}
	wg.Wait()
	if started.Load() != 1 || already.Load() != 499 || other.Load() != 0 {
		close(unexpected)
		counts := map[string]int{}
		for result := range unexpected {
			counts[result]++
		}
		t.Fatalf("started=%d already=%d other=%d unexpected=%v", started.Load(), already.Load(), other.Load(), counts)
	}
	inv, err := all[0].Stream(ctx, "WF_INV")
	if err != nil {
		t.Fatal(err)
	}
	info, err := inv.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("invocation messages=%d", info.State.Msgs)
	}
	run, err := all[0].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	info, err = run.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("run messages=%d", info.State.Msgs)
	}
	_, err = client.NewObserved(all[1], recorder).Start(ctx, "test", "same", []byte(`{"n":2}`))
	if !errors.Is(err, client.ErrInputMismatch) {
		t.Fatalf("mismatch: %v", err)
	}
	result, checkErr := history.CheckStarts(recorder.Snapshot(), 15*time.Second)
	if checkErr != nil || result != porcupine.Ok {
		path := filepath.Join(t.TempDir(), "start-history.jsonl")
		file, fileErr := os.Create(path)
		if fileErr == nil {
			_ = recorder.WriteJSONL(file)
			_ = file.Close()
		}
		t.Fatalf("500-client start history=%s err=%v history=%s", result, checkErr, path)
	}
}

func TestJournalCAS(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	a, b := journal.New(all[0]), journal.New(all[1])
	seq, err := a.Append(ctx, "test", "race", journal.Entry{Epoch: 0, Index: 0, Kind: journal.Started}, 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 100; i++ {
		var wg sync.WaitGroup
		wg.Add(2)
		results := make(chan error, 2)
		for n, s := range []*journal.Store{a, b} {
			go func(n int, s *journal.Store) {
				defer wg.Done()
				_, e := s.Append(ctx, "test", "race", journal.Entry{Epoch: 1, Index: i, Kind: journal.StepCompleted, WorkerID: fmt.Sprint(n)}, seq)
				results <- e
			}(n, s)
		}
		wg.Wait()
		close(results)
		var wins, stales int
		for e := range results {
			if e == nil {
				wins++
			} else if errors.Is(e, journal.ErrStale) {
				stales++
			} else {
				t.Fatalf("round %d: %v", i, e)
			}
		}
		if wins != 1 || stales != 1 {
			t.Fatalf("round %d: wins=%d stales=%d", i, wins, stales)
		}
		records, tail, e := a.Read(ctx, "test", "race")
		if e != nil {
			t.Fatal(e)
		}
		if len(records) != int(i+1) {
			t.Fatalf("records=%d", len(records))
		}
		seq = tail
	}
}

func TestWorkerReplayAndCompletion(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var effects atomic.Int64
	handler := func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
		var n int
		if err := json.Unmarshal(input, &n); err != nil {
			return nil, err
		}
		value, err := wf.Run(c, "double", n, func(context.Context) (int, error) { effects.Add(1); return n * 2, nil })
		if err != nil {
			return nil, err
		}
		out, _ := json.Marshal(value)
		return out, nil
	}
	w, err := worker.New(ctx, all[1], "worker-1", map[string]worker.Handler{"test": handler})
	if err != nil {
		t.Fatal(err)
	}
	c := client.New(all[0])
	_, err = c.Start(ctx, "test", "workflow", []byte(`21`))
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() {
		done <- w.RunPartition(workerCtx, identity.Partition("test", "workflow", provision.Partitions))
	}()
	result, err := c.Await(ctx, "test", "workflow")
	if err != nil {
		select {
		case runErr := <-done:
			t.Fatalf("await: %v, worker: %v", err, runErr)
		default:
			t.Fatalf("await: %v", err)
		}
	}
	if string(result) != "42" || effects.Load() != 1 {
		t.Fatalf("result=%s effects=%d", result, effects.Load())
	}
	stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("worker: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not stop")
	}
	// A new worker reading the terminal journal returns the immutable outcome.
	result, err = c.Await(ctx, "test", "workflow")
	if err != nil || string(result) != "42" || effects.Load() != 1 {
		t.Fatalf("reread=%s err=%v effects=%d", result, err, effects.Load())
	}
	report, err := integrity.Check(ctx, all[0])
	if err != nil {
		t.Fatal(err)
	}
	if report.Invocations != 1 || report.Journals != 1 || report.Terminal != 1 {
		t.Fatalf("integrity report: %+v", report)
	}
}

func TestLeaseFenceAndStartRepair(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	a, err := lease.New(ctx, all[0])
	if err != nil {
		t.Fatal(err)
	}
	b, err := lease.New(ctx, all[1])
	if err != nil {
		t.Fatal(err)
	}
	first, err := a.Acquire(ctx, "test", "fence", "a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Acquire(ctx, "test", "fence", "b"); !errors.Is(err, lease.ErrHeld) {
		t.Fatalf("second acquire: %v", err)
	}
	if err := first.Renew(ctx); err != nil {
		t.Fatal(err)
	}
	if err := first.Release(ctx); err != nil {
		t.Fatal(err)
	}
	second, err := b.Acquire(ctx, "test", "fence", "b")
	if err != nil {
		t.Fatal(err)
	}
	if second.Epoch() <= first.Epoch() {
		t.Fatalf("epochs %d then %d", first.Epoch(), second.Epoch())
	}
	if err := first.Renew(ctx); !errors.Is(err, lease.ErrLost) {
		t.Fatalf("stale renew: %v", err)
	}
	if err := second.Release(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = all[0].Publish(ctx, "wf.inv.test.unqueued", []byte(`1`))
	if err != nil {
		t.Fatal(err)
	}
	scan, err := reconcile.NewStartScan(all[2]).Scan(ctx, 0, 10, false)
	if err != nil {
		t.Fatal(err)
	}
	if scan.Reenqueued != 1 {
		t.Fatalf("repaired=%d", scan.Reenqueued)
	}
	run, err := all[0].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	info, err := run.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("run messages=%d", info.State.Msgs)
	}
}

func TestLargeInputObjectStore(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	input := []byte(`"` + strings.Repeat("x", 2*1024*1024) + `"`)
	c := client.New(all[0])
	_, err := c.Start(ctx, "test", "large", input)
	if err != nil {
		t.Fatal(err)
	}
	w, err := worker.New(ctx, all[1], "large-worker", map[string]worker.Handler{"test": func(_ *wf.Context, data json.RawMessage) (json.RawMessage, error) {
		if len(data) != len(input) {
			return nil, fmt.Errorf("input length %d, want %d", len(data), len(input))
		}
		return json.RawMessage(`true`), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- w.RunPartition(workerCtx, identity.Partition("test", "large", provision.Partitions)) }()
	result, err := c.Await(ctx, "test", "large")
	if err != nil || string(result) != "true" {
		t.Fatalf("result=%s err=%v", result, err)
	}
	stop()
	<-done
}

func TestStartReconcileLoop(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- reconcile.RunStartLoop(ctx, all[1], "scanner", 100*time.Millisecond, 10) }()
	_, err := all[0].Publish(ctx, "wf.inv.test.crashgap", []byte(`1`))
	if err != nil {
		t.Fatal(err)
	}
	run, err := all[0].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	for {
		info, err := run.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if info.State.Msgs == 1 {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("reconciler did not enqueue invocation")
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("loop: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("reconciler did not stop")
	}
}

func TestWorkerWaitsForRun(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	w, err := worker.New(ctx, all[1], "idle-worker", map[string]worker.Handler{"test": func(_ *wf.Context, _ json.RawMessage) (json.RawMessage, error) { return json.RawMessage(`true`), nil }})
	if err != nil {
		t.Fatal(err)
	}
	partition := identity.Partition("test", "late", provision.Partitions)
	done := make(chan error, 1)
	go func() { done <- w.RunPartition(ctx, partition) }()
	time.Sleep(1500 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("worker exited while idle: %v", err)
	default:
	}
	c := client.New(all[0])
	if _, err := c.Start(ctx, "test", "late", []byte(`1`)); err != nil {
		t.Fatal(err)
	}
	result, err := c.Await(ctx, "test", "late")
	if err != nil || string(result) != "true" {
		t.Fatalf("result=%s err=%v", result, err)
	}
	cancel()
	<-done
}

func TestProvisionRejectsMismatch(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := provision.Ensure(ctx, all[1], 3); err != nil {
		t.Fatalf("idempotent provision: %v", err)
	}
	sig, err := all[0].Stream(ctx, "WF_SIG")
	if err != nil {
		t.Fatal(err)
	}
	info, err := sig.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cfg := info.Config
	cfg.MaxAge = 3 * time.Minute
	if _, err := all[0].UpdateStream(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	if err := provision.Ensure(ctx, all[1], 3); err == nil {
		t.Fatal("accepted changed WF_SIG retention")
	}
}

func TestAckedPublishesSurviveLeaderKill(t *testing.T) {
	all, cluster := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	seed, err := testcluster.SeedFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	killAt := 1000 + rand.New(rand.NewSource(seed)).Intn(8000)
	t.Logf("FAULT_SEED=%d kill_after_attempt=%d", seed, killAt)
	stream, err := all[0].CreateStream(ctx, jetstream.StreamConfig{Name: "PLAIN", Subjects: []string{"plain"}, Storage: jetstream.FileStorage, Replicas: 3, Discard: jetstream.DiscardNew})
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	leader := -1
	for i, s := range cluster.Servers {
		if s.Name() == info.Cluster.Leader {
			leader = i
		}
	}
	if leader < 0 {
		t.Fatalf("unknown leader %q", info.Cluster.Leader)
	}
	publisher := (leader + 1) % 3
	acked := map[uint64]string{}
	var ackMu sync.Mutex
	var attempts atomic.Int64
	var killed atomic.Bool
	var ackedAfter atomic.Int64
	killNow := make(chan struct{})
	killDone := make(chan struct{})
	go func() {
		<-killNow
		cluster.KillNode(leader)
		killed.Store(true)
		close(killDone)
	}()
	var writers sync.WaitGroup
	conflicts := make(chan error, 1)
	for writer := 0; writer < 10; writer++ {
		writers.Add(1)
		go func(writer int) {
			defer writers.Done()
			for n := 0; n < 1000; n++ {
				if attempts.Add(1) == int64(killAt) {
					close(killNow)
				}
				attempt, done := context.WithTimeout(ctx, 400*time.Millisecond)
				payload := fmt.Sprint(writer*1000 + n)
				ack, err := all[publisher].Publish(attempt, "plain", []byte(payload))
				done()
				if err != nil {
					continue // no ack leaves the publish outcome unknown
				}
				if killed.Load() {
					ackedAfter.Add(1)
				}
				ackMu.Lock()
				if prior, duplicate := acked[ack.Sequence]; duplicate && prior != payload {
					select {
					case conflicts <- fmt.Errorf("sequence %d acked for %q and %q", ack.Sequence, prior, payload):
					default:
					}
				}
				acked[ack.Sequence] = payload
				ackMu.Unlock()
			}
		}(writer)
	}
	writers.Wait()
	<-killDone
	select {
	case conflict := <-conflicts:
		t.Fatal(conflict)
	default:
	}
	if len(acked) == 0 || ackedAfter.Load() == 0 {
		t.Fatalf("acked=%d after kill=%d", len(acked), ackedAfter.Load())
	}
	var recovered jetstream.Stream
	for ctx.Err() == nil {
		recovered, err = all[publisher].Stream(ctx, "PLAIN")
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := auditPlainStream(ctx, recovered, acked); err != nil {
		t.Fatal(err)
	}
}

func auditPlainStream(ctx context.Context, stream jetstream.Stream, acked map[uint64]string) error {
	info, err := stream.Info(ctx)
	if err != nil {
		return err
	}
	if info.State.FirstSeq != 1 || info.State.Msgs != info.State.LastSeq {
		return fmt.Errorf("plain stream has sequence gaps: first=%d last=%d messages=%d", info.State.FirstSeq, info.State.LastSeq, info.State.Msgs)
	}
	for seq := uint64(1); seq <= info.State.LastSeq; seq++ {
		message, err := stream.GetMsg(ctx, seq)
		if err != nil {
			return fmt.Errorf("sequence %d missing: %w", seq, err)
		}
		if expected, ok := acked[seq]; ok && string(message.Data) != expected {
			return fmt.Errorf("acked sequence %d has %q, want %q", seq, message.Data, expected)
		}
	}
	for seq := range acked {
		if seq > info.State.LastSeq {
			return fmt.Errorf("acked sequence %d past stream end %d", seq, info.State.LastSeq)
		}
	}
	return nil
}

func TestPlainReplicationNegativeControl(t *testing.T) {
	all, cluster := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	stream, err := all[0].CreateStream(ctx, jetstream.StreamConfig{Name: "PLAIN_ONE", Subjects: []string{"plain.one"}, Storage: jetstream.FileStorage, Replicas: 1, Discard: jetstream.DiscardNew})
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	leader := -1
	for i, server := range cluster.Servers {
		if server.Name() == info.Cluster.Leader {
			leader = i
		}
	}
	if leader < 0 {
		t.Fatalf("unknown leader %q", info.Cluster.Leader)
	}
	publisher := (leader + 1) % len(all)
	acked := map[uint64]string{}
	for i := 0; i < 100; i++ {
		payload := fmt.Sprint(i)
		ack, err := all[publisher].Publish(ctx, "plain.one", []byte(payload))
		if err != nil {
			t.Fatal(err)
		}
		acked[ack.Sequence] = payload
	}
	if err := auditPlainStream(ctx, stream, acked); err != nil {
		t.Fatalf("control audit before kill: %v", err)
	}
	cluster.KillNode(leader)
	probeCtx, stop := context.WithTimeout(ctx, 2*time.Second)
	defer stop()
	remaining, err := all[publisher].Stream(probeCtx, "PLAIN_ONE")
	if err == nil {
		err = auditPlainStream(probeCtx, remaining, acked)
	}
	if err == nil {
		t.Fatal("one-replica stream passed audit after its only copy was killed")
	}
}
