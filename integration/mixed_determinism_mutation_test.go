//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/provision"
	"js-wf/testcluster"
	"js-wf/wf"
	"js-wf/worker"
)

// The renamed step is delivered through the production worker after a real
// journal leader SIGKILL, while all four workload classes share retained stores.
func TestMixedDeterminismMutationAfterJournalLeaderKill(t *testing.T) {
	if os.Getenv("WF_MIXED_DETERMINISM_MUTATION") != "1" {
		t.Skip("set WF_MIXED_DETERMINISM_MUTATION=1 for the mixed production guard challenge")
	}
	runMixedGuardMutation(t, false)
}

func TestMixedLeaseMutationAfterJournalLeaderKill(t *testing.T) {
	if os.Getenv("WF_MIXED_LEASE_MUTATION") != "1" {
		t.Skip("set WF_MIXED_LEASE_MUTATION=1 for the mixed lease challenge")
	}
	runMixedGuardMutation(t, true)
}

func TestMixedCASMutationAfterJournalLeaderKill(t *testing.T) {
	if os.Getenv("WF_MIXED_CAS_MUTATION") != "1" {
		t.Skip("set WF_MIXED_CAS_MUTATION=1 for the mixed CAS challenge")
	}
	runMixedGuardChallenge(t, "cas")
}

func TestMixedEnqueueMutationAfterJournalLeaderKill(t *testing.T) {
	if os.Getenv("WF_MIXED_ENQUEUE_MUTATION") != "1" {
		t.Skip("set WF_MIXED_ENQUEUE_MUTATION=1 for the mixed enqueue challenge")
	}
	runMixedGuardChallenge(t, "enqueue")
}

func TestMixedStartRepairMutationAfterJournalLeaderKill(t *testing.T) {
	if os.Getenv("WF_MIXED_START_REPAIR_MUTATION") != "1" {
		t.Skip("set WF_MIXED_START_REPAIR_MUTATION=1 for the mixed start repair challenge")
	}
	runMixedGuardChallenge(t, "start-repair")
}

func TestMixedPurgeMutationAfterJournalLeaderKill(t *testing.T) {
	if os.Getenv("WF_MIXED_PURGE_MUTATION") != "1" {
		t.Skip("set WF_MIXED_PURGE_MUTATION=1 for the mixed purge challenge")
	}
	runMixedGuardChallenge(t, "purge")
}

func runMixedGuardMutation(t *testing.T, leaseChallenge bool) {
	mode := "determinism"
	if leaseChallenge {
		mode = "leases"
	}
	runMixedGuardChallenge(t, mode)
}
func runMixedGuardChallenge(t *testing.T, mode string) {
	t.Helper()
	seed, err := testcluster.SeedFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("FAULT_SEED=%d mixed_lease_challenge=%t", seed, mode == "leases")
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	cluster, err := testcluster.StartProcesses(t.TempDir(), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	nodes := make([]jetstream.JetStream, 3)
	for i, nc := range cluster.Clients {
		nodes[i], err = jetstream.New(nc)
		if err != nil {
			t.Fatal(err)
		}
	}
	for until := time.Now().Add(20 * time.Second); time.Now().Before(until); {
		attempt, stop := context.WithTimeout(ctx, 3*time.Second)
		err = provision.Ensure(attempt, nodes[0], 3)
		stop()
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	runMixedGuardChallengeOnCluster(t, ctx, cluster, nodes, mode, 0)
}

// Borrow the already exercised cluster and retain its completed workload. Each
// guard challenge gets its existing 150s budget even after a sustained row.
func runMixedGuardChallengeOnCluster(t *testing.T, parent context.Context, cluster *testcluster.ProcessCluster, nodes []jetstream.JetStream, mode string, priorInvocations int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 150*time.Second)
	defer cancel()
	leaseChallenge := mode == "leases"
	casChallenge := mode == "cas"
	enqueueChallenge := mode == "enqueue"
	startRepairChallenge := mode == "start-repair"
	purgeChallenge := mode == "purge"
	type invocation struct {
		typ, id string
		input   int
		want    string
	}
	var cohort []invocation
	parts := map[uint32]bool{}
	for _, kind := range []struct {
		typ   string
		count int
		want  string
	}{{"guardshort", 4, "42"}, {"guardtimer", 3, "42"}, {"guardsignal", 2, "42"}, {"guardfanout", 1, "12"}} {
		for index := 0; index < kind.count; index++ {
			for nonce := 0; ; nonce++ {
				id := fmt.Sprintf("%s-%d-%d", kind.typ, index, nonce)
				part := identity.Partition(kind.typ, id, provision.Partitions)
				if !parts[part] {
					parts[part] = true
					cohort = append(cohort, invocation{kind.typ, id, index, kind.want})
					break
				}
			}
		}
	}
	entered := make(chan int, 4)
	var enteredOnce [4]sync.Once
	var changedEffects atomic.Int64
	replacementEntered := make(chan int, 4)
	var replacementOnce [4]sync.Once
	releaseReplacement := make(chan struct{})
	var releaseOnce sync.Once
	releaseEffects := func() { releaseOnce.Do(func() { close(releaseReplacement) }) }
	defer releaseEffects()
	handlers := func(replacement bool) map[string]worker.Handler {
		return map[string]worker.Handler{
			"guardshort": func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
				var index int
				if err := json.Unmarshal(input, &index); err != nil {
					return nil, err
				}
				if index < 0 || index >= len(enteredOnce) {
					return nil, fmt.Errorf("invalid short index%d", index)
				}
				name := "held"
				if replacement && index == 0 && mode == "determinism" {
					name = "changed"
				}
				value, err := wf.Run(c, name, 0, func(effectCtx context.Context) (int, error) {
					if !replacement {
						enteredOnce[index].Do(func() { entered <- index })
						<-effectCtx.Done()
						return 0, effectCtx.Err()
					}
					if leaseChallenge {
						replacementOnce[index].Do(func() { replacementEntered <- index })
						select {
						case <-releaseReplacement:
						case <-effectCtx.Done():
							return 0, effectCtx.Err()
						}
					}
					if index == 0 {
						changedEffects.Add(1)
					}
					return 42, nil
				})
				if err != nil {
					return nil, err
				}
				return json.Marshal(value)
			},
			"guardtimer": func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
				timers := make([]*wf.TimerHandle, 8)
				for i := range timers {
					var err error
					timers[i], err = c.Timer(fmt.Sprintf("wait-%d", i), 30*time.Second)
					if err != nil {
						return nil, err
					}
				}
				for _, timer := range timers {
					if err := timer.Await(); err != nil {
						return nil, err
					}
				}
				return json.RawMessage(`42`), nil
			},
			"guardsignal": func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
				for i := 0; i < 16; i++ {
					data, err := wf.AwaitSignal(c, "go")
					if err != nil {
						return nil, err
					}
					if string(data) != strconv.Itoa(i) {
						return nil, fmt.Errorf("signal order %d: %s", i, data)
					}
				}
				return json.RawMessage(`42`), nil
			},
			"guardfanout": func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
				promises := make([]wf.Promise, 6)
				for i := range promises {
					var err error
					promises[i], err = wf.CallAsync(c, "guardchild", []byte(strconv.Itoa(i)))
					if err != nil {
						return nil, err
					}
				}
				sum := 0
				for _, promise := range promises {
					data, err := wf.AwaitPromise(c, promise)
					if err != nil {
						return nil, err
					}
					var value int
					if err := json.Unmarshal(data, &value); err != nil {
						return nil, err
					}
					sum += value
				}
				return json.RawMessage(strconv.Itoa(sum)), nil
			},
			"guardchild": func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
				promises := make([]wf.Promise, 2)
				for i := range promises {
					var err error
					promises[i], err = wf.CallAsync(c, "guardgrandchild", []byte(strconv.Itoa(i)))
					if err != nil {
						return nil, err
					}
				}
				sum := 0
				for _, promise := range promises {
					data, err := wf.AwaitPromise(c, promise)
					if err != nil {
						return nil, err
					}
					var value int
					if err := json.Unmarshal(data, &value); err != nil {
						return nil, err
					}
					sum += value
				}
				return json.RawMessage(strconv.Itoa(sum)), nil
			},
			"guardgrandchild": func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
				value, err := wf.Run(c, "one", nil, func(effectCtx context.Context) (int, error) {
					if !replacement {
						<-effectCtx.Done()
						return 0, effectCtx.Err()
					}
					return 1, nil
				})
				if err != nil {
					return nil, err
				}
				return json.RawMessage(strconv.Itoa(value)), nil
			},
		}
	}
	// Each stop joins all partition loops before the next worker generation.
	startWorker := func(id string, js jetstream.JetStream, replacement bool) func() {
		t.Helper()
		var w *worker.Worker
		var err error
		for until := time.Now().Add(30 * time.Second); time.Now().Before(until) && ctx.Err() == nil; {
			attempt, stop := context.WithTimeout(ctx, 3*time.Second)
			w, err = worker.New(attempt, js, id, handlers(replacement))
			stop()
			if err == nil {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if err != nil || w == nil {
			t.Fatal("worker initialization", err)
		}
		runCtx, stop := context.WithCancel(ctx)
		var wg sync.WaitGroup
		results := make(chan error, provision.Partitions)
		for part := uint32(0); part < provision.Partitions; part++ {
			wg.Add(1)
			go func(part uint32) { defer wg.Done(); results <- w.RunPartition(runCtx, part) }(part)
		}
		var once sync.Once
		return func() {
			once.Do(func() {
				stop()
				wg.Wait()
				close(results)
				for err := range results {
					if err != nil {
						t.Errorf("worker %s: %v", id, err)
					}
				}
			})
		}
	}
	c := client.New(nodes[0])
	for _, inv := range cohort {
		if _, err := c.Start(ctx, inv.typ, inv.id, []byte(strconv.Itoa(inv.input))); err != nil {
			t.Fatal(err)
		}
	}
	stopFirst := startWorker("mixed-guard-before", nodes[1], false)
	defer stopFirst()
	seenShorts := map[int]bool{}
	for len(seenShorts) < 4 {
		select {
		case index := <-entered:
			if seenShorts[index] {
				t.Fatal("duplicate short admission", index)
			}
			seenShorts[index] = true
		case <-ctx.Done():
			t.Fatal("short effects never entered")
		}
	}
	j := journal.New(nodes[0])
	target := cohort[0]
	prefix, _, err := j.Read(ctx, target.typ, target.id)
	if err != nil || len(prefix) != 2 || prefix[1].Kind != journal.StepRequested {
		t.Fatalf("missing committed pending prefix: %+v %v", prefix, err)
	}
	// Verify durable timer and signal waits before interrupting the cluster.
	for _, inv := range cohort {
		if inv.typ != "guardtimer" && inv.typ != "guardsignal" && inv.typ != "guardfanout" {
			continue
		}
		for ctx.Err() == nil {
			records, _, err := j.Read(ctx, inv.typ, inv.id)
			if err == nil && len(records) > 0 && records[len(records)-1].Kind == journal.Suspended {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if ctx.Err() != nil {
			t.Fatal(ctx.Err())
		}
	}
	t.Log("MIXED_GUARD_ADMISSION shorts_pending=4 timers_suspended=3 signals_suspended=2 fanout_suspended=1")
	stopFirst()
	var orphan client.Handle
	if startRepairChallenge {
		orphan = admitMixedMissingStart(t, ctx, nodes[0])
	}
	stream, err := nodes[0].Stream(ctx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx)
	if err != nil || info.Cluster == nil {
		t.Fatal("journal leader unavailable", err)
	}
	leader, err := strconv.Atoi(strings.TrimPrefix(info.Cluster.Leader, "wf-process-"))
	if err != nil || leader < 0 || leader >= 3 {
		t.Fatal("invalid journal leader", info.Cluster)
	}
	if err := cluster.KillNode(leader); err != nil {
		t.Fatal(err)
	}
	state := cluster.Commands[leader].ProcessState
	status, ok := state.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatal("journal leader did not exit via SIGKILL")
	}
	t.Logf("MIXED_GUARD_FAULT node=%d pid=%d signal=SIGKILL", leader, state.Pid())
	if err := cluster.RestartNode(leader); err != nil {
		t.Fatal(err)
	}
	survivor := (leader + 1) % 3
	if casChallenge {
		challengeMixedCAS(t, ctx, nodes, survivor, target.typ, target.id, prefix)
	}
	var retainedEnqueues int
	if enqueueChallenge {
		retainedEnqueues = challengeMixedEnqueue(t, ctx, nodes, survivor, target.typ, target.id)
	}
	var startRepaired bool
	if startRepairChallenge {
		startRepaired = challengeMixedStartRepair(t, ctx, nodes[survivor], orphan)
	}
	stopSecond := startWorker("mixed-guard-after", nodes[survivor], true)
	defer stopSecond()
	var leaseEscaped bool
	var challengerEpoch uint64
	if leaseChallenge {
		enteredAfter := map[int]bool{}
		for len(enteredAfter) < 4 {
			select {
			case index := <-replacementEntered:
				if enteredAfter[index] {
					t.Fatal("duplicate replacement short", index)
				}
				enteredAfter[index] = true
			case <-ctx.Done():
				t.Fatal("replacement effects never entered", ctx.Err())
			}
		}
		t.Log("MIXED_LEASE_ADMISSION replacement_shorts_pending=4 owner=mixed-guard-after")
		attempt, stop := context.WithTimeout(ctx, 3*time.Second)
		leases, err := lease.New(attempt, nodes[survivor])
		if err != nil {
			stop()
			t.Fatal(err)
		}
		rival, err := leases.Acquire(attempt, target.typ, target.id, "mixed-lease-challenger")
		stop()
		if err == nil {
			leaseEscaped = true
			challengerEpoch = rival.Epoch()
			releaseCtx, releaseStop := context.WithTimeout(ctx, 3*time.Second)
			releaseErr := rival.Release(releaseCtx)
			releaseStop()
			if releaseErr != nil {
				t.Fatal("challenger cleanup", releaseErr)
			}
		} else if !errors.Is(err, lease.ErrHeld) {
			t.Fatal("unexpected lease challenge", err)
		}
		releaseEffects()
	}
	c = client.New(nodes[survivor])
	for _, inv := range cohort {
		if inv.typ == "guardsignal" {
			for i := 0; i < 16; i++ {
				for {
					attempt, stop := context.WithTimeout(ctx, 3*time.Second)
					_, err := c.Signal(attempt, inv.typ, inv.id, "go", []byte(strconv.Itoa(i)), fmt.Sprintf("ordered-%d", i))
					stop()
					if err == nil {
						break
					}
					if ctx.Err() != nil || !(errors.Is(err, client.ErrSignalUnknown) || errors.Is(err, client.ErrEnqueueUnknown) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrTimeout) || errors.Is(err, nats.ErrNoResponders) || errors.Is(err, jetstream.ErrNoStreamResponse)) {
						t.Fatalf("signal %s index%d: %v", inv.id, i, err)
					}
					t.Logf("MIXED_GUARD_SIGNAL_RETRY id=%s index=%d err=%v", inv.id, i, err)
				}
			}
		}
	}
	for _, inv := range cohort[1:] {
		value, err := c.Await(ctx, inv.typ, inv.id)
		if err != nil || string(value) != inv.want {
			t.Fatalf("mixed %s/%s result=%s err=%v", inv.typ, inv.id, value, err)
		}
	}
	value, resultErr := c.Await(ctx, target.typ, target.id)
	expectedInvocations, expectedTerminals := priorInvocations+28, priorInvocations+28
	if startRepairChallenge {
		expectedInvocations = priorInvocations + 29
		if startRepaired {
			extra, err := c.Await(ctx, orphan.Type, orphan.ID)
			if err != nil || string(extra) != "42" {
				t.Fatalf("repaired start result=%s err=%v", extra, err)
			}
			expectedTerminals = priorInvocations + 29
		}
	}
	var report integrity.Report
	for ctx.Err() == nil {
		report, err = integrity.Check(ctx, nodes[survivor])
		if err == nil && report.Invocations == expectedInvocations && report.Journals == expectedTerminals && report.Terminal == expectedTerminals {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil || report.Invocations != expectedInvocations || report.Journals != expectedTerminals || report.Terminal != expectedTerminals {
		t.Fatalf("mixed retained audit %+v err%v", report, err)
	}
	records, _, err := journal.New(nodes[survivor]).Read(ctx, target.typ, target.id)
	if err != nil || len(records) <= len(prefix) || !reflect.DeepEqual(prefix, records[:len(prefix)]) || records[len(records)-1].Epoch <= prefix[1].Epoch {
		t.Fatalf("lost prefix or epoch handoff: %v", err)
	}
	t.Logf("MIXED_GUARD_COHORT shorts=4 timers=3 signals=2 fanout=1 children=6 grandchildren=12 terminal=28 entries=%d", report.Entries)
	if purgeChallenge {
		if resultErr != nil || string(value) != "42" {
			t.Fatalf("purge target result=%s err=%v", value, resultErr)
		}
		stopSecond()
		reused := challengeMixedPurge(t, ctx, nodes[survivor], target.typ, target.id, records)
		stopReused := startWorker("mixed-purge-reused", nodes[survivor], true)
		defer stopReused()
		result, err := c.Await(ctx, reused.Type, reused.ID)
		if err != nil || string(result) != "42" {
			t.Fatalf("reused purge result=%s err=%v", result, err)
		}
		verifyMixedPurgeReuse(t, ctx, nodes[survivor], reused, records, priorInvocations+28)
		return
	}
	if startRepairChallenge {
		if resultErr != nil || string(value) != "42" {
			t.Fatalf("original cohort target result=%s err=%v", value, resultErr)
		}
		verifyMixedStartRepairOutcome(t, ctx, nodes[survivor], orphan, startRepaired)
		if !startRepaired {
			t.Fatal("MIXED_MUTATION_ESCAPE category=skipped_start_reconciler retained_invocations=29 original_terminal=28 orphan_journal=absent orphan_terminal=absent")
		}
		t.Log("MIXED_START_REPAIR_REJECTED retained_invocations=29 terminal=29")
		return
	}
	if enqueueChallenge {
		if resultErr != nil || string(value) != "42" || records[len(records)-1].Kind != journal.Completed {
			t.Fatalf("enqueue cohort target result=%s err=%v", value, resultErr)
		}
		if retainedEnqueues > 1 {
			t.Fatalf("MIXED_MUTATION_ESCAPE category=missing_run_message_id acknowledged_calls=64 retained_at_least64=true terminal=28 retained=%d", retainedEnqueues)
		}
		t.Log("MIXED_ENQUEUE_REJECTED acknowledged_calls=64 retained=1 terminal=28")
		return
	}
	if casChallenge {
		if resultErr != nil || string(value) != "42" || records[len(records)-1].Kind != journal.Completed {
			t.Fatalf("CAS cohort target result=%s err=%v", value, resultErr)
		}
		t.Log("MIXED_CAS_REJECTED acknowledged_winners=1 stale_rejections=1 terminal=28")
		return
	}
	if leaseChallenge {
		if resultErr != nil || string(value) != "42" || records[len(records)-1].Kind != journal.Completed {
			t.Fatalf("lease cohort target result=%s err=%v", value, resultErr)
		}
		if leaseEscaped {
			t.Fatalf("MIXED_MUTATION_ESCAPE category=independent_worker_leases admitted=1 rival_epoch=%d terminal=Completed", challengerEpoch)
		}
		t.Log("MIXED_LEASE_REJECTED admitted=0 error=workflow_lease_is_held terminal=28")
		return
	}
	if changedEffects.Load() != 0 || resultErr == nil || !strings.Contains(resultErr.Error(), wf.ErrNonDeterministic.Error()) || records[len(records)-1].Kind != journal.Failed {
		t.Fatalf("MIXED_MUTATION_ESCAPE invariant=I4 effects=%d result=%s error=%v terminal=%s", changedEffects.Load(), value, resultErr, records[len(records)-1].Kind)
	}
	t.Logf("MIXED_GUARD_REJECTED index=1 recorded=held requested=changed effects=0 prefix=%d old_epoch=%d new_epoch=%d", len(prefix), prefix[1].Epoch, records[len(records)-1].Epoch)
}
