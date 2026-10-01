package sim

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go/jetstream"
)

// A missing response honors the context sent by production worker code. The
// model advances the bounded request cost without wall-clock sleeping; it does
// not invent a NATS server mechanism or replay the real failing interleaving.
type snapshotBudgetPort struct {
	*SnapshotReadTransport
	schedule    *Scheduler
	stalled     bool
	budgetError error
}

func (p *snapshotBudgetPort) GetManifestRevision(ctx context.Context, key string) (journal.SnapshotManifestValue, error) {
	if !p.stalled {
		p.stalled = true
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > 15*time.Second {
			p.budgetError = fmt.Errorf("automatic snapshot request inherits delivery lifetime instead of a 15s budget")
			return journal.SnapshotManifestValue{}, p.budgetError
		}
		if err := p.schedule.AdvanceMillis(15000); err != nil {
			return journal.SnapshotManifestValue{}, err
		}
		p.schedule.RecordTransport(TransportEvent{Operation: "snapshot_missing_reply", Subject: key, Outcome: "deadline_15s"})
		return journal.SnapshotManifestValue{}, context.DeadlineExceeded
	}
	return p.SnapshotReadTransport.GetManifestRevision(ctx, key)
}

func runSeededWorkerSnapshotBudget(seed int64, replay *Trace) (trace Trace, runErr error) {
	schedule := NewScheduler(seed)
	if replay != nil {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	if err := schedule.SetWorkload("worker_snapshot_response_budget"); err != nil {
		return trace, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"completed", "failed", "suspended"})
	if err != nil {
		return trace, err
	}
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Minute)
	defer stop()
	const typ = "test"
	id := integratedWorkerIDs(1)[0]
	transport := NewWorkerTransport(schedule, 3*time.Second)
	live := NewJournalTransport(schedule)
	snapshots := NewSnapshotReadTransport(schedule)
	snapshots.BindJournal(live)
	port := &snapshotBudgetPort{SnapshotReadTransport: snapshots, schedule: schedule}
	store := journal.NewWithSnapshotPort(live, live, port)
	kv := NewKVTransport(schedule, 30*time.Second)
	leasing := lease.NewWithKVPort(kv)
	outcomes := NewKVTransport(schedule, 0)
	c := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport)
	effects, calls := 0, 0
	snapshotsFinished := 0
	w, err := worker.NewWithPorts("snapshot-budget", map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		calls++
		if mode == "failed" {
			return nil, errors.New("expected failure")
		}
		value, err := wf.Run(c, "once", 42, func(context.Context) (int, error) { effects++; return 42, nil })
		if err != nil {
			return nil, err
		}
		if mode == "suspended" {
			if _, err := wf.AwaitSignal(c, "continue"); err != nil {
				return nil, err
			}
		}
		return json.Marshal(value)
	}}, worker.ModeledWorkerPorts{Journal: store, Leases: leasing, Outcome: outcomes, Invocation: transport.SignalTransport, Signals: transport.SignalTransport, Client: c,
		OperationObserver: func(e worker.OperationEvent) {
			if e.Operation == "journal_snapshot" {
				snapshotsFinished++
			}
		}, OperationNow: func() time.Time { return time.UnixMilli(schedule.NowMillis()) }})
	if err != nil {
		return trace, err
	}
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		return trace, err
	}
	attempt, stopAttempt := context.WithCancel(ctx)
	transport.Dispatch.StopAfterNextNak(stopAttempt)
	if err := w.RunPartitionWithTransport(attempt, 0, transport.Dispatch); err != nil {
		return trace, err
	}
	if port.budgetError != nil {
		return trace, port.budgetError
	}
	if transport.Dispatch.Pending() != 1 || schedule.NowMillis() != 15000 || snapshotsFinished != 1 {
		return trace, fmt.Errorf("missing-reply recovery pending=%d time=%d observed=%d", transport.Dispatch.Pending(), schedule.NowMillis(), snapshotsFinished)
	}
	if _, err := kv.Get(ctx, identity.Key(typ, id)); !errors.Is(err, jetstream.ErrKeyNotFound) {
		return trace, fmt.Errorf("snapshot timeout retained lease: %v", err)
	}
	prefix, tail, err := store.Read(ctx, typ, id)
	if err != nil {
		return trace, err
	}
	before, err := outcomes.Get(ctx, identity.Key(typ, id))
	if mode == "suspended" {
		if !errors.Is(err, jetstream.ErrKeyNotFound) || prefix[len(prefix)-1].Kind != journal.Suspended {
			return trace, fmt.Errorf("suspended timeout state: %v", err)
		}
		if _, err := c.Signal(ctx, typ, id, "continue", []byte(`true`), "resume"); err != nil {
			return trace, err
		}
	} else if err != nil {
		return trace, err
	}
	if err := schedule.AdvanceMillis(1000); err != nil {
		return trace, err
	}
	retry, stopRetry := context.WithCancel(ctx)
	transport.Dispatch.StopWhenDrained(stopRetry)
	if err := w.RunPartitionWithTransport(retry, 0, transport.Dispatch); err != nil || transport.Dispatch.Pending() != 0 {
		return trace, fmt.Errorf("retry drain pending=%d err=%v", transport.Dispatch.Pending(), err)
	}
	records, newTail, err := store.Read(ctx, typ, id)
	if err != nil {
		return trace, err
	}
	after, err := outcomes.Get(ctx, identity.Key(typ, id))
	if err != nil {
		return trace, err
	}
	if mode != "suspended" && (newTail != tail || !reflect.DeepEqual(prefix, records) || !bytes.Equal(before.Value, after.Value)) {
		return trace, fmt.Errorf("terminal changed on snapshot retry")
	}
	wantCalls, wantEffects := 1, 1
	if mode == "suspended" {
		wantCalls = 2
	}
	if mode == "failed" {
		wantEffects = 0
	}
	if calls != wantCalls || effects != wantEffects || snapshotsFinished != 2 {
		return trace, fmt.Errorf("snapshot replay calls=%d effects=%d snapshots=%d", calls, effects, snapshotsFinished)
	}
	if _, err := kv.Get(ctx, identity.Key(typ, id)); !errors.Is(err, jetstream.ErrKeyNotFound) {
		return trace, fmt.Errorf("retry retained lease: %v", err)
	}
	if _, err := integrity.CheckSnapshot(integrity.Snapshot{Invocations: []string{identity.InvocationSubject(typ, id)}, Journals: map[string][]journal.Record{identity.JournalSubject(typ, id): records}, TerminalState: map[string][]byte{identity.Key(typ, id): after.Value}}); err != nil {
		return trace, err
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_snapshot_response_budget", Subject: identity.Key(typ, id), Outcome: mode})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededWorkerSnapshotBudgetReplay(t *testing.T) {
	if os.Getenv("SIM_SNAPSHOT_BUDGET_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededWorkerSnapshotBudget(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_SNAPSHOT_BUDGET_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	seen := map[string]bool{}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		trace, err := runSeededWorkerSnapshotBudget(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "snapshot-budget-failure.json")
			}
			_ = trace.Save(path)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		seen[trace.Decisions[0].Chosen] = true
		if seed <= 10 {
			other, err := runSeededWorkerSnapshotBudget(seed, &trace)
			if err != nil || !reflect.DeepEqual(trace, other) {
				t.Fatalf("seed %d replay: %v", seed, err)
			}
		}
	}
	if len(seen) != 3 {
		t.Fatalf("modes=%d", len(seen))
	}
	var paths [2]string
	for i := range paths {
		paths[i] = filepath.Join(t.TempDir(), fmt.Sprintf("snapshot-budget-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededWorkerSnapshotBudgetReplay$")
		cmd.Env = append(os.Environ(), "SIM_SNAPSHOT_BUDGET_HELPER=1", "SIM_SNAPSHOT_BUDGET_OUT="+paths[i], "FAULT_SEED=42")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child: %v: %s", err, output)
		}
	}
	first, err := os.ReadFile(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(paths[1])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("snapshot budget trace changed across processes")
	}
}
