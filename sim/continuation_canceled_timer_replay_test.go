package sim

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/wf"
	"js-wf/worker"
)

type continuationCancelJournal struct {
	*JournalTransport
	mode  string
	fired bool
}

func (p *continuationCancelJournal) Publish(ctx context.Context, subject string, data []byte, expected uint64) (uint64, error) {
	var entry journal.Entry
	if err := json.Unmarshal(data, &entry); err != nil {
		return 0, err
	}
	if !p.fired && strings.HasPrefix(p.mode, "cancel_") && entry.Kind == journal.StepCompleted && string(entry.Payload) == `{"cancelled":true}` {
		kind := DropBeforeCommit
		if p.mode == "cancel_ack_lost" {
			kind = LoseAckAfterCommit
		}
		if err := p.QueueFault(Fault{Kind: kind}); err != nil {
			return 0, err
		}
		p.fired = true
	}
	return p.JournalTransport.Publish(ctx, subject, data, expected)
}

func runSeededContinuationCanceledTimer(seed int64, replay *Trace) (trace Trace, runErr error) {
	schedule := NewScheduler(seed)
	if replay != nil {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	if err := schedule.SetWorkload("continuation_canceled_timer"); err != nil {
		return trace, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"clean", "timer_drop", "timer_ack_lost", "cancel_drop", "cancel_ack_lost", "wake_ack_lost", "consumer_changed"})
	if err != nil {
		return trace, err
	}
	durationChoice, err := schedule.Choose([]string{"5000", "7000"})
	if err != nil {
		return trace, err
	}
	durationMS, _ := strconv.Atoi(durationChoice)
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	const typ = "test"
	id := integratedWorkerIDs(1)[0]
	base := time.Unix(1700000000, 0).UTC()
	transport := NewWorkerTransport(schedule, 3*time.Second)
	timers := NewTimerScheduleTransport(schedule, base)
	timers.OnNativeDelivery(func(msg *nats.Msg, timestamp time.Time) {
		transport.Dispatch.PublishRunMessage(msg.Subject, msg.Data, msg.Header, timestamp)
	})
	live := NewJournalTransport(schedule)
	appendPort := &continuationCancelJournal{JournalTransport: live, mode: mode}
	snapshots := &continuationWorkerSnapshots{SnapshotReadTransport: NewSnapshotReadTransport(schedule), mode: "clean"}
	snapshots.BindJournal(live)
	snapshots.BindSignals(transport.SignalTransport)
	store := journal.NewWithSnapshotPort(appendPort, live, snapshots)
	leasing := lease.NewWithKVPort(NewKVTransport(schedule, 30*time.Second))
	outcomes := NewKVTransport(schedule, 0)
	c := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport)
	var initialCalls, stageCalls int
	stageMax := 2
	handlers := map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		initialCalls++
		version, err := wf.Version(c, "prefix-choice", 1, 2)
		if err != nil {
			return nil, err
		}
		timer, err := c.Timer("canceled-before-frame", time.Duration(durationMS)*time.Millisecond)
		if err != nil {
			return nil, err
		}
		if err := timer.Cancel(); err != nil {
			return nil, err
		}
		return nil, wf.Continue(c, "finish_v1", version)
	}}
	stages := map[string]worker.ContinuationHandler{"finish_v1": func(c *wf.Context, _, locals json.RawMessage) (json.RawMessage, error) {
		stageCalls++
		version, err := wf.Version(c, "suffix-choice", 1, stageMax)
		if err != nil {
			return nil, err
		}
		if version != 2 || string(locals) != "2" {
			return nil, fmt.Errorf("version drift locals=%s version=%d", locals, version)
		}
		if _, err := wf.AwaitSignal(c, "gate"); err != nil {
			return nil, err
		}
		return json.RawMessage(`2`), nil
	}}
	ports := worker.ModeledWorkerPorts{Journal: store, Leases: leasing, Outcome: outcomes, Invocation: transport.SignalTransport, Signals: transport.SignalTransport, ResultBlobs: snapshots, Client: c, Timer: timers, TimerNow: func(context.Context) (time.Time, error) {
		return base.Add(time.Duration(schedule.NowMillis()) * time.Millisecond), nil
	}, NativeTimer: true}
	first, err := worker.NewWithPorts("cancel-first", handlers, ports, worker.WithContinuations(typ, stages))
	if err != nil {
		return trace, err
	}
	handle, err := c.Start(ctx, typ, id, []byte(`null`))
	if err != nil {
		return trace, err
	}
	if strings.HasPrefix(mode, "timer_") {
		kind := DropBeforeCommit
		if mode == "timer_ack_lost" {
			kind = LoseAckAfterCommit
		}
		if err := timers.QueueFault(kind); err != nil {
			return trace, err
		}
	}
	firstCtx, stopFirst := context.WithCancel(ctx)
	defer stopFirst()
	transport.Dispatch.StopWhenDrained(stopFirst)
	if err := first.RunPartitionWithTransport(firstCtx, 0, transport.Dispatch); err != nil || transport.Dispatch.Pending() != 0 {
		return trace, fmt.Errorf("first pending=%d err=%v", transport.Dispatch.Pending(), err)
	}
	before, _, err := store.Read(ctx, typ, id)
	if err != nil || len(before) == 0 || before[len(before)-1].Kind != journal.Suspended || string(before[len(before)-1].Payload) != `{"waiting_on":"signal:gate"}` {
		return trace, fmt.Errorf("before=%v err=%v", before, err)
	}
	initialBefore, stageBefore := initialCalls, stageCalls
	view, err := store.ReadCheckpoint(ctx, typ, id, handle.InvSeq)
	if err != nil || view == nil {
		return trace, fmt.Errorf("view=%+v err=%v", view, err)
	}
	runtime := view.Snapshot.Runtime
	_, info, err := wf.NewCheckpointContext(ctx, nil, nil, view.Frame, wf.CheckpointLocation{Type: typ, ID: id, InvSeq: handle.InvSeq, Index: runtime.Index, Epoch: runtime.Epoch, Hash: runtime.SHA256})
	if err != nil || len(info.CancelledTimers) != 1 || info.CancelledTimers[0] != 2 {
		return trace, fmt.Errorf("cancel facts=%+v err=%v", info, err)
	}
	if len(timers.NativeSources()) != 1 {
		return trace, fmt.Errorf("native sources=%d", len(timers.NativeSources()))
	}
	source := timers.NativeSources()[0]
	due, err := time.Parse(time.RFC3339Nano, source.Header.Get(jetstream.ScheduleHeader)[4:])
	if err != nil {
		return trace, err
	}
	remaining := due.Sub(base.Add(time.Duration(schedule.NowMillis()) * time.Millisecond))
	if remaining <= 0 {
		return trace, fmt.Errorf("timer became due before replacement")
	}
	guard := &continuationPromiseGuard{SnapshotWritePort: snapshots}
	ports.Journal = journal.NewWithSnapshotPort(appendPort, live, guard)
	second, err := worker.NewWithPorts("cancel-second", handlers, ports, worker.WithContinuations(typ, stages))
	if err != nil {
		return trace, err
	}
	stageMax = 3
	if mode == "wake_ack_lost" {
		if err := transport.Dispatch.QueueFault(DispatchFault{Operation: "ack", Kind: "lose_ack_after_commit"}); err != nil {
			return trace, err
		}
	}
	if mode == "consumer_changed" {
		if err := transport.Dispatch.QueueFault(DispatchFault{Operation: "consumer", Kind: "leader_changed"}); err != nil {
			return trace, err
		}
	}
	if err := timers.Advance(remaining - time.Millisecond); err != nil || transport.Dispatch.Pending() != 0 {
		return trace, fmt.Errorf("early pending=%d err=%v", transport.Dispatch.Pending(), err)
	}
	if err := timers.Advance(time.Millisecond); err != nil || transport.Dispatch.Pending() != 1 {
		return trace, fmt.Errorf("due pending=%d err=%v", transport.Dispatch.Pending(), err)
	}
	run := func(instance *worker.Worker) error {
		runCtx, stopRun := context.WithCancel(ctx)
		defer stopRun()
		transport.Dispatch.StopWhenDrained(stopRun)
		return instance.RunPartitionWithTransport(runCtx, 0, transport.Dispatch)
	}
	if err := run(second); err != nil || transport.Dispatch.Pending() != 0 {
		return trace, fmt.Errorf("wakeup pending=%d err=%v", transport.Dispatch.Pending(), err)
	}
	after, _, err := store.Read(ctx, typ, id)
	if err != nil {
		return trace, err
	}
	if initialCalls != initialBefore || stageCalls != stageBefore || !reflect.DeepEqual(before, after) {
		return trace, fmt.Errorf("canceled wakeup entered handler: initial=%d/%d stage=%d/%d records=%d/%d", initialCalls, initialBefore, stageCalls, stageBefore, len(after), len(before))
	}
	expectedNoops := uint64(1)
	if mode == "wake_ack_lost" {
		expectedNoops = 0
	}
	if metrics := second.Metrics(); metrics.CancelledTimerNoOps != expectedNoops || metrics.TimersFired != 0 || metrics.TimersScheduled != 0 {
		return trace, fmt.Errorf("metrics=%+v want_noops=%d", metrics, expectedNoops)
	}
	if _, err := c.Signal(ctx, typ, id, "gate", []byte(`true`), "gate-key"); err != nil {
		return trace, err
	}
	if err := run(second); err != nil || transport.Dispatch.Pending() != 0 {
		return trace, fmt.Errorf("gate pending=%d err=%v", transport.Dispatch.Pending(), err)
	}
	records, tail, err := store.Read(ctx, typ, id)
	if err != nil || len(records) == 0 || records[len(records)-1].Kind != journal.Completed {
		return trace, fmt.Errorf("terminal=%v err=%v", records, err)
	}
	var outcome wf.Outcome
	if err := json.Unmarshal(records[len(records)-1].Payload, &outcome); err != nil || outcome.InvSeq != handle.InvSeq || string(outcome.Result) != "2" || outcome.Error != "" {
		return trace, fmt.Errorf("outcome=%+v err=%v", outcome, err)
	}
	if initialCalls != initialBefore || stageCalls != stageBefore+1 || guard.archives != 0 || guard.frames == 0 {
		return trace, fmt.Errorf("prefix/entry drift or archive read: calls=%d/%d reads=%d/%d", initialCalls, stageCalls, guard.archives, guard.frames)
	}
	// Recreate the reachable terminal-journal / absent-outcome boundary. This
	// deliberately omits state in the fixture; it does not model lost KV data.
	stateKey := identity.Key(typ, id)
	saved, err := outcomes.Get(ctx, stateKey)
	if err != nil {
		return trace, err
	}
	if err := outcomes.Delete(ctx, stateKey, saved.Revision); err != nil {
		return trace, err
	}
	repair, err := worker.NewWithPorts("cancel-terminal-repair", handlers, ports, worker.WithContinuations(typ, stages))
	if err != nil {
		return trace, err
	}
	transport.Dispatch.PublishRunMessage(source.Header.Get(jetstream.ScheduleTargetHeader), source.Data, source.Header, base.Add(time.Duration(schedule.NowMillis())*time.Millisecond))
	if err := run(repair); err != nil || transport.Dispatch.Pending() != 0 {
		return trace, fmt.Errorf("terminal repair pending=%d err=%v", transport.Dispatch.Pending(), err)
	}
	restored, err := outcomes.Get(ctx, stateKey)
	if err != nil || !bytes.Equal(restored.Value, saved.Value) || initialCalls != initialBefore || stageCalls != stageBefore+1 || repair.Metrics().CancelledTimerNoOps != 1 {
		return trace, fmt.Errorf("terminal repair lost or dispatched handler: err=%v calls=%d/%d noops=%d", err, initialCalls, stageCalls, repair.Metrics().CancelledTimerNoOps)
	}
	unchanged, _, err := store.Read(ctx, typ, id)
	if err != nil || !reflect.DeepEqual(unchanged, records) {
		return trace, fmt.Errorf("terminal repair changed journal: %v", err)
	}
	raw, err := retainedModelSnapshot(transport.StartTransport, live, outcomes)
	if err != nil {
		return trace, err
	}
	raw.Journals[identity.JournalSubject(typ, id)] = records
	report, err := integrity.CheckSnapshot(raw)
	if err != nil || report.Invocations != 1 || report.Terminal != 1 {
		return trace, fmt.Errorf("audit=%+v err=%v", report, err)
	}
	if strings.HasPrefix(mode, "cancel_") && !appendPort.fired {
		return trace, fmt.Errorf("cancel fault not consumed")
	}
	remainingTimerFaults := len(timers.faults)
	transport.Dispatch.mu.Lock()
	remainingDispatchFaults := len(transport.Dispatch.faults)
	transport.Dispatch.mu.Unlock()
	if remainingTimerFaults != 0 || remainingDispatchFaults != 0 {
		return trace, fmt.Errorf("unused faults timer=%d dispatch=%d", remainingTimerFaults, remainingDispatchFaults)
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_continuation_canceled_timer", Subject: identity.JournalSubject(typ, id), Sequence: tail, Outcome: mode, AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededContinuationCanceledTimerReplay(t *testing.T) {
	if os.Getenv("SIM_CONTINUATION_CANCELED_TIMER_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededContinuationCanceledTimer(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_CONTINUATION_CANCELED_TIMER_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	observed := map[string]bool{}
	combinations := map[string]bool{}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		generated, err := runSeededContinuationCanceledTimer(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-continuation-canceled-timer-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-continuation-canceled-timer.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		observed[generated.Decisions[0].Chosen] = true
		combinations[generated.Decisions[0].Chosen+":"+generated.Decisions[1].Chosen] = true
		if seed <= 10 {
			replayed, err := runSeededContinuationCanceledTimer(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d continuation canceled timer replay: %v", seed, err)
			}
		}
	}
	if len(observed) != 7 {
		t.Fatalf("covered %d/7 continuation canceled timer modes", len(observed))
	}
	if len(combinations) != 14 {
		t.Fatalf("covered %d/14 duration/fault combinations", len(combinations))
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("continuation-canceled-timer-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededContinuationCanceledTimerReplay$")
		cmd.Env = append(os.Environ(), "SIM_CONTINUATION_CANCELED_TIMER_HELPER=1", "SIM_CONTINUATION_CANCELED_TIMER_OUT="+files[i], "FAULT_SEED=42")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child %d: %v: %s", i, err, output)
		}
	}
	first, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(files[1])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("continuation canceled timer trace changed across processes")
	}
}
