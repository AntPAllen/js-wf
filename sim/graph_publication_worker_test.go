package sim

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/wf"
	"js-wf/worker"
)

var graphWorkerModes = []string{"ordinary", "put_drop", "put_lost", "append_lost", "readback_lost", "collector_wins"}

func runGraphWorker(seed int64, replay *Trace) (trace Trace, runErr error) {
	return runGraphWorkerWithInterleaving(seed, replay, "")
}

func runGraphWorkerWithInterleaving(seed int64, replay *Trace, interleaving string) (trace Trace, runErr error) {
	schedule := NewScheduler(seed)
	if replay != nil {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	if err := schedule.SetWorkload("graph_worker_execution"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose(graphWorkerModes)
	if err != nil {
		return trace, err
	}
	if interleaving != "" {
		mode = interleaving
	}
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	m := NewGraphPublicationTransport(schedule)
	now := func() time.Time { return time.UnixMilli(schedule.NowMillis()).UTC() }
	graph, err := journal.NewGraphStore(journal.GraphConfig{Protocol: m.Protocol(), Now: now, PinTTL: 30 * time.Second, IntentTTL: time.Second})
	if err != nil {
		return trace, err
	}
	transport := NewWorkerTransport(schedule, 3*time.Second)
	legacy := NewJournalTransport(schedule)
	store := journal.NewWithPorts(legacy, legacy)
	leasing := lease.NewWithKVPort(NewKVTransport(schedule, 30*time.Second))
	outcomes := NewKVTransport(schedule, 0)
	c := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport)
	const typ = "test"
	id := integratedWorkerIDs(1)[0]
	effects := 0
	fired := false
	handler := func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
		if !bytes.Equal(input, []byte(`7`)) {
			return nil, fmt.Errorf("graph input differs")
		}
		value, err := wf.Run(c, "effect", 7, func(context.Context) (int, error) { effects++; return 42, nil })
		if err != nil {
			return nil, err
		}
		if value != 42 {
			return nil, fmt.Errorf("graph result differs")
		}
		signal, err := wf.AwaitSignal(c, "resume")
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(signal, []byte(`true`)) {
			return nil, fmt.Errorf("graph signal differs")
		}
		return json.RawMessage(`42`), nil
	}
	build := func(name string) (*worker.Worker, error) {
		return worker.NewWithPorts(name, map[string]worker.Handler{typ: handler}, worker.ModeledWorkerPorts{Journal: store, Leases: leasing, Outcome: outcomes, Invocation: transport.SignalTransport, Signals: transport.SignalTransport, Client: c, HeartbeatTicks: make(chan time.Time), OperationNow: now, OperationObserver: func(e worker.OperationEvent) {
			if fired || mode == "ordinary" || e.Operation != "lease_renew_append" || e.JournalKind != journal.StepCompleted || e.Error != "" {
				return
			}
			fired = true
			switch mode {
			case "put_drop":
				runErr = m.QueueFault("put", DropBeforeCommit)
			case "put_lost":
				runErr = m.QueueFault("put", LoseAckAfterCommit)
			case "append_lost", "readback_lost":
				runErr = m.QueueFault("cas_root", LoseAckAfterCommit)
				if mode == "readback_lost" {
					m.PauseBefore("cas_root", func() error { return m.QueueFault("read_root", DropBeforeCommit) })
				}
			case "collector_wins":
				m.PauseBefore("cas_root", func() error {
					if e := schedule.AdvanceMillis(1000); e != nil {
						return e
					}
					_, e := m.Protocol().SweepWithReaders(ctx, now())
					return e
				})
			case "reader_pin":
				m.PauseBefore("cas_root", func() error {
					view, e := graph.OpenExisting(ctx, typ, id, 1)
					if e != nil {
						return e
					}
					if view == nil {
						return fmt.Errorf("missing interleaved reader")
					}
					return view.Close(ctx)
				})
			case "unknown_completion":
				runErr = m.QueueFault("cas_root", DropBeforeCommit)
			}
		}}, worker.WithGraphJournal(graph))
	}
	first, err := build("graph-first")
	if err != nil {
		return trace, err
	}
	handle, err := c.Start(ctx, typ, id, []byte(`7`))
	if err != nil {
		return trace, err
	}
	run := func(w *worker.Worker) error {
		runCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		transport.Dispatch.StopWhenDrained(cancel)
		return w.RunPartitionWithTransport(runCtx, 0, transport.Dispatch)
	}
	if err = run(first); err != nil {
		return trace, err
	}
	if runErr != nil {
		return trace, runErr
	}
	records, _, err := graph.Read(ctx, typ, id, handle.InvSeq)
	if err != nil || len(records) == 0 || records[len(records)-1].Kind != journal.Suspended {
		return trace, fmt.Errorf("graph worker did not suspend: %v", err)
	}
	if _, err = c.Signal(ctx, typ, id, "resume", []byte(`true`), "resume-key"); err != nil {
		return trace, err
	}
	second, err := build("graph-second")
	if err != nil {
		return trace, err
	}
	if err = run(second); err != nil {
		return trace, err
	}
	records, tail, err := graph.Read(ctx, typ, id, handle.InvSeq)
	if err != nil || len(records) == 0 || records[len(records)-1].Kind != journal.Completed {
		return trace, fmt.Errorf("graph worker did not complete: %v", err)
	}
	state, err := outcomes.Get(ctx, identity.Key(typ, id))
	if err != nil || !bytes.Equal(state.Value, records[len(records)-1].Payload) {
		return trace, fmt.Errorf("terminal graph and state differ")
	}
	wantEffects := 1
	if mode == "put_drop" || mode == "put_lost" || mode == "unknown_completion" {
		wantEffects = 2
	}
	if effects != wantEffects {
		return trace, fmt.Errorf("effects=%d expected=%d", effects, wantEffects)
	}
	if mode != "ordinary" && !fired {
		return trace, fmt.Errorf("fault not exercised")
	}
	legacyRecords, _, err := store.Read(ctx, typ, id)
	if err != nil || len(legacyRecords) != 0 {
		return trace, fmt.Errorf("graph execution wrote legacy journal")
	}
	if err = outcomes.Delete(ctx, identity.Key(typ, id), state.Revision); err != nil {
		return trace, err
	}
	transport.PurgeInvocation(identity.InvocationSubject(typ, id))
	if err = graph.Retire(ctx, typ, id, handle.InvSeq, tail); err != nil {
		return trace, err
	}
	if err = schedule.AdvanceMillis(60000); err != nil {
		return trace, err
	}
	if _, err = m.Protocol().SweepWithReaders(ctx, now()); err != nil {
		return trace, err
	}
	objects, err := m.Objects(ctx)
	if err != nil || len(objects) != 0 {
		return trace, fmt.Errorf("graph worker objects not drained: %v", err)
	}
	if err = m.CheckReferences(); err != nil {
		return trace, err
	}
	if err = transport.Dispatch.CheckDrained(); err != nil {
		return trace, err
	}
	if ctx.Err() != nil {
		return trace, ctx.Err()
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_graph_worker_execution", Outcome: mode})
	if err = schedule.Finish(); err != nil {
		return trace, err
	}
	return trace, nil
}

func TestGraphWorkerCompletionAppendReaderInterleaving(t *testing.T) {
	for seed := int64(1); seed <= 32; seed++ {
		for _, mode := range []string{"reader_pin", "unknown_completion"} {
			t.Run(fmt.Sprintf("seed%d/%s", seed, mode), func(t *testing.T) {
				generated, err := runGraphWorkerWithInterleaving(seed, nil, mode)
				if err != nil {
					t.Fatal(err)
				}
				replayed, err := runGraphWorkerWithInterleaving(seed, &generated, mode)
				if err != nil || !reflect.DeepEqual(generated, replayed) {
					t.Fatal("completion retry interleaving did not replay", err)
				}
			})
		}
	}
}

func TestSeededGraphWorkerReplay(t *testing.T) {
	observed := map[string]int{}
	fail := func(seed int64, trace Trace, cause error) {
		path := os.Getenv("FAULT_TRACE_OUT")
		if path == "" {
			dir, err := os.MkdirTemp("", "js-wf-graph-worker-failure-")
			if err != nil {
				t.Fatal(err)
			}
			path = filepath.Join(dir, "trace.json")
		}
		if err := trace.Save(path); err != nil {
			t.Fatal(err)
		}
		t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, cause)
	}
	for seed := range seededSchedules(t) {
		generated, err := runGraphWorker(seed, nil)
		if err != nil {
			fail(seed, generated, err)
		}
		mode := generated.Decisions[0].Chosen
		observed[mode]++
		replayed, err := runGraphWorker(seed, &generated)
		if err != nil || !reflect.DeepEqual(generated, replayed) {
			fail(seed, generated, fmt.Errorf("graph worker replay differs: %v", err))
		}
		if dir := os.Getenv("SIM_GRAPH_WORKER_ROOT"); dir != "" && observed[mode] == 1 {
			if err = generated.Save(filepath.Join(dir, mode+".json")); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(observed) != len(graphWorkerModes) {
		t.Fatalf("worker coverage=%v", observed)
	}
	t.Logf("graph worker: modes=%v; exact replay, suspension/replacement, atomic payloads, one recorded outcome and complete drain", observed)
}
