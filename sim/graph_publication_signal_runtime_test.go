package sim

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/internal/graphpublication"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/reconcile"
	"js-wf/wf"
	"js-wf/worker"
)

var graphSignalRuntimeModes = []string{"healthy", "reserved", "source_committed", "bound_no_enqueue", "bound_source_purged", "source_drop", "source_lost_ack", "queue_drop", "queue_lost_readback", "enqueue_drop", "enqueue_lost_ack", "catalog_drop", "lifecycle_unknown", "dry_catalog", "batch_restart", "consumption_lost_ack", "consumption_unknown", "prepared_append_repair"}

type graphSignalRuntimeCut struct {
	*GraphPublicationTransport
	mode  string
	fired bool
}

func (p *graphSignalRuntimeCut) CASRoot(ctx context.Context, key string, head uint64, root graphpublication.Root) (graphpublication.Root, error) {
	if !p.fired && (p.mode == "queue_drop" || p.mode == "queue_lost_readback") {
		for _, stream := range root.Streams {
			if stream.Name == "signal-queue" && stream.Graph.Count == 1 {
				p.fired = true
				fault := DropBeforeCommit
				if p.mode == "queue_lost_readback" {
					fault = LoseAckAfterCommit
					p.PauseBefore("cas_root", func() error { return p.QueueFault("read_root", DropBeforeCommit) })
				}
				if e := p.QueueFault("cas_root", fault); e != nil {
					return root, e
				}
			}
		}
	}
	return p.GraphPublicationTransport.CASRoot(ctx, key, head, root)
}

// One scheduler records client, canonical authority, repair, worker and GC
// operations. Reopened adapters retain no caller input or process-local cursor.
func runGraphSignalRuntime(seed int64, replay *Trace) (Trace, error) {
	return runGraphSignalRuntimeSchedule(seed, replay, false)
}

func runGraphSignalRuntimeCombined(seed int64, replay *Trace) (Trace, error) {
	return runGraphSignalRuntimeSchedule(seed, replay, true)
}

func runGraphSignalRuntimeSchedule(seed int64, replay *Trace, combined bool) (trace Trace, runErr error) {
	schedule := NewScheduler(seed)
	if replay != nil {
		var e error
		schedule, e = ReplayScheduler(*replay)
		if e != nil {
			return trace, e
		}
	}
	workload := "graph_canonical_signal_runtime"
	if combined {
		workload = "graph_canonical_signal_runtime_combined"
	}
	if e := schedule.SetWorkload(workload); e != nil {
		return trace, e
	}
	defer func() { trace = schedule.Trace() }()
	modes := graphSignalRuntimeModes
	if combined {
		modes = []string{"healthy", "source_drop", "source_lost_ack", "queue_drop", "queue_lost_readback", "reserved", "batch_restart"}
	}
	mode, err := schedule.Choose(modes)
	if err != nil {
		return trace, err
	}
	discoveryMode, workerMode, enqueueMode := mode, mode, "healthy"
	if combined {
		for _, dimension := range []struct {
			out     *string
			options []string
		}{
			{&discoveryMode, []string{"healthy", "catalog_drop", "lifecycle_unknown", "dry_catalog"}},
			{&enqueueMode, []string{"healthy", "enqueue_drop", "enqueue_lost_ack"}},
			{&workerMode, []string{"healthy", "consumption_lost_ack", "consumption_unknown", "prepared_append_repair"}},
		} {
			*dimension.out, err = schedule.Choose(dimension.options)
			if err != nil {
				return trace, err
			}
		}
	}
	// Wall-clock CPU watchdog for one generated/replayed model schedule.
	// Race instrumentation and concurrent qualification must not redefine the
	// virtual transport/recovery deadlines asserted by the workload.
	ctx, stop := context.WithTimeout(context.Background(), time.Minute)
	defer stop()
	model := NewGraphPublicationTransport(schedule)
	cut := &graphSignalRuntimeCut{GraphPublicationTransport: model, mode: mode}
	protocol := model.Protocol()
	protocol.Port = cut
	now := func() time.Time { return time.UnixMilli(schedule.NowMillis()).UTC() }
	newStore := func() (*journal.GraphStore, error) {
		return journal.NewGraphStore(journal.GraphConfig{Protocol: protocol, Now: now, PinTTL: 30 * time.Second, IntentTTL: time.Second, CanonicalStarts: true, CanonicalSignals: true})
	}
	graph, err := newStore()
	if err != nil {
		return trace, err
	}
	transport := NewWorkerTransport(schedule, 3*time.Second)
	newClient := func() (*client.Client, error) {
		return client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport).WithGraphJournal(graph)
	}
	c, err := newClient()
	if err != nil {
		return trace, err
	}
	const typ = "test"
	id := integratedWorkerIDs(1)[0]
	h, err := c.Start(ctx, typ, id, []byte(`7`))
	if err != nil {
		return trace, err
	}
	requests := []journal.GraphSignalRequest{{Type: typ, ID: id, Invocation: h.InvSeq, Name: "first", Key: "first"}}
	if mode == "batch_restart" {
		for i := 1; i < 19; i++ {
			r := requests[0]
			r.Key = fmt.Sprint(i)
			requests = append(requests, r)
		}
	}
	if enqueueMode != "healthy" {
		kind := "drop_before_commit"
		if enqueueMode == "enqueue_lost_ack" {
			kind = "lose_ack_after_commit"
		}
		if err = transport.QueueFault(StartFault{Operation: "enqueue_run", Kind: kind}); err != nil {
			return trace, err
		}
	}
	fixture := mode == "reserved" || mode == "source_committed" || mode == "bound_no_enqueue" || mode == "bound_source_purged" || mode == "batch_restart" || mode == "dry_catalog" || mode == "catalog_drop" || mode == "lifecycle_unknown"
	if fixture {
		for _, r := range requests {
			input, e := graph.ReserveSignal(ctx, r, []byte(`true`), false)
			if e != nil {
				return trace, e
			}
			if mode == "source_committed" || mode == "bound_no_enqueue" || mode == "bound_source_purged" {
				msg := &nats.Msg{Subject: "wf.sig." + typ + "." + id + ".first", Data: input.PointerBytes(), Header: nats.Header{}}
				msg.Header.Set(journal.GraphSignalTokenHeader, input.Token)
				msg.Header.Set("Wf-Input-SHA256", input.InputSHA256)
				msg.Header.Set("Wf-Inv-Seq", strconv.FormatUint(h.InvSeq, 10))
				seq := transport.CommitSignal(msg)
				if mode != "source_committed" {
					if progress, e := graph.BindNextSignal(ctx, typ, id, h.InvSeq, seq, transport.SignalTransport); e != nil || !progress {
						return trace, fmt.Errorf("bind fixture: %v", e)
					}
					if mode == "bound_source_purged" {
						transport.PurgeSignal(seq)
					}
				}
			}
		}
	} else {
		switch mode {
		case "source_drop":
			err = transport.QueueSignalFault(SignalDropBeforeCommit)
		case "source_lost_ack":
			err = transport.QueueSignalFault(SignalLoseAckAfterCommit)
		case "enqueue_drop":
			err = transport.QueueFault(StartFault{Operation: "enqueue_run", Kind: "drop_before_commit"})
		case "enqueue_lost_ack":
			err = transport.QueueFault(StartFault{Operation: "enqueue_run", Kind: "lose_ack_after_commit"})
		}
		if err != nil {
			return trace, err
		}
		_, err = c.Signal(ctx, typ, id, "first", []byte(`true`), "first")
		if combined {
			if err != nil && !errors.Is(err, client.ErrSignalUnknown) && !errors.Is(err, client.ErrEnqueueUnknown) {
				return trace, err
			}
		} else {
			switch mode {
			case "source_drop", "queue_drop":
				if !errors.Is(err, client.ErrSignalUnknown) {
					return trace, fmt.Errorf("expected uncertain publication: %v", err)
				}
			case "enqueue_drop", "enqueue_lost_ack":
				if !errors.Is(err, client.ErrEnqueueUnknown) {
					return trace, fmt.Errorf("expected uncertain enqueue: %v", err)
				}
			default:
				if err != nil {
					return trace, err
				}
			}
		}
	}
	// Reopen and discover without passing requests to the scanner.
	graph, err = newStore()
	if err != nil {
		return trace, err
	}
	c, err = newClient()
	if err != nil {
		return trace, err
	}
	scan, err := reconcile.NewCanonicalSignalScanWithPort(graph, c)
	if err != nil {
		return trace, err
	}
	if discoveryMode == "dry_catalog" {
		beforeRuns := len(transport.Runs())
		dry, e := scan.Scan(ctx, 1, 1, true)
		if e != nil || len(dry.Candidates) < 1 || (!combined && len(dry.Candidates) != 1) || len(dry.Candidates) > reconcile.CanonicalSignalRepairBatch || dry.Reenqueued != 0 || len(transport.Runs()) != beforeRuns {
			return trace, fmt.Errorf("dry repair mutated: %+v %v", dry, e)
		}
	}
	if discoveryMode == "catalog_drop" || discoveryMode == "lifecycle_unknown" {
		op := "next_root"
		if discoveryMode == "lifecycle_unknown" {
			op = "read_root"
		}
		if err = model.QueueFault(op, DropBeforeCommit); err != nil {
			return trace, err
		}
		result, e := scan.Scan(ctx, 1, 1, false)
		if !errors.Is(e, journal.ErrUnknown) || result.RetrySequence != 0 || result.NextSequence != 1 {
			return trace, fmt.Errorf("uncertain discovery advanced: %+v %v", result, e)
		}
	}
	repairPasses := 3
	if combined {
		repairPasses = 8
	}
	for pass := 0; pass < repairPasses; pass++ {
		result, e := scan.Scan(ctx, 1, 1, false)
		if e != nil {
			if combined && errors.Is(e, journal.ErrUnknown) && result.NextSequence == 1 && result.RetrySequence == 0 {
				// Reopen after every unknown outcome; no local recovery cursor
				// or caller payload survives between attempts.
				graph, err = newStore()
				if err != nil {
					return trace, err
				}
				c, err = newClient()
				if err != nil {
					return trace, err
				}
				scan, err = reconcile.NewCanonicalSignalScanWithPort(graph, c)
				if err != nil {
					return trace, err
				}
				continue
			}
			return trace, fmt.Errorf("repair %d: %+v %w", pass, result, e)
		}
		state, e := graph.InspectStart(ctx, typ, id)
		if e != nil {
			return trace, e
		}
		if state.SignalBindings == uint64(len(requests)) {
			break
		}
		if !combined && (mode != "batch_restart" || state.SignalRepair != uint64((pass+1)*reconcile.CanonicalSignalRepairBatch)) {
			return trace, fmt.Errorf("repair position: %+v", state)
		}
		graph, err = newStore()
		if err != nil {
			return trace, err
		}
		c, err = newClient()
		if err != nil {
			return trace, err
		}
		scan, err = reconcile.NewCanonicalSignalScanWithPort(graph, c)
		if err != nil {
			return trace, err
		}
	}
	for _, r := range requests {
		b, body, found, e := graph.ReadSignalBinding(ctx, r)
		if e != nil || !found || !bytes.Equal(body, []byte(`true`)) {
			return trace, fmt.Errorf("missing recovered input: %v", e)
		}
		transport.PurgeSignal(b.Sequence)
	}
	if (mode == "queue_drop" || mode == "queue_lost_readback") && !cut.fired {
		return trace, fmt.Errorf("queue cut not reached")
	}
	legacy := NewJournalTransport(schedule)
	legacyStore := journal.NewWithPorts(legacy, legacy)
	leasing := lease.NewWithKVPort(NewKVTransport(schedule, 30*time.Second))
	outcomes := NewKVTransport(schedule, 0)
	effects := 0
	workerCut := false
	preparedCuts := 0
	build := func(name string) (*worker.Worker, error) {
		return worker.NewWithPorts(name, map[string]worker.Handler{typ: func(w *wf.Context, input json.RawMessage) (json.RawMessage, error) {
			if string(input) != "7" {
				return nil, fmt.Errorf("Start input changed")
			}
			if _, e := wf.Run(w, "once", 0, func(context.Context) (int, error) { effects++; return 42, nil }); e != nil {
				return nil, e
			}
			for _, name := range []string{"first", "second"} {
				body, e := wf.AwaitSignal(w, name)
				if e != nil {
					return nil, e
				}
				if string(body) != "true" {
					return nil, fmt.Errorf("Signal body changed")
				}
			}
			return json.RawMessage(`42`), nil
		}}, worker.ModeledWorkerPorts{Journal: legacyStore, Leases: leasing, Outcome: outcomes, Invocation: transport.SignalTransport, Signals: transport.SignalTransport, Client: c, HeartbeatTicks: make(chan time.Time), OperationNow: now, OperationObserver: func(e worker.OperationEvent) {
			if workerCut || e.Operation != "lease_renew_append" || e.JournalKind != journal.SignalConsumed || e.Error != "" {
				return
			}
			if workerMode != "consumption_lost_ack" && workerMode != "consumption_unknown" && workerMode != "prepared_append_repair" {
				return
			}
			workerCut = true
			if workerMode == "prepared_append_repair" {
				model.PauseBefore("cas_root", func() error {
					preparedCuts++
					result, e := scan.Scan(ctx, 1, 1, false)
					if e == nil && result.Reenqueued != 1 {
						return fmt.Errorf("prepared repair missing")
					}
					return e
				})
			} else {
				runErr = model.QueueFault("cas_root", LoseAckAfterCommit)
				if workerMode == "consumption_unknown" {
					model.PauseBefore("cas_root", func() error { return model.QueueFault("read_root", DropBeforeCommit) })
				}
			}
		}}, worker.WithGraphJournal(graph))
	}
	run := func(name string) error {
		w, e := build(name)
		if e != nil {
			return e
		}
		runCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		transport.Dispatch.StopWhenDrained(cancel)
		return w.RunPartitionWithTransport(runCtx, 0, transport.Dispatch)
	}
	if err = run("canonical-signal-first"); err != nil {
		return trace, err
	}
	if runErr != nil {
		return trace, runErr
	}
	records, _, err := graph.Read(ctx, typ, id, h.InvSeq)
	if err != nil || len(records) == 0 || records[len(records)-1].Kind != journal.Suspended {
		return trace, fmt.Errorf("worker failed to suspend: %v", err)
	}
	if effects != 1 {
		return trace, fmt.Errorf("first effect count %d", effects)
	}
	if _, err = c.Signal(ctx, typ, id, "second", []byte(`true`), "second"); err != nil {
		return trace, err
	}
	graph, err = newStore()
	if err != nil {
		return trace, err
	}
	c, err = newClient()
	if err != nil {
		return trace, err
	}
	if err = run("canonical-signal-reopened"); err != nil {
		return trace, err
	}
	if err = c.Enqueue(ctx, typ, id, "completed-replay"); err != nil {
		return trace, err
	}
	if err = run("canonical-signal-completed"); err != nil {
		return trace, err
	}
	records, tail, err := graph.Read(ctx, typ, id, h.InvSeq)
	if err != nil || len(records) == 0 || records[len(records)-1].Kind != journal.Completed || effects != 1 {
		return trace, fmt.Errorf("completion effects=%d: %v", effects, err)
	}
	consumed := 0
	for _, record := range records {
		if record.Kind == journal.SignalConsumed {
			consumed++
		}
	}
	status, err := graph.InspectStart(ctx, typ, id)
	if err != nil || consumed != len(requests)+1 || status.SignalConsumed != uint64(consumed) {
		return trace, fmt.Errorf("consumption prefix=%d: %v", consumed, err)
	}
	if (workerMode == "consumption_lost_ack" || workerMode == "consumption_unknown" || workerMode == "prepared_append_repair") && !workerCut {
		return trace, fmt.Errorf("worker cut not reached")
	}
	if workerMode == "prepared_append_repair" {
		if preparedCuts != 1 {
			return trace, fmt.Errorf("prepared repair cuts=%d", preparedCuts)
		}
		for _, event := range schedule.Trace().Transport {
			if event.Operation == "graph_publication_cas_root" && event.Outcome == "conflict" {
				return trace, fmt.Errorf("metadata repair invalidated a prepared append")
			}
		}
	}
	if old, _, e := legacyStore.Read(ctx, typ, id); e != nil || len(old) != 0 {
		return trace, fmt.Errorf("legacy journal written: %v", e)
	}
	state, err := outcomes.Get(ctx, identity.Key(typ, id))
	if err != nil || !bytes.Equal(state.Value, records[len(records)-1].Payload) {
		return trace, fmt.Errorf("terminal projection differs: %v", err)
	}
	if err = outcomes.Delete(ctx, identity.Key(typ, id), state.Revision); err != nil {
		return trace, err
	}
	if err = model.CheckReferences(); err != nil {
		return trace, err
	}
	if err = graph.Retire(ctx, typ, id, h.InvSeq, tail); err != nil {
		return trace, err
	}
	transport.PurgeInvocation(identity.InvocationSubject(typ, id))
	if err = schedule.AdvanceMillis(60000); err != nil {
		return trace, err
	}
	if _, err = protocol.SweepWithReaders(ctx, now()); err != nil {
		return trace, err
	}
	objects, err := model.Objects(ctx)
	if err != nil || len(objects) != 0 {
		return trace, fmt.Errorf("graph did not drain: %d %v", len(objects), err)
	}
	if err = model.CheckReferences(); err != nil {
		return trace, err
	}
	if err = transport.Dispatch.CheckDrained(); err != nil {
		return trace, err
	}
	if combined {
		// Every queued fault must have reached a real production-port call.
		// A surviving injection would make marginal coverage misleading.
		transport.SignalTransport.mu.Lock()
		signalPending := len(transport.SignalTransport.faults)
		transport.SignalTransport.mu.Unlock()
		transport.StartTransport.mu.Lock()
		enqueuePending := len(transport.StartTransport.faults)
		transport.StartTransport.mu.Unlock()
		model.mu.Lock()
		graphPending := 0
		for _, faults := range model.faults {
			graphPending += len(faults)
		}
		model.mu.Unlock()
		if signalPending+enqueuePending+graphPending != 0 {
			return trace, fmt.Errorf("unreached combined fault: signal=%d enqueue=%d graph=%d", signalPending, enqueuePending, graphPending)
		}
		mode = mode + "/" + discoveryMode + "/" + enqueueMode + "/" + workerMode
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_graph_canonical_signal_runtime", Outcome: mode})
	if err = schedule.Finish(); err != nil {
		return trace, err
	}
	return trace, nil
}

func TestSeededGraphSignalRuntimeReplay(t *testing.T) {
	observed := map[string]int{}
	fail := func(seed int64, trace Trace, cause error) {
		path, e := saveSeedFailureTrace(t.Name(), seed, trace)
		if e != nil {
			t.Fatal(e)
		}
		t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, cause)
	}
	for seed := range seededSchedules(t) {
		generated, e := runGraphSignalRuntime(seed, nil)
		if e != nil {
			fail(seed, generated, e)
		}
		replayed, e := runGraphSignalRuntime(seed, &generated)
		if e != nil || !reflect.DeepEqual(generated, replayed) {
			fail(seed, generated, fmt.Errorf("replay differs: %v", e))
		}
		mode := generated.Decisions[0].Chosen
		observed[mode]++
		if dir := os.Getenv("SIM_GRAPH_SIGNAL_RUNTIME_ROOT"); dir != "" && observed[mode] == 1 {
			if e = generated.Save(filepath.Join(dir, mode+".json")); e != nil {
				t.Fatal(e)
			}
		}
	}
	if len(observed) != len(graphSignalRuntimeModes) {
		t.Fatalf("coverage=%v", observed)
	}
	t.Logf("canonical Signal runtime modes=%v; production client, bounded repair, worker consumption/replay, effect one and fixture graph drain", observed)
}
