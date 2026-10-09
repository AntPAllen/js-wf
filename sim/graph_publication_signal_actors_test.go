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
	"strings"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/internal/graphpublication"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/reconcile"
	"js-wf/wf"
	"js-wf/worker"
)

var graphSignalActorModes = []string{"healthy", "source_drop", "source_lost_ack", "enqueue_drop", "enqueue_lost_ack"}

// Client publication, canonical discovery, execution and collection share a
// scheduler, but hold independent adapter instances and process-local state.
func runGraphSignalActors(seed int64, replay *Trace) (trace Trace, runErr error) {
	s := NewScheduler(seed)
	if replay != nil {
		var e error
		s, e = ReplayScheduler(*replay)
		if e != nil {
			return trace, e
		}
	}
	if e := s.SetWorkload("graph_signal_operation_actors"); e != nil {
		return trace, e
	}
	defer func() { trace = s.Trace() }()
	mode, e := s.Choose(graphSignalActorModes)
	if e != nil {
		return trace, e
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	model := NewGraphPublicationTransport(s)
	transport := NewWorkerTransport(s, 3*time.Second)
	now := func() time.Time { return time.UnixMilli(s.NowMillis()).UTC() }
	store := func(y YieldFunc) (*journal.GraphStore, error) {
		p := model.Protocol()
		if y != nil {
			p = GraphProtocolWithYield(model, y)
		}
		return journal.NewGraphStore(journal.GraphConfig{Protocol: p, Now: now, PinTTL: 30 * time.Second, IntentTTL: time.Second, CanonicalStarts: true, CanonicalSignals: true})
	}
	clients := func(g *journal.GraphStore, y YieldFunc) (*client.Client, error) {
		if y == nil {
			return client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport).WithGraphJournal(g)
		}
		return client.NewWithSignalPorts(yieldingStartPort{yield: y, transport: transport.SignalTransport}, yieldingCanonicalSignalPort{yieldingClientSignalPort: yieldingClientSignalPort{yield: y, transport: transport.SignalTransport}, source: transport.SignalTransport}).WithGraphJournal(g)
	}
	g, e := store(nil)
	if e != nil {
		return trace, e
	}
	c, e := clients(g, nil)
	if e != nil {
		return trace, e
	}
	const typ = "test"
	id := integratedWorkerIDs(1)[0]
	h, e := c.Start(ctx, typ, id, []byte(`7`))
	if e != nil {
		return trace, e
	}
	switch mode {
	case "source_drop":
		e = transport.QueueSignalFault(SignalDropBeforeCommit)
	case "source_lost_ack":
		e = transport.QueueSignalFault(SignalLoseAckAfterCommit)
	case "enqueue_drop":
		e = transport.QueueFault(StartFault{Operation: "enqueue_run", Kind: "drop_before_commit"})
	case "enqueue_lost_ack":
		e = transport.QueueFault(StartFault{Operation: "enqueue_run", Kind: "lose_ack_after_commit"})
	}
	if e != nil {
		return trace, e
	}
	legacy := NewJournalTransport(s)
	legacyStore := journal.NewWithPorts(legacy, legacy)
	leasing := lease.NewWithKVPort(NewKVTransport(s, 30*time.Second))
	outcomes := NewKVTransport(s, 0)
	effects := 0
	runWorker := func(ctx context.Context, y YieldFunc, name string) error {
		g, e := store(y)
		if e != nil {
			return e
		}
		c, e := clients(g, y)
		if e != nil {
			return e
		}
		w, e := worker.NewWithPorts(name, map[string]worker.Handler{typ: func(w *wf.Context, input json.RawMessage) (json.RawMessage, error) {
			if string(input) != "7" {
				return nil, fmt.Errorf("changed Start input")
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
					return nil, fmt.Errorf("changed Signal input")
				}
			}
			return json.RawMessage(`42`), nil
		}}, worker.ModeledWorkerPorts{Journal: legacyStore, Leases: leasing, Outcome: outcomes, Invocation: transport.SignalTransport, Signals: transport.SignalTransport, Client: c, HeartbeatTicks: make(chan time.Time), OperationNow: now}, worker.WithGraphJournal(g))
		if e != nil {
			return e
		}
		runCtx, stop := context.WithCancel(ctx)
		defer stop()
		transport.Dispatch.StopWhenDrained(stop)
		var dispatch worker.DispatchPort = transport.Dispatch
		if y != nil {
			dispatch = yieldingDispatchPort{actorCtx: ctx, yield: y, transport: transport.Dispatch}
		}
		return w.RunPartitionWithTransport(runCtx, 0, dispatch)
	}
	actors := []CooperativeActor{}
	for _, name := range []string{"first", "second"} {
		name := name
		actors = append(actors, CooperativeActor{Name: name, Run: func(ctx context.Context, y YieldFunc) error {
			var last error
			for attempt := 0; attempt < 8; attempt++ {
				g, e := store(y)
				if e != nil {
					return e
				}
				c, e := clients(g, y)
				if e != nil {
					return e
				}
				_, e = c.Signal(ctx, typ, id, name, []byte(`true`), name)
				last = e
				if e == nil {
					return nil
				}
				if !errors.Is(e, client.ErrSignalUnknown) && !errors.Is(e, client.ErrEnqueueUnknown) && !errors.Is(e, journal.ErrUnknown) && !errors.Is(e, graphpublication.ErrConflict) {
					return e
				}
			}
			return fmt.Errorf("Signal retry budget exhausted: %w", last)
		}})
	}
	actors = append(actors, CooperativeActor{Name: "repair", Run: func(ctx context.Context, y YieldFunc) error {
		for attempt := 0; attempt < 3; attempt++ {
			g, e := store(y)
			if e != nil {
				return e
			}
			c, e := clients(g, y)
			if e != nil {
				return e
			}
			scan, e := reconcile.NewCanonicalSignalScanWithPort(g, c)
			if e != nil {
				return e
			}
			_, e = scan.Scan(ctx, 1, 1, false)
			if e != nil && !errors.Is(e, journal.ErrUnknown) {
				return e
			}
		}
		return nil
	}}, CooperativeActor{Name: "collector", Run: func(ctx context.Context, y YieldFunc) error {
		p := GraphProtocolWithYield(model, y)
		for i := 0; i < 2; i++ {
			if _, e := p.SweepWithReaders(ctx, now()); e != nil {
				return e
			}
		}
		return nil
	}}, CooperativeActor{Name: "worker", Run: func(ctx context.Context, y YieldFunc) error { return runWorker(ctx, y, "interleaved") }})
	results, e := RunCooperative(ctx, s, actors)
	if e != nil {
		return trace, e
	}
	for _, actor := range actors {
		if e := results[actor.Name]; e != nil {
			return trace, fmt.Errorf("actor %s: %w", actor.Name, e)
		}
	}
	// A worker may drain before a later client publishes. Fresh discovery and a
	// fresh worker recover that legitimate ordering without retaining caller input.
	g, e = store(nil)
	if e != nil {
		return trace, e
	}
	c, e = clients(g, nil)
	if e != nil {
		return trace, e
	}
	scan, e := reconcile.NewCanonicalSignalScanWithPort(g, c)
	if e != nil {
		return trace, e
	}
	if _, e = scan.Scan(ctx, 1, 1, false); e != nil {
		return trace, e
	}
	if e = c.Enqueue(ctx, typ, id, "recovery-wakeup"); e != nil {
		return trace, e
	}
	if e = runWorker(ctx, nil, "reopened"); e != nil {
		return trace, e
	}
	if e = c.Enqueue(ctx, typ, id, "completed-replay"); e != nil {
		return trace, e
	}
	if e = runWorker(ctx, nil, "completed"); e != nil {
		return trace, e
	}
	records, tail, e := g.Read(ctx, typ, id, h.InvSeq)
	if e != nil || len(records) == 0 || records[len(records)-1].Kind != journal.Completed || effects != 1 {
		return trace, fmt.Errorf("completion effects=%d: %v", effects, e)
	}
	consumed := 0
	for _, r := range records {
		if r.Kind == journal.SignalConsumed {
			consumed++
		}
	}
	status, e := g.InspectStart(ctx, typ, id)
	if e != nil || consumed != 2 || status.SignalConsumed != 2 || status.SignalBindings != 2 {
		return trace, fmt.Errorf("consumption/binding counts=%d %+v: %v", consumed, status, e)
	}
	for _, name := range []string{"first", "second"} {
		_, body, found, e := g.ReadSignalBinding(ctx, journal.GraphSignalRequest{Type: typ, ID: id, Invocation: h.InvSeq, Name: name, Key: name})
		if e != nil || !found || !bytes.Equal(body, []byte(`true`)) {
			return trace, fmt.Errorf("missing %s input: %v", name, e)
		}
	}
	if old, _, e := legacyStore.Read(ctx, typ, id); e != nil || len(old) != 0 {
		return trace, fmt.Errorf("legacy journal written: %v", e)
	}
	state, e := outcomes.Get(ctx, identity.Key(typ, id))
	if e != nil || !bytes.Equal(state.Value, records[len(records)-1].Payload) {
		return trace, fmt.Errorf("terminal projection differs: %v", e)
	}
	if e = model.CheckReferences(); e != nil {
		return trace, e
	}
	if e = g.Retire(ctx, typ, id, h.InvSeq, tail); e != nil {
		return trace, e
	}
	transport.PurgeInvocation(identity.InvocationSubject(typ, id))
	if e = s.AdvanceMillis(60000); e != nil {
		return trace, e
	}
	if _, e = model.Protocol().SweepWithReaders(ctx, now()); e != nil {
		return trace, e
	}
	objects, e := model.Objects(ctx)
	if e != nil || len(objects) != 0 {
		return trace, fmt.Errorf("graph did not drain: %d %v", len(objects), e)
	}
	if e = model.CheckReferences(); e != nil {
		return trace, e
	}
	if e = transport.Dispatch.CheckDrained(); e != nil {
		return trace, e
	}
	transport.SignalTransport.mu.Lock()
	pendingSignal := len(transport.SignalTransport.faults)
	transport.SignalTransport.mu.Unlock()
	transport.StartTransport.mu.Lock()
	pendingEnqueue := len(transport.StartTransport.faults)
	transport.StartTransport.mu.Unlock()
	if pendingSignal+pendingEnqueue != 0 {
		return trace, fmt.Errorf("unreached faults signal=%d enqueue=%d", pendingSignal, pendingEnqueue)
	}
	reached := map[string]bool{}
	switches := 0
	previous := ""
	for _, decision := range s.Trace().Decisions[1:] {
		actor := strings.SplitN(decision.Chosen, ":", 2)[0]
		reached[actor] = true
		if previous != "" && previous != actor {
			switches++
		}
		previous = actor
	}
	if len(reached) != 5 || switches < 4 {
		return trace, fmt.Errorf("actor coverage=%v switches=%d", reached, switches)
	}
	s.RecordTransport(TransportEvent{Operation: "check_graph_signal_operation_actors", Outcome: mode})
	return trace, s.Finish()
}

func TestSeededGraphSignalOperationActorsReplay(t *testing.T) {
	observed := map[string]int{}
	for seed := range seededSchedules(t) {
		generated, e := runGraphSignalActors(seed, nil)
		if e != nil {
			path, _ := saveSeedFailureTrace(t.Name(), seed, generated)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, e)
		}
		replayed, e := runGraphSignalActors(seed, &generated)
		if e != nil || !reflect.DeepEqual(generated, replayed) {
			path, _ := saveSeedFailureTrace(t.Name(), seed, generated)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s replay: %v", seed, path, e)
		}
		mode := generated.Decisions[0].Chosen
		observed[mode]++
		if dir := os.Getenv("SIM_GRAPH_SIGNAL_ACTORS_ROOT"); dir != "" && observed[mode] == 1 {
			if e = generated.Save(filepath.Join(dir, mode+".json")); e != nil {
				t.Fatal(e)
			}
		}
	}
	if len(observed) != len(graphSignalActorModes) {
		t.Fatalf("mode coverage=%v", observed)
	}
	t.Logf("operation-level actors: %v; five actors reached per schedule, all selected faults consumed, effect one, two bindings/consumptions, terminal equality and graph/dispatch drain", observed)
}
