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
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/internal/graphpublication"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/wf"
	"js-wf/worker"
)

var graphWorkerDeadlineModes = []string{"grant_read", "cas_not_committed", "cas_committed_unknown", "cas_committed_readback"}

type workerDeadlineAuthority struct {
	*GraphPublicationTransport
	mode                          string
	armed, fired, readback, scope bool
	casCalls                      int
}

func (p *workerDeadlineAuthority) deadline(op, key string) {
	p.schedule.RecordTransport(TransportEvent{Operation: "worker_append_deadline_" + op, Subject: key, Outcome: p.mode})
}
func (p *workerDeadlineAuthority) ReadBlob(ctx context.Context, key string) (graphpublication.Record, error) {
	record, err := p.GraphPublicationTransport.ReadBlob(ctx, key)
	if err == nil && p.armed && p.mode == "grant_read" && record.Fence.Phase == "ready" {
		p.armed = false
		p.fired = true
		p.deadline("grant", key)
		return graphpublication.Record{}, context.DeadlineExceeded
	}
	return record, err
}
func (p *workerDeadlineAuthority) ReadRoot(ctx context.Context, key string) (graphpublication.Root, error) {
	if p.readback {
		p.readback = false
		p.deadline("readback", key)
		return graphpublication.Root{}, context.DeadlineExceeded
	}
	return p.GraphPublicationTransport.ReadRoot(ctx, key)
}
func (p *workerDeadlineAuthority) CASRoot(ctx context.Context, key string, expected uint64, root graphpublication.Root) (graphpublication.Root, error) {
	p.mu.Lock()
	previous := p.roots[key]
	p.mu.Unlock()
	appendMutation := root.Graph.Count == previous.Graph.Count+1
	if p.scope && appendMutation {
		p.casCalls++
	}
	if !p.armed || p.mode == "grant_read" || !appendMutation {
		return p.GraphPublicationTransport.CASRoot(ctx, key, expected, root)
	}
	p.armed = false
	p.fired = true
	if p.mode == "cas_not_committed" {
		p.deadline("cas", key)
		return graphpublication.Root{}, context.DeadlineExceeded
	}
	ack, err := p.GraphPublicationTransport.CASRoot(ctx, key, expected, root)
	if err != nil {
		return ack, err
	}
	p.readback = p.mode == "cas_committed_unknown"
	p.deadline("cas", key)
	return graphpublication.Root{}, context.DeadlineExceeded
}

func runGraphWorkerDeadline(seed int64, replay *Trace) (trace Trace, runErr error) {
	s := NewScheduler(seed)
	if replay != nil {
		var err error
		s, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	if err := s.SetWorkload("graph_worker_append_deadline"); err != nil {
		return trace, err
	}
	defer func() { trace = s.Trace() }()
	stage, err := s.Choose([]string{"request", "completion", "terminal"})
	if err != nil {
		return trace, err
	}
	encoding, err := s.Choose([]string{string(journal.JSON), string(journal.ProtobufV1)})
	if err != nil {
		return trace, err
	}
	mode, err := s.Choose(graphWorkerDeadlineModes)
	if err != nil {
		return trace, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	m := NewGraphPublicationTransport(s)
	p := &workerDeadlineAuthority{GraphPublicationTransport: m, mode: mode}
	protocol := m.Protocol()
	protocol.Port = p
	now := func() time.Time { return time.UnixMilli(s.NowMillis()).UTC() }
	graph, err := journal.NewGraphStore(journal.GraphConfig{Protocol: protocol, Encoding: journal.Encoding(encoding), Now: now, PinTTL: 30 * time.Second, IntentTTL: time.Second})
	if err != nil {
		return trace, err
	}
	transport := NewWorkerTransport(s, 3*time.Second)
	legacy := NewJournalTransport(s)
	store := journal.NewWithPorts(legacy, legacy)
	leasing := lease.NewWithKVPort(NewKVTransport(s, 30*time.Second))
	outcomes := NewKVTransport(s, 0)
	c := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport)
	const typ = "test"
	id := integratedWorkerIDs(1)[0]
	effects, handlers, appendErrors, retryRenewals := 0, 0, 0, 0
	var observedErr error
	target := journal.StepRequested
	targetIndex := uint64(1)
	if stage == "completion" {
		target = journal.StepCompleted
		targetIndex = 2
	}
	if stage == "terminal" {
		target = journal.Completed
		targetIndex = 3
	}
	handler := func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
		handlers++
		if !bytes.Equal(input, []byte(`7`)) {
			return nil, fmt.Errorf("input changed")
		}
		result, err := wf.Run(c, "effect", 7, func(context.Context) (int, error) { effects++; return 42, nil })
		if err != nil {
			return nil, err
		}
		if result != 42 {
			return nil, fmt.Errorf("result changed")
		}
		return json.RawMessage(`42`), nil
	}
	build := func(name string) (*worker.Worker, error) {
		return worker.NewWithPorts(name, map[string]worker.Handler{typ: handler}, worker.ModeledWorkerPorts{Journal: store, Leases: leasing, Outcome: outcomes, Invocation: transport.SignalTransport, Signals: transport.SignalTransport, Client: c, HeartbeatTicks: make(chan time.Time), OperationNow: now, OperationObserver: func(e worker.OperationEvent) {
			if e.Operation == "lease_renew_append_retry" {
				retryRenewals++
			}
			if e.Operation == "lease_renew_append" && e.JournalKind == target && e.Error == "" && !p.fired {
				p.armed = true
				p.scope = true
			}
			if e.Operation == "journal_append" && e.JournalKind == target && p.scope {
				p.scope = false
				if e.Error != "" {
					appendErrors++
					observedErr = errors.New(e.Error)
				}
			}
		}}, worker.WithGraphJournal(graph))
	}
	first, err := build("deadline-first")
	if err != nil {
		return trace, err
	}
	handle, err := c.Start(ctx, typ, id, []byte(`7`))
	if err != nil {
		return trace, err
	}
	run := func(w *worker.Worker) error {
		attempt, stop := context.WithCancel(ctx)
		defer stop()
		transport.Dispatch.StopAfterNextNak(stop)
		transport.Dispatch.StopAfterNextAck(stop)
		return w.RunPartitionWithTransport(attempt, 0, transport.Dispatch)
	}
	uncertain := mode != "cas_committed_readback"
	if err = run(first); err != nil {
		return trace, err
	}
	if !p.fired || p.armed || p.readback || p.scope {
		return trace, fmt.Errorf("cut not completed")
	}
	wantCAS := 1
	if mode == "grant_read" {
		wantCAS = 0
	}
	if retryRenewals != 0 {
		return trace, fmt.Errorf("uncertain append retried after %d renewals", retryRenewals)
	}
	if p.casCalls != wantCAS {
		return trace, fmt.Errorf("uncertain operation retried: cas=%d expected=%d", p.casCalls, wantCAS)
	}
	if uncertain && (appendErrors != 1 || observedErr == nil) || !uncertain && appendErrors != 0 {
		return trace, fmt.Errorf("wrong append error observation %v", observedErr)
	}
	if ctx.Err() != nil {
		return trace, fmt.Errorf("enclosing context expired: %w", ctx.Err())
	}
	records, tail, err := graph.Read(ctx, typ, id, handle.InvSeq)
	if err != nil {
		return trace, err
	}
	wantCount := targetIndex
	if mode == "cas_committed_unknown" {
		wantCount++
	}
	if !uncertain {
		wantCount = 4
	}
	if uint64(len(records)) != wantCount || tail != wantCount {
		return trace, fmt.Errorf("cut journal population=%d tail=%d expected=%d", len(records), tail, wantCount)
	}
	for _, record := range records {
		if record.Kind == journal.Failed {
			return trace, fmt.Errorf("uncertainty became durable failure")
		}
	}
	state, stateErr := outcomes.Get(ctx, identity.Key(typ, id))
	if uncertain && !errors.Is(stateErr, jetstream.ErrKeyNotFound) {
		return trace, fmt.Errorf("uncertain append projected outcome: %v", stateErr)
	}
	if !uncertain && (stateErr != nil || !bytes.Equal(state.Value, records[3].Payload)) {
		return trace, fmt.Errorf("resolved append did not project exact outcome")
	}
	if uncertain && transport.Dispatch.Pending() != 1 || !uncertain && transport.Dispatch.Pending() != 0 {
		return trace, fmt.Errorf("wrong ACK/NAK state")
	}
	firstEffects := 1
	if stage == "request" && uncertain {
		firstEffects = 0
	}
	if effects != firstEffects || handlers != 1 {
		return trace, fmt.Errorf("first attempt effects=%d handlers=%d", effects, handlers)
	}
	// A new worker reacquires the released lease and reads authoritative history.
	if !uncertain {
		if err = c.Enqueue(ctx, typ, id, "deadline-duplicate"); err != nil {
			return trace, err
		}
	}
	second, err := build("deadline-second")
	if err != nil {
		return trace, err
	}
	if err = run(second); err != nil {
		return trace, err
	}
	records, tail, err = graph.Read(ctx, typ, id, handle.InvSeq)
	if err != nil {
		return trace, err
	}
	if len(records) != 4 || tail != 4 {
		return trace, fmt.Errorf("recovery journal population=%d tail=%d", len(records), tail)
	}
	for index, kind := range []journal.Kind{journal.Started, journal.StepRequested, journal.StepCompleted, journal.Completed} {
		if records[index].Index != uint64(index) || records[index].Kind != kind {
			return trace, fmt.Errorf("recovery changed record order")
		}
	}
	wantEffects := 1
	if stage == "completion" && (mode == "grant_read" || mode == "cas_not_committed") {
		wantEffects = 2
	}
	wantHandlers := 2
	if !uncertain || stage == "terminal" && mode == "cas_committed_unknown" {
		wantHandlers = 1
	}
	if effects != wantEffects || handlers != wantHandlers {
		return trace, fmt.Errorf("recovery effects=%d/%d handlers=%d/%d", effects, wantEffects, handlers, wantHandlers)
	}
	state, err = outcomes.Get(ctx, identity.Key(typ, id))
	if err != nil || !bytes.Equal(state.Value, records[3].Payload) {
		return trace, fmt.Errorf("recovery projection differs: %v", err)
	}
	var outcome wf.Outcome
	if wf.DecodeOutcome(state.Value, &outcome) != nil || outcome.InvSeq != handle.InvSeq || string(outcome.Result) != "42" || outcome.Error != "" {
		return trace, fmt.Errorf("invalid recovered outcome")
	}
	old, _, err := store.Read(ctx, typ, id)
	if err != nil || len(old) != 0 {
		return trace, fmt.Errorf("wrote legacy journal")
	}
	if err = m.CheckReferences(); err != nil {
		return trace, err
	}
	if err = transport.Dispatch.CheckDrained(); err != nil {
		return trace, err
	}
	if err = outcomes.Delete(ctx, identity.Key(typ, id), state.Revision); err != nil {
		return trace, err
	}
	transport.PurgeInvocation(identity.InvocationSubject(typ, id))
	if err = graph.Retire(ctx, typ, id, handle.InvSeq, tail); err != nil {
		return trace, err
	}
	if err = s.AdvanceMillis(60000); err != nil {
		return trace, err
	}
	if _, err = protocol.SweepWithReaders(ctx, now()); err != nil {
		return trace, err
	}
	objects, err := m.Objects(ctx)
	if err != nil || len(objects) != 0 {
		return trace, fmt.Errorf("retained attempts not drained: %v", err)
	}
	if err = m.CheckReferences(); err != nil {
		return trace, err
	}
	s.RecordTransport(TransportEvent{Operation: "check_graph_worker_append_deadline", Outcome: stage + "_" + encoding + "_" + mode})
	return trace, s.Finish()
}

func TestSeededGraphWorkerAppendDeadlineReplay(t *testing.T) {
	seen := map[string]int{}
	for seed := range seededSchedules(t) {
		generated, err := runGraphWorkerDeadline(seed, nil)
		if err != nil {
			path, saveErr := saveStartSourceHeaderFailure(generated, os.Getenv("FAULT_TRACE_OUT"))
			t.Fatalf("seed=%d trace=%s save=%v: %v", seed, path, saveErr, err)
		}
		replayed, err := runGraphWorkerDeadline(seed, &generated)
		if err != nil || !reflect.DeepEqual(generated, replayed) {
			t.Fatalf("seed=%d replay differs: %v", seed, err)
		}
		mode := generated.Decisions[0].Chosen + "_" + generated.Decisions[1].Chosen + "_" + generated.Decisions[2].Chosen
		seen[mode]++
		if root := os.Getenv("SIM_GRAPH_WORKER_DEADLINE_ROOT"); root != "" && seen[mode] == 1 {
			if err = generated.Save(filepath.Join(root, "graph_worker_append_deadline_"+mode+".json")); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(seen) != 24 {
		t.Fatalf("coverage=%v", seen)
	}
	t.Logf("WORKER_APPEND_DEADLINE modes=%v; ACK/NAK cut, fresh worker, recorded effect replay, exact four records and physical drain", seen)
}
