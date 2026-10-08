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

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/wf"
	"js-wf/worker"
)

var graphStartModes = []string{"ordinary", "reserved", "source_committed", "bound_no_enqueue", "drop_reserve", "lost_reserve", "unknown_reserve", "put_drop", "put_lost", "drop_source", "lost_source", "bind_drop", "bind_lost", "unknown_bind", "enqueue_drop", "enqueue_lost", "input_mismatch", "foreign_pointer"}

type graphStartPort struct {
	*SignalTransport
	afterPublish func() error
}

func (p *graphStartPort) PutInput(context.Context, string, []byte) error {
	return fmt.Errorf("canonical start used legacy input staging")
}
func (p *graphStartPort) PublishInvocation(ctx context.Context, msg *nats.Msg) (uint64, error) {
	sequence, err := p.SignalTransport.PublishInvocation(ctx, msg)
	if err == nil && p.afterPublish != nil {
		callback := p.afterPublish
		p.afterPublish = nil
		if e := callback(); e != nil {
			return sequence, e
		}
	}
	return sequence, err
}

func runGraphStart(seed int64, replay *Trace) (trace Trace, runErr error) {
	s := NewScheduler(seed)
	if replay != nil {
		var err error
		s, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	if err := s.SetWorkload("graph_canonical_start"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = s.Trace() }()
	mode, err := s.Choose(graphStartModes)
	if err != nil {
		return trace, err
	}
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	m := NewGraphPublicationTransport(s)
	now := func() time.Time { return time.UnixMilli(s.NowMillis()).UTC() }
	newStore := func() (*journal.GraphStore, error) {
		return journal.NewGraphStore(journal.GraphConfig{Protocol: m.Protocol(), Now: now, PinTTL: 30 * time.Second, IntentTTL: time.Second, CanonicalStarts: true})
	}
	graph, err := newStore()
	if err != nil {
		return trace, err
	}
	transport := NewWorkerTransport(s, 3*time.Second)
	port := &graphStartPort{SignalTransport: transport.SignalTransport}
	newClient := func(store *journal.GraphStore) (*client.Client, error) {
		return client.NewWithSignalPorts(port, port).WithGraphJournal(store)
	}
	c, err := newClient(graph)
	if err != nil {
		return trace, err
	}
	const typ = "test"
	id := integratedWorkerIDs(1)[0]
	input := []byte(`7`)
	request := journal.GraphStartRequest{Type: typ, ID: id}
	if mode == "reserved" || mode == "source_committed" || mode == "bound_no_enqueue" || mode == "input_mismatch" || mode == "foreign_pointer" {
		state, e := graph.ReserveStart(ctx, request, input)
		if e != nil {
			return trace, e
		}
		if _, e = graph.Begin(ctx, typ, id, 1); !errors.Is(e, journal.ErrStale) {
			return trace, fmt.Errorf("pending start admitted execution: %v", e)
		}
		if mode == "source_committed" || mode == "bound_no_enqueue" || mode == "foreign_pointer" {
			msg := &nats.Msg{Subject: identity.InvocationSubject(typ, id), Data: state.Start.PointerBytes(), Header: nats.Header{}}
			msg.Header.Set(journal.GraphStartTokenHeader, state.Start.Token)
			msg.Header.Set("Wf-Input-SHA256", state.Start.InputSHA256)
			msg.Header.Set(jetstream.ExpectedLastSubjSeqHeader, "0")
			if mode == "foreign_pointer" {
				msg.Header.Set(journal.GraphStartTokenHeader, "foreign")
			}
			seq, e := port.PublishInvocation(ctx, msg)
			if e != nil {
				return trace, e
			}
			if mode == "bound_no_enqueue" {
				if e = graph.BindStart(ctx, typ, id, state.Start.Token, seq); e != nil {
					return trace, e
				}
			}
		}
	}
	switch mode {
	case "drop_reserve":
		err = m.QueueFault("cas_root", DropBeforeCommit)
	case "lost_reserve", "unknown_reserve":
		err = m.QueueFault("cas_root", LoseAckAfterCommit)
		if mode == "unknown_reserve" {
			m.PauseBefore("cas_root", func() error { return m.QueueFault("read_root", DropBeforeCommit) })
		}
	case "put_drop":
		err = m.QueueFault("put", DropBeforeCommit)
	case "put_lost":
		err = m.QueueFault("put", LoseAckAfterCommit)
	case "drop_source":
		err = port.QueueFault(StartFault{Operation: "publish_invocation", Kind: "drop_before_commit"})
	case "lost_source":
		err = port.QueueFault(StartFault{Operation: "publish_invocation", Kind: "lose_ack_after_commit"})
	case "enqueue_drop":
		err = port.QueueFault(StartFault{Operation: "enqueue_run", Kind: "drop_before_commit"})
	case "enqueue_lost":
		err = port.QueueFault(StartFault{Operation: "enqueue_run", Kind: "lose_ack_after_commit"})
	case "bind_drop", "bind_lost", "unknown_bind":
		port.afterPublish = func() error {
			// Bind validates input under an acquire/release pair before its CAS.
			m.PauseBefore("cas_root", func() error {
				m.PauseBefore("cas_root", func() error {
					m.PauseBefore("cas_root", func() error {
						fault := LoseAckAfterCommit
						if mode == "bind_drop" {
							fault = DropBeforeCommit
						}
						if e := m.QueueFault("cas_root", fault); e != nil {
							return e
						}
						if mode == "unknown_bind" {
							return m.QueueFault("read_root", DropBeforeCommit)
						}
						return nil
					})
					return nil
				})
				return nil
			})
			return nil
		}
	}
	if err != nil {
		return trace, err
	}
	var handle client.Handle
	if mode == "reserved" || mode == "source_committed" || mode == "bound_no_enqueue" || mode == "foreign_pointer" {
		handle, err = c.RecoverStart(ctx, typ, id)
	} else {
		data := input
		if mode == "input_mismatch" {
			data = []byte(`8`)
		}
		handle, err = c.Start(ctx, typ, id, data)
	}
	unknown := mode == "drop_reserve" || mode == "unknown_reserve" || mode == "put_drop" || mode == "put_lost" || mode == "drop_source" || mode == "bind_drop" || mode == "unknown_bind" || mode == "foreign_pointer"
	switch {
	case mode == "input_mismatch":
		if !errors.Is(err, client.ErrInputMismatch) {
			return trace, fmt.Errorf("different input accepted: %v", err)
		}
	case unknown:
		if !errors.Is(err, client.ErrStartUnknown) {
			return trace, fmt.Errorf("unknown stage accepted: %s %v", mode, err)
		}
	case mode == "enqueue_drop" || mode == "enqueue_lost":
		if !errors.Is(err, client.ErrEnqueueUnknown) {
			return trace, fmt.Errorf("unknown enqueue accepted: %v", err)
		}
	case mode == "lost_source":
		if !errors.Is(err, client.ErrAlreadyStarted) {
			return trace, fmt.Errorf("lost source not confirmed: %v", err)
		}
	default:
		if err != nil {
			return trace, err
		}
	}
	if unknown || mode == "input_mismatch" {
		if len(port.Runs()) != 0 {
			return trace, fmt.Errorf("unconfirmed start enqueued a run")
		}
		if mode == "foreign_pointer" {
			port.PurgeInvocation(identity.InvocationSubject(typ, id))
		}
	}
	// Discard client/store handles; recover only from durable transport state.
	graph, err = newStore()
	if err != nil {
		return trace, err
	}
	c, err = newClient(graph)
	if err != nil {
		return trace, err
	}
	if mode == "drop_reserve" || mode == "put_drop" || mode == "put_lost" {
		if _, e := c.RecoverStart(ctx, typ, id); !errors.Is(e, client.ErrNotFound) {
			return trace, fmt.Errorf("unpublished fork adopted: %v", e)
		}
		handle, err = c.Start(ctx, typ, id, input)
	} else {
		handle, err = c.RecoverStart(ctx, typ, id)
	}
	if err != nil {
		return trace, err
	}
	raw, err := port.LastInvocation(ctx, identity.InvocationSubject(typ, id))
	if err != nil || raw.Sequence != handle.InvSeq || raw.Header.Get(journal.GraphStartTokenHeader) == "" || raw.Header.Get("Wf-Input-Ref") != "" {
		return trace, fmt.Errorf("bound source pointer: %v", err)
	}
	state, owned, err := graph.ReadStart(ctx, typ, id)
	if err != nil || state.Pending || state.Invocation != handle.InvSeq || !bytes.Equal(owned, input) || !state.Start.MatchesInvocation(raw) {
		return trace, fmt.Errorf("canonical start not ready: %v", err)
	}
	if duplicate, e := c.Start(ctx, typ, id, input); !errors.Is(e, client.ErrAlreadyStarted) || duplicate != handle {
		return trace, fmt.Errorf("idempotent start changed handle: %v", e)
	}
	legacy := NewJournalTransport(s)
	legacyStore := journal.NewWithPorts(legacy, legacy)
	leasing := lease.NewWithKVPort(NewKVTransport(s, 30*time.Second))
	outcomes := NewKVTransport(s, 0)
	effects := 0
	build := func(name string) (*worker.Worker, error) {
		return worker.NewWithPorts(name, map[string]worker.Handler{typ: func(c *wf.Context, got json.RawMessage) (json.RawMessage, error) {
			if !bytes.Equal(got, input) {
				return nil, fmt.Errorf("worker input differs")
			}
			_, e := wf.Run(c, "once", 0, func(context.Context) (int, error) { effects++; return 42, nil })
			if e != nil {
				return nil, e
			}
			return json.RawMessage(`42`), nil
		}}, worker.ModeledWorkerPorts{Journal: legacyStore, Leases: leasing, Outcome: outcomes, Invocation: transport.SignalTransport, Signals: transport.SignalTransport, Client: c, HeartbeatTicks: make(chan time.Time), OperationNow: now}, worker.WithGraphJournal(graph))
	}
	run := func(w *worker.Worker) error {
		runctx, cancel := context.WithCancel(ctx)
		defer cancel()
		transport.Dispatch.StopWhenDrained(cancel)
		return w.RunPartitionWithTransport(runctx, 0, transport.Dispatch)
	}
	w, err := build("canonical-first")
	if err != nil {
		return trace, err
	}
	if err = run(w); err != nil {
		return trace, err
	}
	if err = c.Enqueue(ctx, typ, id, "canonical-replay"); err != nil {
		return trace, err
	}
	w, err = build("canonical-replay")
	if err != nil {
		return trace, err
	}
	if err = run(w); err != nil {
		return trace, err
	}
	records, tail, err := graph.Read(ctx, typ, id, handle.InvSeq)
	if err != nil || len(records) == 0 || records[len(records)-1].Kind != journal.Completed || effects != 1 {
		return trace, fmt.Errorf("execution/replay effects=%d: %v", effects, err)
	}
	if old, _, e := legacyStore.Read(ctx, typ, id); e != nil || len(old) != 0 {
		return trace, fmt.Errorf("legacy journal written: %v", e)
	}
	if err = m.CheckReferences(); err != nil {
		return trace, err
	}
	if err = graph.FencePurge(ctx, typ, id, handle.InvSeq, tail); err != nil {
		return trace, err
	}
	if _, e := c.Start(ctx, typ, id, input); !errors.Is(e, client.ErrPurged) {
		return trace, fmt.Errorf("fenced start admitted: %v", e)
	}
	if err = graph.Retire(ctx, typ, id, handle.InvSeq, tail); err != nil {
		return trace, err
	}
	port.PurgeInvocation(identity.InvocationSubject(typ, id))
	if err = s.AdvanceMillis(60000); err != nil {
		return trace, err
	}
	if _, err = m.Protocol().SweepWithReaders(ctx, now()); err != nil {
		return trace, err
	}
	objects, err := m.Objects(ctx)
	if err != nil || len(objects) != 0 {
		return trace, fmt.Errorf("graph objects not drained: %d %v", len(objects), err)
	}
	if err = m.CheckReferences(); err != nil {
		return trace, err
	}
	if err = transport.Dispatch.CheckDrained(); err != nil {
		return trace, err
	}
	s.RecordTransport(TransportEvent{Operation: "check_graph_canonical_start", Outcome: mode})
	if err = s.Finish(); err != nil {
		return trace, err
	}
	return trace, nil
}

func TestSeededGraphStartReplay(t *testing.T) {
	observed := map[string]int{}
	fail := func(seed int64, trace Trace, cause error) {
		path := os.Getenv("FAULT_TRACE_OUT")
		if path == "" {
			dir, e := os.MkdirTemp("", "js-wf-graph-start-failure-")
			if e != nil {
				t.Fatal(e)
			}
			path = filepath.Join(dir, "trace.json")
		}
		if e := trace.Save(path); e != nil {
			t.Fatal(e)
		}
		t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, cause)
	}
	for seed := range seededSchedules(t) {
		generated, e := runGraphStart(seed, nil)
		if e != nil {
			fail(seed, generated, e)
		}
		mode := generated.Decisions[0].Chosen
		observed[mode]++
		replayed, e := runGraphStart(seed, &generated)
		if e != nil || !reflect.DeepEqual(generated, replayed) {
			fail(seed, generated, fmt.Errorf("start replay differs: %v", e))
		}
		if dir := os.Getenv("SIM_GRAPH_START_ROOT"); dir != "" && observed[mode] == 1 {
			if e = generated.Save(filepath.Join(dir, mode+".json")); e != nil {
				t.Fatal(e)
			}
		}
	}
	if len(observed) != len(graphStartModes) {
		t.Fatalf("start coverage=%v", observed)
	}
	t.Logf("graph start: modes=%v; canonical staging, source-sequence binding, durable restart recovery, production worker replay/effect one and explicit fixture graph drain", observed)
}
