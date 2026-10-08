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
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/reconcile"
	"js-wf/retention"
	"js-wf/wf"
	"js-wf/worker"
)

type terminalCatalogProjection struct{ kv *KVTransport }

func (p terminalCatalogProjection) ProjectionPresent(ctx context.Context, typ, id string) (bool, error) {
	_, e := p.kv.Get(ctx, identity.Key(typ, id))
	if errors.Is(e, jetstream.ErrKeyNotFound) {
		return false, nil
	}
	return e == nil, e
}

var terminalCatalogModes = []string{"healthy", "lookup_unknown", "catalog_unknown", "watermark_unknown", "enqueue_drop", "enqueue_lost_ack", "repeat_delete", "dry_run", "failed_terminal", "present", "purge_marker", "retired"}

func runGraphTerminalCatalog(seed int64, replay *Trace) (trace Trace, runErr error) {
	schedule := NewScheduler(seed)
	if replay != nil {
		var e error
		schedule, e = ReplayScheduler(*replay)
		if e != nil {
			return trace, e
		}
	}
	if e := schedule.SetWorkload("graph_terminal_catalog_recovery"); e != nil {
		return trace, e
	}
	defer func() { trace = schedule.Trace() }()
	mode, e := schedule.Choose(terminalCatalogModes)
	if e != nil {
		return trace, e
	}
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	model := NewGraphPublicationTransport(schedule)
	now := func() time.Time { return time.UnixMilli(schedule.NowMillis()).UTC() }
	newGraph := func() (*journal.GraphStore, error) {
		return journal.NewGraphStore(journal.GraphConfig{Protocol: model.Protocol(), CanonicalStarts: true, Now: now, PinTTL: 30 * time.Second, IntentTTL: time.Second})
	}
	graph, e := newGraph()
	if e != nil {
		return trace, e
	}
	transport := NewWorkerTransport(schedule, 3*time.Second)
	c, e := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport).WithGraphJournal(graph)
	if e != nil {
		return trace, e
	}
	const typ = "test"
	id := integratedWorkerIDs(1)[0]
	h, e := c.Start(ctx, typ, id, []byte(`7`))
	if e != nil {
		return trace, e
	}
	status, e := graph.InspectStart(ctx, typ, id)
	if e != nil {
		return trace, e
	}
	tail, e := graph.Begin(ctx, typ, id, h.InvSeq)
	if e != nil {
		return trace, e
	}
	started, _ := json.Marshal(map[string]string{"input_sha256": status.State.Start.InputSHA256})
	tail, e = graph.Append(ctx, typ, id, h.InvSeq, journal.Entry{Kind: journal.Started, Payload: started}, tail, [][]byte{[]byte(`7`)}, nil)
	if e != nil {
		return trace, e
	}
	outcome := wf.Outcome{InvSeq: h.InvSeq, Result: json.RawMessage(`42`)}
	kind := journal.Completed
	if mode == "failed_terminal" {
		outcome = wf.Outcome{InvSeq: h.InvSeq, Error: "fixture terminal failure"}
		kind = journal.Failed
	}
	body, _ := json.Marshal(outcome)
	tail, e = graph.Append(ctx, typ, id, h.InvSeq, journal.Entry{Index: 1, Kind: kind, Payload: body}, tail, nil, nil)
	if e != nil {
		return trace, e
	}
	// Remove the original wakeup after the prepared terminal publication. The
	// scanner is the sole source of subsequent dispatch; no caller key is supplied.
	consumer, e := transport.Dispatch.Consumer(ctx, 0)
	if e != nil {
		return trace, e
	}
	batch, e := consumer.FetchOne(ctx)
	if e != nil {
		return trace, e
	}
	msg, ok := <-batch.Messages()
	if !ok {
		return trace, fmt.Errorf("missing original dispatch")
	}
	if e = msg.DoubleAck(ctx); e != nil {
		return trace, e
	}
	if e = transport.Dispatch.CheckDrained(); e != nil {
		return trace, e
	}
	projection := NewKVTransport(schedule, 0)
	if mode == "present" {
		if _, e = projection.Create(ctx, identity.Key(typ, id), body); e != nil {
			return trace, e
		}
	}
	if mode == "purge_marker" {
		value, _ := json.Marshal(retention.Tombstone{Tombstone: true, InvSeq: h.InvSeq, PurgedAt: now(), ExpiresAt: now().Add(time.Hour)})
		if _, e = projection.Create(ctx, identity.Key(typ, id), value); e != nil {
			return trace, e
		}
	}
	if mode == "retired" {
		if e = graph.Retire(ctx, typ, id, h.InvSeq, tail); e != nil {
			return trace, e
		}
	}
	graph, e = newGraph()
	if e != nil {
		return trace, e
	}
	c, e = client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport).WithGraphJournal(graph)
	if e != nil {
		return trace, e
	}
	scan, e := reconcile.NewCanonicalTerminalScanWithPort(graph, terminalCatalogProjection{projection}, c)
	if e != nil {
		return trace, e
	}
	if mode == "lookup_unknown" {
		e = projection.QueueFault(KVFault{Operation: "get", Kind: KVGetTransportLost})
	}
	if mode == "catalog_unknown" {
		e = model.QueueFault("next_root", DropBeforeCommit)
	}
	if mode == "watermark_unknown" {
		e = model.QueueFault("catalog_high_water", DropBeforeCommit)
	}
	if mode == "enqueue_drop" || mode == "enqueue_lost_ack" {
		fault := "drop_before_commit"
		if mode == "enqueue_lost_ack" {
			fault = "lose_ack_after_commit"
		}
		e = transport.QueueFault(StartFault{Operation: "enqueue_run", Kind: fault})
	}
	if e != nil {
		return trace, e
	}
	if mode == "dry_run" {
		r, e := scan.Scan(ctx, 1, 1, true)
		if e != nil || len(r.Candidates) != 1 || r.Reenqueued != 0 || transport.Dispatch.Pending() != 0 {
			return trace, fmt.Errorf("dry catalog changed dispatch: %+v %v", r, e)
		}
	}
	if mode == "lookup_unknown" || mode == "catalog_unknown" || mode == "watermark_unknown" || mode == "enqueue_drop" || mode == "enqueue_lost_ack" {
		r, e := scan.Scan(ctx, 1, 1, false)
		if !errors.Is(e, journal.ErrUnknown) || r.NextSequence != 1 || r.RetrySequence != 0 {
			return trace, fmt.Errorf("unknown catalog advanced cursor: %+v %v", r, e)
		}
		// Reopened scanner retains no local cycle watermark or repair keys.
		graph, e = newGraph()
		if e != nil {
			return trace, e
		}
		c, e = client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport).WithGraphJournal(graph)
		if e != nil {
			return trace, e
		}
		scan, e = reconcile.NewCanonicalTerminalScanWithPort(graph, terminalCatalogProjection{projection}, c)
		if e != nil {
			return trace, e
		}
	}
	cursor := uint64(1)
	if mode == "present" || mode == "purge_marker" || mode == "retired" {
		for pass := 0; pass < 4; pass++ {
			r, e := scan.Scan(ctx, cursor, 1, false)
			if e != nil || r.Reenqueued != 0 || transport.Dispatch.Pending() != 0 {
				return trace, fmt.Errorf("ineligible terminal enqueued: %+v %v", r, e)
			}
			cursor = r.NextSequence
		}
	} else {
		leasesKV := NewKVTransport(schedule, 30*time.Second)
		leasing := lease.NewWithKVPort(leasesKV)
		owner, e := leasing.Acquire(ctx, typ, id, "healthy-owner")
		if e != nil {
			return trace, e
		}
		before, e := leasesKV.Get(ctx, identity.Key(typ, id))
		if e != nil {
			return trace, e
		}
		legacy := NewJournalTransport(schedule)
		legacyStore := journal.NewWithPorts(legacy, legacy)
		handlers := 0
		recover := func() error {
			reached := false
			offset := len(schedule.Trace().Transport)
			for pass := 0; pass < 8; pass++ {
				r, e := scan.Scan(ctx, cursor, 1, false)
				if e != nil {
					return e
				}
				cursor = r.NextSequence
				if r.Reenqueued == 1 {
					reached = true
					break
				}
			}
			if !reached {
				return fmt.Errorf("bounded cycle failed to discover terminal")
			}
			for _, event := range schedule.Trace().Transport[offset:] {
				switch event.Operation {
				case "graph_publication_get", "graph_publication_put", "graph_publication_cas_root", "graph_publication_cas_blob":
					return fmt.Errorf("catalog changed payload ownership: %+v", event)
				}
			}
			w, e := worker.NewWithPorts("terminal-catalog", map[string]worker.Handler{typ: func(*wf.Context, json.RawMessage) (json.RawMessage, error) {
				handlers++
				return nil, fmt.Errorf("terminal handler entered")
			}}, worker.ModeledWorkerPorts{Journal: legacyStore, Leases: leasing, Outcome: projection, Invocation: transport.SignalTransport, Signals: transport.SignalTransport, Client: c, HeartbeatTicks: make(chan time.Time), OperationNow: now}, worker.WithGraphJournal(graph))
			if e != nil {
				return e
			}
			runCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			transport.Dispatch.StopWhenDrained(cancel)
			if e = w.RunPartitionWithTransport(runCtx, 0, transport.Dispatch); e != nil {
				return e
			}
			cache, e := projection.Get(ctx, identity.Key(typ, id))
			if e != nil || !bytes.Equal(cache.Value, body) {
				return fmt.Errorf("projection not recovered: %v", e)
			}
			return transport.Dispatch.CheckDrained()
		}
		if e = recover(); e != nil {
			return trace, e
		}
		if mode == "repeat_delete" {
			cache, e := projection.Get(ctx, identity.Key(typ, id))
			if e != nil {
				return trace, e
			}
			if e = projection.Delete(ctx, identity.Key(typ, id), cache.Revision); e != nil {
				return trace, e
			}
			if e = recover(); e != nil {
				return trace, e
			}
		}
		after, e := leasesKV.Get(ctx, identity.Key(typ, id))
		if e != nil || before.Revision != after.Revision || !bytes.Equal(before.Value, after.Value) || handlers != 0 {
			return trace, fmt.Errorf("catalog recovery changed foreign lease or entered handler: %v/%d", e, handlers)
		}
		if e = owner.Renew(ctx); e != nil {
			return trace, e
		}
		if e = owner.Release(ctx); e != nil {
			return trace, e
		}
		records, current, e := graph.Read(ctx, typ, id, h.InvSeq)
		if e != nil || current != tail || len(records) != 2 || !bytes.Equal(records[1].Payload, body) {
			return trace, fmt.Errorf("catalog recovery changed terminal: %v", e)
		}
		if rows, _, e := legacyStore.Read(ctx, typ, id); e != nil || len(rows) != 0 {
			return trace, fmt.Errorf("legacy journal changed: %v", e)
		}
	}
	if cache, e := projection.Get(ctx, identity.Key(typ, id)); e == nil {
		if e = projection.Delete(ctx, identity.Key(typ, id), cache.Revision); e != nil {
			return trace, e
		}
	}
	if mode != "retired" {
		if e = graph.Retire(ctx, typ, id, h.InvSeq, tail); e != nil {
			return trace, e
		}
	}
	transport.PurgeInvocation(identity.InvocationSubject(typ, id))
	if e = schedule.AdvanceMillis(60000); e != nil {
		return trace, e
	}
	if _, e = model.Protocol().SweepWithReaders(ctx, now()); e != nil {
		return trace, e
	}
	objects, e := model.Objects(ctx)
	if e != nil || len(objects) != 0 {
		return trace, fmt.Errorf("terminal catalog graph did not drain: %d %v", len(objects), e)
	}
	if e = model.CheckReferences(); e != nil {
		return trace, e
	}
	if e = transport.Dispatch.CheckDrained(); e != nil {
		return trace, e
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_graph_terminal_catalog", Outcome: mode})
	if e = schedule.Finish(); e != nil {
		return trace, e
	}
	return trace, nil
}

func TestSeededGraphTerminalCatalogReplay(t *testing.T) {
	observed := map[string]int{}
	for seed := range seededSchedules(t) {
		generated, e := runGraphTerminalCatalog(seed, nil)
		fail := func(cause error) {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, e := os.MkdirTemp("", "js-wf-terminal-catalog-failure-")
				if e != nil {
					t.Fatal(e)
				}
				path = filepath.Join(dir, "trace.json")
			}
			if e := generated.Save(path); e != nil {
				t.Fatal(e)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, cause)
		}
		if e != nil {
			fail(e)
		}
		replayed, e := runGraphTerminalCatalog(seed, &generated)
		if e != nil || !reflect.DeepEqual(generated, replayed) {
			fail(fmt.Errorf("catalog replay differs: %v", e))
		}
		mode := generated.Decisions[0].Chosen
		observed[mode]++
		if dir := os.Getenv("SIM_GRAPH_TERMINAL_CATALOG_ROOT"); dir != "" && observed[mode] == 1 {
			if e = generated.Save(filepath.Join(dir, mode+".json")); e != nil {
				t.Fatal(e)
			}
		}
	}
	if len(observed) != len(terminalCatalogModes) {
		t.Fatalf("catalog coverage=%v", observed)
	}
	t.Logf("terminal catalog modes=%v; bounded watermark cycles, sole dispatch discovery, exact projection recovery, repeated deletion inside dedup window, no handler or foreign-lease mutation, complete graph/dispatch drain", observed)
}
