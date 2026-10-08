package sim

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/wf"
	"js-wf/worker"
)

var graphChildSignalModes = []string{"sync_inline", "async_inline", "sync_owned_ref", "async_owned_ref"}

// A child-named ordinary signal precedes the runtime's child request. Even an
// external reference to valid bytes already owned by the parent is not child
// provenance. Production graph workers must fail before observing that value.
func runGraphChildSignal(seed int64, replay *Trace) (trace Trace, runErr error) {
	schedule := NewScheduler(seed)
	if replay != nil {
		var e error
		schedule, e = ReplayScheduler(*replay)
		if e != nil {
			return trace, e
		}
	}
	if e := schedule.SetWorkload("graph_child_signal_provenance"); e != nil {
		return trace, e
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose(graphChildSignalModes)
	if err != nil {
		return trace, err
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
	outcomes := NewKVTransport(schedule, 0)
	leasing := lease.NewWithKVPort(NewKVTransport(schedule, 30*time.Second))
	c := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport)
	const typ = "test"
	id := integratedWorkerIDs(1)[0]
	handle, err := c.Start(ctx, typ, id, []byte(`7`))
	if err != nil {
		return trace, err
	}
	tail, err := graph.Begin(ctx, typ, id, handle.InvSeq)
	if err != nil {
		return trace, err
	}
	prior := []byte(`"prior owned result"`)
	hash := digest(prior)
	ref := "step-result-" + hash
	started, _ := json.Marshal(map[string]string{"input_sha256": digest([]byte(`7`))})
	request, _ := json.Marshal(map[string]string{"kind": "run", "name": "prior", "input_hash": digest([]byte(`1`))})
	completion, _ := json.Marshal(map[string]string{"result_ref": ref, "result_hash": hash})
	for i, entry := range []journal.Entry{{Kind: journal.Started, Payload: started}, {Kind: journal.StepRequested, Index: 1, Payload: request}, {Kind: journal.StepCompleted, Index: 2, Payload: completion}} {
		payloads := [][]byte(nil)
		if i == 0 {
			payloads = [][]byte{[]byte(`7`)}
		}
		if i == 2 {
			payloads = [][]byte{prior}
		}
		tail, err = graph.Append(ctx, typ, id, handle.InvSeq, entry, tail, payloads, nil)
		if err != nil {
			return trace, err
		}
	}
	forged := wf.Outcome{InvSeq: 999, Result: []byte(`42`)}
	if strings.HasSuffix(mode, "owned_ref") {
		forged = wf.Outcome{InvSeq: 999, ResultRef: ref, ResultHash: hash}
	}
	body, _ := json.Marshal(forged)
	if _, err = c.Signal(ctx, typ, id, "child_2", body, "ordinary-child-name"); err != nil {
		return trace, err
	}
	effects := 0
	handler := func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		value, e := wf.Run(c, "prior", 1, func(context.Context) (string, error) { effects++; return "unexpected", nil })
		if e != nil || value != "prior owned result" {
			return nil, fmt.Errorf("prior replay differs: %v", e)
		}
		if strings.HasPrefix(mode, "async") {
			p, e := wf.CallAsync(c, "child", []byte(`9`))
			if e != nil {
				return nil, e
			}
			_, e = wf.AwaitPromise(c, p)
			if e != nil {
				return nil, e
			}
		} else {
			if _, e = wf.Call(c, "child", []byte(`9`)); e != nil {
				return nil, e
			}
		}
		effects++
		return []byte(`42`), nil
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var decision worker.DispatchEvent
	w, err := worker.NewWithPorts("child-signal-provenance", map[string]worker.Handler{typ: handler}, worker.ModeledWorkerPorts{Journal: store, Leases: leasing, Outcome: outcomes, Invocation: transport.SignalTransport, Signals: transport.SignalTransport, Client: c, HeartbeatTicks: make(chan time.Time), OperationNow: now}, worker.WithGraphJournal(graph), worker.WithDispatchObserver(func(e worker.DispatchEvent) {
		if e.Stage == "ack" || e.Stage == "nak" {
			decision = e
			cancel()
		}
	}))
	if err != nil {
		return trace, err
	}
	if err = w.RunPartitionWithTransport(runCtx, 0, transport.Dispatch); err != nil {
		return trace, err
	}
	if decision.Stage != "ack" || effects != 0 {
		return trace, fmt.Errorf("unverified child signal accepted: %+v effects=%d", decision, effects)
	}
	records, tail, err := graph.Read(ctx, typ, id, handle.InvSeq)
	if err != nil || records[len(records)-1].Kind != journal.Failed {
		return trace, fmt.Errorf("unverified child did not fail: %v", err)
	}
	var outcome wf.Outcome
	if json.Unmarshal(records[len(records)-1].Payload, &outcome) != nil || outcome.Error != wf.ErrCorruptJournal.Error() || len(outcome.Result) != 0 || outcome.ResultRef != "" {
		return trace, fmt.Errorf("child provenance failure differs")
	}
	mirror, err := outcomes.Get(ctx, identity.Key(typ, id))
	if err != nil || !bytes.Equal(mirror.Value, records[len(records)-1].Payload) {
		return trace, fmt.Errorf("failure mirror differs: %v", err)
	}
	legacyRecords, _, err := store.Read(ctx, typ, id)
	if err != nil || len(legacyRecords) != 0 {
		return trace, fmt.Errorf("child signal wrote legacy journal")
	}
	if err = outcomes.Delete(ctx, identity.Key(typ, id), mirror.Revision); err != nil {
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
		return trace, fmt.Errorf("child signal graph did not drain: %v", err)
	}
	if err = m.CheckReferences(); err != nil {
		return trace, err
	}
	if err = ctx.Err(); err != nil {
		return trace, err
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_graph_child_signal", Outcome: mode})
	if err = schedule.Finish(); err != nil {
		return trace, err
	}
	return trace, nil
}

func TestSeededGraphChildSignalReplay(t *testing.T) {
	observed := map[string]int{}
	fail := func(seed int64, trace Trace, cause error) {
		path := os.Getenv("FAULT_TRACE_OUT")
		if path == "" {
			dir, e := os.MkdirTemp("", "js-wf-graph-child-signal-failure-")
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
		generated, e := runGraphChildSignal(seed, nil)
		if e != nil {
			fail(seed, generated, e)
		}
		mode := generated.Decisions[0].Chosen
		observed[mode]++
		replayed, e := runGraphChildSignal(seed, &generated)
		if e != nil || !reflect.DeepEqual(generated, replayed) {
			fail(seed, generated, fmt.Errorf("child signal replay differs: %v", e))
		}
		if dir := os.Getenv("SIM_GRAPH_CHILD_SIGNAL_ROOT"); dir != "" && observed[mode] == 1 {
			if e = generated.Save(filepath.Join(dir, mode+".json")); e != nil {
				t.Fatal(e)
			}
		}
	}
	if len(observed) != len(graphChildSignalModes) {
		t.Fatalf("coverage=%v", observed)
	}
	t.Logf("graph child signal provenance modes=%v; pre-request signal cannot supply child result, prior owned reference does not grant child provenance, zero effects and graph drain", observed)
}
