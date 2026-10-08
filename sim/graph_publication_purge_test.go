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

	"github.com/nats-io/nats.go"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/retention"
	"js-wf/wf"
)

var graphPurgeModes = []string{"clean", "missing_mirror", "forged_mirror", "older_tombstone", "failed", "cancelled", "pending", "uninitialized", "bad_terminal", "wrong_generation", "fence_drop", "fence_lost_known", "fence_lost_unknown", "retire_drop", "retire_lost_unknown", "marker_drop", "marker_lost", "signals_drop", "signals_lost", "journal_drop", "timers_drop", "snapshot_drop", "tombstone_drop", "tombstone_lost", "event_lost", "invocation_lost", "held_reader", "reuse"}

type graphPurgeFaultPort struct {
	*PurgeTransport
	graph *GraphPublicationTransport
	mode  string
	armed bool
}

func (p *graphPurgeFaultPort) State(ctx context.Context, key string) (retention.PurgeState, error) {
	value, err := p.PurgeTransport.State(ctx, key)
	if strings.HasPrefix(key, "snap.") && !p.armed && strings.HasPrefix(p.mode, "retire_") {
		p.armed = true
		kind := DropBeforeCommit
		if p.mode == "retire_lost_unknown" {
			kind = LoseAckAfterCommit
			p.graph.PauseBefore("cas_root", func() error { return p.graph.QueueFault("read_root", DropBeforeCommit) })
		}
		if e := p.graph.QueueFault("cas_root", kind); e != nil {
			return value, e
		}
	}
	return value, err
}

// Production purge over prepared canonical histories and the shared retained
// stream/KV model. Reader/fence/retirement operations use the common graph port.
func runGraphPurge(seed int64, replay *Trace) (trace Trace, runErr error) {
	s := NewScheduler(seed)
	if replay != nil {
		var e error
		s, e = ReplayScheduler(*replay)
		if e != nil {
			return trace, e
		}
	}
	if e := s.SetWorkload("graph_canonical_purge"); e != nil {
		return trace, e
	}
	defer func() { trace = s.Trace() }()
	mode, err := s.Choose(graphPurgeModes)
	if err != nil {
		return trace, err
	}
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	m := NewGraphPublicationTransport(s)
	now := func() time.Time { return time.UnixMilli(s.NowMillis()).UTC() }
	graph, err := journal.NewGraphStore(journal.GraphConfig{Protocol: m.Protocol(), Now: now, PinTTL: 30 * time.Second, IntentTTL: time.Second})
	if err != nil {
		return trace, err
	}
	transport := NewPurgeTransport(s)
	transport.EnableFallbackTimers()
	port := &graphPurgeFaultPort{PurgeTransport: transport, graph: m, mode: mode}
	const typ, id = "test", "purge"
	key := identity.Key(typ, id)
	if _, err = transport.Blobs.PublishSubject("WF_INV", "wf.inv.test.other", nil, []byte(`0`)); err != nil {
		return trace, err
	}
	invocation, err := transport.Blobs.PublishSubject("WF_INV", identity.InvocationSubject(typ, id), nil, []byte(`7`))
	if err != nil {
		return trace, err
	}
	generation := invocation
	if mode == "wrong_generation" {
		generation++
	}
	var tail uint64
	if mode != "uninitialized" {
		tail, err = graph.Begin(ctx, typ, id, generation)
		if err != nil {
			return trace, err
		}
		tail, err = graph.Append(ctx, typ, id, generation, journal.Entry{Kind: journal.Started}, tail, nil, nil)
		if err != nil {
			return trace, err
		}
	}
	result := []byte(`"external result"`)
	outcome := wf.Outcome{InvSeq: generation, ResultRef: "terminal-" + digest(result), ResultHash: digest(result)}
	kind := journal.Completed
	if mode == "failed" || mode == "cancelled" {
		kind = journal.Failed
		outcome = wf.Outcome{InvSeq: generation, Error: "failure"}
		if mode == "cancelled" {
			outcome.Error = "workflow cancelled"
		}
	}
	if mode == "bad_terminal" {
		outcome.InvSeq++
	}
	terminal, _ := json.Marshal(outcome)
	if mode != "pending" && mode != "uninitialized" {
		var payloads [][]byte
		if kind == journal.Completed {
			payloads = [][]byte{result}
		}
		tail, err = graph.Append(ctx, typ, id, generation, journal.Entry{Kind: kind, Index: 1, Payload: terminal}, tail, payloads, nil)
		if err != nil {
			return trace, err
		}
	}
	// Misleading compatibility state never selects graph terminal bytes.
	if mode != "missing_mirror" {
		mirror := terminal
		if mode == "forged_mirror" || mode == "pending" {
			mirror = []byte(`{"inv_seq":999,"result":"OTk="}`)
		}
		if mode == "older_tombstone" {
			mirror, _ = json.Marshal(retention.Tombstone{Tombstone: true, InvSeq: 1, PurgedAt: now(), ExpiresAt: now().Add(time.Minute)})
		}
		if err = transport.PutState(ctx, key, mirror); err != nil {
			return trace, err
		}
	}
	if err = transport.PutState(ctx, "snap."+key, []byte(`{}`)); err != nil {
		return trace, err
	}
	if _, err = transport.Blobs.PublishSubject("WF_SIG", "wf.sig.test.purge.go", nil, []byte(`42`)); err != nil {
		return trace, err
	}
	if _, err = transport.Blobs.PublishSubject("WF_TIMER", identity.TimerSubject(typ, id, invocation, 1), nil, []byte(`{}`)); err != nil {
		return trace, err
	}
	if _, err = transport.Blobs.PublishSubject("WF_RUN", "wf.schedule.test.purge.1", nats.Header{identity.TimerInvSeqHeader: {fmt.Sprint(invocation)}, identity.TimerStepHeader: {"1"}}, []byte(key)); err != nil {
		return trace, err
	}
	// A later generation's retained hint must survive the old purge.
	if _, err = transport.Blobs.PublishSubject("WF_RUN", "wf.schedule.test.purge.2", nats.Header{identity.TimerInvSeqHeader: {fmt.Sprint(invocation + 1)}, identity.TimerStepHeader: {"2"}}, []byte(key)); err != nil {
		return trace, err
	}
	var held *journal.GraphView
	if mode == "held_reader" {
		held, err = graph.OpenTerminal(ctx, typ, id, generation)
		if err != nil {
			return trace, err
		}
	}
	fault := false
	known := false
	switch mode {
	case "fence_drop", "fence_lost_known", "fence_lost_unknown":
		fault = mode != "fence_lost_known"
		known = !fault
		m.PauseBefore("get", func() error {
			f := DropBeforeCommit
			if mode != "fence_drop" {
				f = LoseAckAfterCommit
			}
			if mode == "fence_lost_unknown" {
				m.PauseBefore("cas_root", func() error { return m.QueueFault("read_root", DropBeforeCommit) })
			}
			return m.QueueFault("cas_root", f)
		})
	case "retire_drop", "retire_lost_unknown":
		fault = true
	case "marker_drop", "marker_lost":
		f := KVDropBeforeCommit
		if mode == "marker_lost" {
			f = KVLoseAckAfterCommit
		}
		err = transport.Blobs.State().QueueFault(KVFault{Operation: "put", Kind: f})
		fault = true
	case "signals_drop", "signals_lost", "journal_drop", "timers_drop":
		stream := "WF_SIG"
		if mode == "journal_drop" {
			stream = "WF_JRN"
		}
		if mode == "timers_drop" {
			stream = "WF_TIMER"
		}
		f := DropBeforeCommit
		if mode == "signals_lost" {
			f = LoseAckAfterCommit
		}
		err = transport.QueueFault(PurgeFault{Operation: "purge_" + stream, Kind: f})
		fault = true
	case "snapshot_drop":
		err = transport.Blobs.State().QueueFault(KVFault{Operation: "delete", Kind: KVDropBeforeCommit})
		fault = true
	case "tombstone_drop", "tombstone_lost":
		f := KVDropBeforeCommit
		if mode == "tombstone_lost" {
			f = KVLoseAckAfterCommit
		}
		err = transport.Blobs.State().QueueFault(KVFault{Operation: "update", Kind: f})
		fault = mode == "tombstone_drop"
		known = !fault
	case "event_lost":
		err = transport.QueueFault(PurgeFault{Operation: "publish_purge", Kind: LoseAckAfterCommit})
		fault = true
	case "invocation_lost":
		err = transport.QueueFault(PurgeFault{Operation: "purge_WF_INV", Kind: LoseAckAfterCommit})
		fault = true
	}
	if err != nil {
		return trace, err
	}
	purge := func() error {
		return retention.PurgeGraphInvocationWithPort(ctx, port, graph, typ, id, invocation, time.Minute)
	}
	purgeErr := purge()
	negative := mode == "pending" || mode == "uninitialized" || mode == "bad_terminal" || mode == "wrong_generation"
	if negative {
		want := retention.ErrNotTerminal
		if mode == "bad_terminal" {
			want = wf.ErrCorruptJournal
		}
		if mode == "wrong_generation" {
			want = journal.ErrStale
		}
		if !errors.Is(purgeErr, want) {
			return trace, fmt.Errorf("invalid source authorized purge: %v want%v", purgeErr, want)
		}
		status, e := graph.InspectRetirement(ctx, typ, id)
		if e != nil || status.Purging || status.Retired {
			return trace, fmt.Errorf("negative source fenced: %+v %v", status, e)
		}
		if _, e = port.Invocation(ctx, identity.InvocationSubject(typ, id)); e != nil {
			return trace, e
		}
		// Explicitly retire this failed fixture; this is not a successful purge.
		if mode == "pending" {
			body, _ := json.Marshal(wf.Outcome{InvSeq: generation, Error: "fixture cleanup"})
			tail, err = graph.Append(ctx, typ, id, generation, journal.Entry{Kind: journal.Failed, Index: 1, Payload: body}, tail, nil, nil)
			if err != nil {
				return trace, err
			}
		}
		if mode != "uninitialized" {
			if err = graph.Retire(ctx, typ, id, generation, tail); err != nil {
				return trace, err
			}
		}
	} else {
		if fault {
			if purgeErr == nil {
				return trace, fmt.Errorf("uncertain purge reported success for %s", mode)
			}
			if err = purge(); err != nil {
				return trace, fmt.Errorf("purge retry %s: %w", mode, err)
			}
		} else if purgeErr != nil {
			return trace, fmt.Errorf("purge %s: %w", mode, purgeErr)
		}
		if err = purge(); err != nil {
			return trace, fmt.Errorf("idempotent purge: %w", err)
		}
		status, e := graph.InspectRetirement(ctx, typ, id)
		if e != nil || !status.Purging || !status.Retired || status.Invocation != invocation {
			return trace, fmt.Errorf("purge did not seal canonical cursor: %+v %v", status, e)
		}
		if view, e := graph.OpenTerminal(ctx, typ, id, invocation); view != nil || e != journal.ErrStale {
			return trace, fmt.Errorf("purged generation reopened: %v", e)
		}
		state, e := port.State(ctx, key)
		if e != nil {
			return trace, e
		}
		marker, tomb, e := retention.Decode(state.Value)
		if e != nil || !tomb || marker.InvSeq != invocation {
			return trace, fmt.Errorf("tombstone differs")
		}
		if _, e = port.Invocation(ctx, identity.InvocationSubject(typ, id)); e == nil {
			return trace, fmt.Errorf("invocation survived purge")
		}
		if held != nil {
			verified, e := wf.ReadGraphTerminal(ctx, held, invocation, 1000)
			if e != nil || !bytes.Equal(verified.Result, result) {
				return trace, fmt.Errorf("held reader lost bytes: %v", e)
			}
			if e = held.Close(ctx); e != nil {
				return trace, e
			}
		}
		if mode == "reuse" {
			newGeneration, e := transport.Blobs.PublishSubject("WF_INV", identity.InvocationSubject(typ, id), nil, []byte(`8`))
			if e != nil {
				return trace, e
			}
			if _, e = graph.Begin(ctx, typ, id, newGeneration); e != nil {
				return trace, e
			}
			if _, e = transport.Blobs.PublishSubject("WF_SIG", "wf.sig.test.purge.go", nil, []byte(`new generation`)); e != nil {
				return trace, e
			}
			if e = purge(); e != journal.ErrStale {
				return trace, fmt.Errorf("old target crossed new invocation: %v", e)
			}
			transport.Blobs.mu.Lock()
			signals := len(transport.Blobs.streams["WF_SIG"].messages)
			transport.Blobs.mu.Unlock()
			if signals != 1 {
				return trace, fmt.Errorf("old target removed replacement signal")
			}

		}
	}
	transport.Blobs.mu.Lock()
	nativeHints := len(transport.Blobs.streams["WF_RUN"].messages)
	transport.Blobs.mu.Unlock()
	if !negative && nativeHints != 1 {
		return trace, fmt.Errorf("native hint generation isolation failed: %d", nativeHints)
	}
	if known {
		found := false
		for _, event := range s.Trace().Transport {
			if event.Outcome == string(LoseAckAfterCommit) || event.Outcome == string(KVLoseAckAfterCommit) {
				found = true
			}
		}
		if !found {
			return trace, fmt.Errorf("known lost-reply control not injected")
		}
	}
	if err = s.AdvanceMillis(60000); err != nil {
		return trace, err
	}
	if _, err = m.Protocol().SweepWithReaders(ctx, now()); err != nil {
		return trace, err
	}
	objects, err := m.Objects(ctx)
	if err != nil || len(objects) != 0 {
		return trace, fmt.Errorf("graph fixture did not drain: %d %v", len(objects), err)
	}
	if err = m.CheckReferences(); err != nil {
		return trace, err
	}
	if err = ctx.Err(); err != nil {
		return trace, err
	}
	s.RecordTransport(TransportEvent{Operation: "check_graph_purge", Outcome: mode})
	if err = s.Finish(); err != nil {
		return trace, err
	}
	return trace, nil
}

func TestSeededGraphPurgeReplay(t *testing.T) {
	observed := map[string]int{}
	fail := func(seed int64, trace Trace, cause error) {
		path := os.Getenv("FAULT_TRACE_OUT")
		if path == "" {
			dir, e := os.MkdirTemp("", "js-wf-graph-purge-failure-")
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
		generated, e := runGraphPurge(seed, nil)
		if e != nil {
			fail(seed, generated, e)
		}
		mode := generated.Decisions[0].Chosen
		observed[mode]++
		replayed, e := runGraphPurge(seed, &generated)
		if e != nil || !reflect.DeepEqual(generated, replayed) {
			fail(seed, generated, fmt.Errorf("purge replay differs: %v", e))
		}
		if dir := os.Getenv("SIM_GRAPH_PURGE_ROOT"); dir != "" && observed[mode] == 1 {
			if e = generated.Save(filepath.Join(dir, mode+".json")); e != nil {
				t.Fatal(e)
			}
		}
	}
	if len(observed) != len(graphPurgeModes) {
		t.Fatalf("coverage=%v", observed)
	}
	t.Logf("graph purge modes=%v; canonical fence and retirement, conservative recovery, retained readers, generation-bound reuse and graph drain", observed)
}
