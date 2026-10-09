package sim

import (
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
	"js-wf/reconcile"
)

var graphReconcileModes = []string{
	"start_missing", "start_initialized", "start_existing", "signal_pending", "signal_consumed", "signal_terminal",
	"timer_due", "timer_future", "timer_terminal", "suspended_signal", "suspended_timer", "suspended_terminal", "suspended_no_signal",
	"read_unknown", "pin_lost_known", "release_lost_known", "pin_unknown", "payload_unknown", "release_unknown", "stale_generation", "retired_generation", "enqueue_unknown",
	"dry_start", "dry_signal", "dry_timer", "dry_suspended",
}

// Real scanner decisions over prepared canonical histories, with misleading
// legacy histories. This fixture retains repair wakeups; it does not execute a
// worker or certify cross-store invocation purge coordination.
func runGraphReconcile(seed int64, replay *Trace) (trace Trace, runErr error) {
	s := NewScheduler(seed)
	if replay != nil {
		var e error
		s, e = ReplayScheduler(*replay)
		if e != nil {
			return trace, e
		}
	}
	if e := s.SetWorkload("graph_reconciler_history"); e != nil {
		return trace, e
	}
	defer func() { trace = s.Trace() }()
	mode, err := s.Choose(graphReconcileModes)
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
	port := NewSignalTransport(s)
	const typ, id = "test", "graph-reconcile"
	invocation, err := port.PublishInvocation(ctx, &nats.Msg{Subject: identity.InvocationSubject(typ, id), Data: []byte(`7`), Header: nats.Header{}})
	if err != nil {
		return trace, err
	}
	// If a scanner accidentally falls back, it will suppress a required repair.
	port.MarkJournal(typ, id)
	port.SetJournal(typ, id, []journal.Record{{Entry: journal.Entry{Kind: journal.Completed}}})
	kind := "signal"
	switch {
	case strings.HasPrefix(mode, "start"), mode == "dry_start":
		kind = "start"
	case strings.HasPrefix(mode, "timer"), mode == "dry_timer":
		kind = "timer"
	case strings.HasPrefix(mode, "suspended"), mode == "dry_suspended":
		kind = "suspended"
	}
	missing := mode == "start_missing" || mode == "dry_start"
	generation := invocation
	if mode == "stale_generation" {
		generation++
	}
	var entries []journal.Entry
	var tail uint64
	appendEntry := func(entry journal.Entry) error {
		entry.Index = uint64(len(entries))
		var e error
		tail, e = graph.Append(ctx, typ, id, generation, entry, tail, nil, nil)
		if e == nil {
			entries = append(entries, entry)
		}
		return e
	}
	if !missing {
		tail, err = graph.Begin(ctx, typ, id, generation)
		if err != nil {
			return trace, err
		}
		if mode != "start_initialized" {
			if err = appendEntry(journal.Entry{Kind: journal.Started}); err != nil {
				return trace, err
			}
		}
	}
	var signal uint64
	if kind == "signal" || kind == "suspended" && mode != "suspended_no_signal" {
		signal = port.CommitSignal(&nats.Msg{Subject: "wf.sig." + typ + "." + id + ".go", Data: []byte(`42`), Header: nats.Header{"Wf-Inv-Seq": {fmt.Sprint(invocation)}}})
	}
	if kind == "timer" || kind == "suspended" {
		var request []byte
		waiting := "signal:go"
		if kind == "timer" || mode == "suspended_timer" || mode == "dry_suspended" {
			fire := now().Add(-time.Second)
			if mode == "timer_future" {
				fire = now().Add(time.Hour)
			}
			request, _ = json.Marshal(map[string]any{"kind": "timer", "name": "wake", "fire_at": fire})
			waiting = "timer:wake"
		} else {
			request = []byte(`{"kind":"signal","name":"go"}`)
		}
		if err = appendEntry(journal.Entry{Kind: journal.StepRequested, Payload: request}); err != nil {
			return trace, err
		}
		if kind == "suspended" {
			body, _ := json.Marshal(map[string]string{"waiting_on": waiting})
			if err = appendEntry(journal.Entry{Kind: journal.Suspended, Payload: body}); err != nil {
				return trace, err
			}
		}
	}
	if mode == "signal_consumed" {
		body, _ := json.Marshal(map[string]any{"sig_seq": signal, "name": "go", "data": []byte(`42`)})
		if err = appendEntry(journal.Entry{Kind: journal.SignalConsumed, Payload: body}); err != nil {
			return trace, err
		}
	}
	terminal := strings.HasSuffix(mode, "_terminal") || mode == "retired_generation"
	if terminal {
		if err = appendEntry(journal.Entry{Kind: journal.Failed, Payload: []byte(`{}`)}); err != nil {
			return trace, err
		}
	}
	if mode == "retired_generation" {
		if err = graph.Retire(ctx, typ, id, generation, tail); err != nil {
			return trace, err
		}
	}
	fault := false
	switch mode {
	case "read_unknown":
		err = m.QueueFault("read_root", DropBeforeCommit)
		fault = true
	case "pin_lost_known":
		err = m.QueueFault("cas_root", LoseAckAfterCommit)
	case "release_lost_known":
		m.PauseBefore("get", func() error { return m.QueueFault("cas_root", LoseAckAfterCommit) })
	case "pin_unknown":
		err = m.QueueFault("cas_root", LoseAckAfterCommit)
		m.PauseBefore("cas_root", func() error { return m.QueueFault("read_root", DropBeforeCommit) })
		fault = true
	case "payload_unknown":
		err = m.QueueFault("get", DropBeforeCommit)
		fault = true
	case "release_unknown":
		// Install the lost CAS reply only once the pinned entry read starts.
		m.PauseBefore("get", func() error {
			if e := m.QueueFault("cas_root", LoseAckAfterCommit); e != nil {
				return e
			}
			m.PauseBefore("cas_root", func() error { return m.QueueFault("read_root", DropBeforeCommit) })
			return nil
		})
		fault = true
	case "enqueue_unknown":
		err = port.QueueFault(StartFault{Operation: "enqueue_run", Kind: "lose_ack_after_commit"})
		fault = true
	}
	if err != nil {
		return trace, err
	}
	var scan func(context.Context, uint64, int, bool) (reconcile.ScanResult, error)
	switch kind {
	case "start":
		q, e := reconcile.NewStartScanWithGraphJournalPort(port, graph)
		err = e
		if q != nil {
			scan = q.Scan
		}
	case "signal":
		q, e := reconcile.NewSignalScanWithGraphJournalPort(port, graph)
		err = e
		if q != nil {
			scan = q.Scan
		}
	case "timer":
		q, e := reconcile.NewTimerScanWithGraphJournalPort(port, graph)
		err = e
		if q != nil {
			q.Now = now
			scan = q.Scan
		}
	case "suspended":
		q, e := reconcile.NewSuspendedScanWithGraphJournalPort(port, graph)
		err = e
		if q != nil {
			q.Now = now
			q.Grace = 0
			scan = q.Scan
		}
	}
	if err != nil {
		return trace, err
	}
	dry := strings.HasPrefix(mode, "dry_")
	result, scanErr := scan(ctx, 1, 1, dry)
	stale := mode == "stale_generation" || mode == "retired_generation"
	if stale {
		if !errors.Is(scanErr, journal.ErrStale) || !errors.Is(scanErr, journal.ErrUnknown) || len(port.Runs()) != 0 || result.RetrySequence != 0 {
			return trace, fmt.Errorf("stale history authorized repair: %+v %v", result, scanErr)
		}
	} else if fault {
		if scanErr == nil || result.RetrySequence != 0 {
			return trace, fmt.Errorf("uncertainty skipped unresolved boundary: %+v %v", result, scanErr)
		}
		if mode != "enqueue_unknown" && len(port.Runs()) != 0 {
			return trace, fmt.Errorf("unknown history enqueued")
		}
		if mode == "enqueue_unknown" && len(port.Runs()) != 1 {
			return trace, fmt.Errorf("lost enqueue did not commit")
		}
		result, scanErr = scan(ctx, 1, 1, false)
		if scanErr != nil || result.Reenqueued != 1 || len(port.Runs()) != 1 {
			return trace, fmt.Errorf("repair retry differs: %+v %v", result, scanErr)
		}
	} else {
		want := 1
		if mode == "start_existing" || mode == "signal_consumed" || terminal || mode == "timer_future" || mode == "suspended_no_signal" {
			want = 0
		}
		if scanErr != nil || result.Reenqueued != want {
			return trace, fmt.Errorf("repair count want%d got%+v err%v", want, result, scanErr)
		}
		wantRuns := want
		if dry {
			wantRuns = 0
		}
		if len(port.Runs()) != wantRuns {
			return trace, fmt.Errorf("repair publication count differs")
		}
	}
	lostKnownObserved := false
	for _, e := range s.Trace().Transport {
		if e.Operation == "graph_publication_cas_root" && e.Outcome == string(LoseAckAfterCommit) {
			lostKnownObserved = true
		}
		if e.Operation == "read_journal" || e.Operation == "journal_exists" {
			return trace, fmt.Errorf("legacy journal fallback")
		}
	}
	if (mode == "pin_lost_known" || mode == "release_lost_known") && !lostKnownObserved {
		return trace, fmt.Errorf("lost reply control was not injected")
	}
	// Complete and retire this prepared fixture explicitly, then expire any
	// pin retained by a lost reply. Wakeups deliberately remain pending.
	if !missing {
		if len(entries) == 0 {
			if err = appendEntry(journal.Entry{Kind: journal.Started}); err != nil {
				return trace, err
			}
		}
		if !terminal {
			if err = appendEntry(journal.Entry{Kind: journal.Failed, Payload: []byte(`{}`)}); err != nil {
				return trace, err
			}
		}
		port.PurgeInvocation(identity.InvocationSubject(typ, id))
		if err = graph.Retire(ctx, typ, id, generation, tail); err != nil {
			return trace, err
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
	s.RecordTransport(TransportEvent{Operation: "check_graph_reconcile", Outcome: mode})
	if err = s.Finish(); err != nil {
		return trace, err
	}
	return trace, nil
}

func TestSeededGraphReconcileReplay(t *testing.T) {
	observed := map[string]int{}
	fail := func(seed int64, trace Trace, cause error) {
		path := os.Getenv("FAULT_TRACE_OUT")
		if path == "" {
			dir, e := os.MkdirTemp("", "js-wf-graph-reconcile-failure-")
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
		generated, e := runGraphReconcile(seed, nil)
		if e != nil {
			fail(seed, generated, e)
		}
		mode := generated.Decisions[0].Chosen
		observed[mode]++
		replayed, e := runGraphReconcile(seed, &generated)
		if e != nil || !reflect.DeepEqual(generated, replayed) {
			fail(seed, generated, fmt.Errorf("graph reconcile replay differs: %v", e))
		}
		if dir := os.Getenv("SIM_GRAPH_RECONCILE_ROOT"); dir != "" && observed[mode] == 1 {
			if e = generated.Save(filepath.Join(dir, mode+".json")); e != nil {
				t.Fatal(e)
			}
		}
	}
	if len(observed) != len(graphReconcileModes) {
		t.Fatalf("coverage=%v", observed)
	}
	t.Logf("graph reconcile modes=%v; canonical history, no legacy fallback, conservative uncertainty/cursors, graph drain after explicit fixture cleanup", observed)
}
