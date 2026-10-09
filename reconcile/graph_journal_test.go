package reconcile_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
	"js-wf/internal/graphpublication"
	"js-wf/journal"
	"js-wf/reconcile"
	"js-wf/sim"
)

type changingGraphSignalPort struct {
	*sim.SignalTransport
	change func() error
}

func (p *changingGraphSignalPort) GetSignal(ctx context.Context, seq uint64) (*jetstream.RawStreamMsg, error) {
	if seq == 2 && p.change != nil {
		change := p.change
		p.change = nil
		if err := change(); err != nil {
			return nil, err
		}
	}
	return p.SignalTransport.GetSignal(ctx, seq)
}

func TestGraphReconcileSignalCacheBindsInvocationGeneration(t *testing.T) {
	ctx := context.Background()
	s := sim.NewScheduler(1)
	m := sim.NewGraphPublicationTransport(s)
	graph, err := journal.NewGraphStore(journal.GraphConfig{Protocol: m.Protocol()})
	if err != nil {
		t.Fatal(err)
	}
	transport := sim.NewSignalTransport(s)
	const typ, id = "test", "reuse"
	publish := func() (uint64, error) {
		return transport.PublishInvocation(ctx, &nats.Msg{Subject: identity.InvocationSubject(typ, id), Header: nats.Header{}, Data: []byte(`7`)})
	}
	first, err := publish()
	if err != nil {
		t.Fatal(err)
	}
	tail, err := graph.Begin(ctx, typ, id, first)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range []journal.Entry{{Kind: journal.Started}, {Kind: journal.Failed, Index: 1}} {
		tail, err = graph.Append(ctx, typ, id, first, e, tail, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, gen := range []uint64{first, first + 1} {
		transport.CommitSignal(&nats.Msg{Subject: "wf.sig.test.reuse.go", Data: []byte(`42`), Header: nats.Header{"Wf-Inv-Seq": {fmt.Sprint(gen)}}})
	}
	port := &changingGraphSignalPort{SignalTransport: transport, change: func() error {
		if e := graph.Retire(ctx, typ, id, first, tail); e != nil {
			return e
		}
		transport.PurgeInvocation(identity.InvocationSubject(typ, id))
		second, e := publish()
		if e != nil {
			return e
		}
		tail, e = graph.Begin(ctx, typ, id, second)
		if e != nil {
			return e
		}
		_, e = graph.Append(ctx, typ, id, second, journal.Entry{Kind: journal.Started}, tail, nil, nil)
		return e
	}}
	scan, err := reconcile.NewSignalScanWithGraphJournalPort(port, graph)
	if err != nil {
		t.Fatal(err)
	}
	result, err := scan.Scan(ctx, 1, 2, false)
	if err != nil || result.Reenqueued != 1 || result.Inspected != 2 || len(transport.Runs()) != 1 {
		t.Fatalf("new generation suppressed by old terminal cache: %+v %v", result, err)
	}
}

func TestGraphReconcileRejectsMissingConfiguration(t *testing.T) {
	port := sim.NewSignalTransport(sim.NewScheduler(1))
	checks := []error{}
	_, e := reconcile.NewStartScanWithGraphJournalPort(port, nil)
	checks = append(checks, e)
	_, e = reconcile.NewSignalScanWithGraphJournalPort(port, nil)
	checks = append(checks, e)
	_, e = reconcile.NewTimerScanWithGraphJournalPort(port, nil)
	checks = append(checks, e)
	_, e = reconcile.NewSuspendedScanWithGraphJournalPort(port, nil)
	checks = append(checks, e)
	graph, e := journal.NewGraphStore(journal.GraphConfig{Protocol: sim.NewGraphPublicationTransport(sim.NewScheduler(2)).Protocol()})
	if e != nil {
		t.Fatal(e)
	}
	_, e = reconcile.NewStartScanWithGraphJournalPort(nil, graph)
	checks = append(checks, e)
	_, e = reconcile.NewSignalScanWithGraphJournalPort(nil, graph)
	checks = append(checks, e)
	_, e = reconcile.NewTimerScanWithGraphJournalPort(nil, graph)
	checks = append(checks, e)
	_, e = reconcile.NewSuspendedScanWithGraphJournalPort(nil, graph)
	checks = append(checks, e)
	checks = append(checks, reconcile.RunRepairLoopWithGraphJournal(context.Background(), nil, "test", "start", time.Second, 1, nil, nil, nil, nil))
	checks = append(checks, reconcile.RunRepairLoopWithGraphJournal(context.Background(), nil, "test", "tombstone", time.Second, 1, graph, nil, nil, nil))
	for i, e := range checks {
		if e == nil {
			t.Fatalf("configuration%d accepted", i)
		}
	}
}

// The legacy port must never become a fallback after an uncertain graph read.
type forbiddenGraphLegacyPort struct{ *sim.SignalTransport }

func (p forbiddenGraphLegacyPort) ReadJournal(context.Context, string, string) ([]journal.Record, error) {
	return nil, errors.New("legacy read forbidden")
}
func (p forbiddenGraphLegacyPort) JournalExists(context.Context, string) (bool, error) {
	return false, errors.New("legacy existence forbidden")
}

func TestGraphReconcileFailedReadCannotCertifyCursorPrefix(t *testing.T) {
	for _, kind := range []string{"start", "signal", "timer", "suspended"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			s := sim.NewScheduler(1)
			m := sim.NewGraphPublicationTransport(s)
			graph, e := journal.NewGraphStore(journal.GraphConfig{Protocol: m.Protocol()})
			if e != nil {
				t.Fatal(e)
			}
			transport := sim.NewSignalTransport(s)
			port := forbiddenGraphLegacyPort{transport}
			for i := 1; i <= 2; i++ {
				id := fmt.Sprintf("prefix%d", i)
				gen, e := transport.PublishInvocation(ctx, &nats.Msg{Subject: identity.InvocationSubject("test", id), Header: nats.Header{}})
				if e != nil {
					t.Fatal(e)
				}
				tail, e := graph.Begin(ctx, "test", id, gen)
				if e != nil {
					t.Fatal(e)
				}
				tail, e = graph.Append(ctx, "test", id, gen, journal.Entry{Kind: journal.Started}, tail, nil, nil)
				if e != nil {
					t.Fatal(e)
				}
				if i == 1 {
					_, e = graph.Append(ctx, "test", id, gen, journal.Entry{Kind: journal.Failed, Index: 1}, tail, nil, nil)
					if e != nil {
						t.Fatal(e)
					}
				}
				transport.CommitSignal(&nats.Msg{Subject: "wf.sig.test." + id + ".go", Header: nats.Header{"Wf-Inv-Seq": {fmt.Sprint(gen)}}})
			}
			// Queue the failure when the second graph destination is observed,
			// independent of the suspended scanner's parallel inspection order.
			_, _, e = graph.Read(ctx, "test", "prefix1", 1)
			if e != nil {
				t.Fatal(e)
			}
			// Hash-bound root keys are discovered once from the model's catalog.
			keys, e := m.RootKeys(ctx)
			if e != nil || len(keys) != 2 {
				t.Fatal(keys, e)
			}
			// Select the second destination using a recording read before injection.
			before := len(s.Trace().Transport)
			_, _, e = graph.Read(ctx, "test", "prefix2", 2)
			if e != nil {
				t.Fatal(e)
			}
			var second string
			for _, ev := range s.Trace().Transport[before:] {
				if ev.Operation == "graph_publication_read_root" {
					second = ev.Subject
					break
				}
			}
			if second == "" {
				t.Fatal("second root not found")
			}
			wrapped := &faultRootPort{Port: m, subject: second}
			graph, e = journal.NewGraphStore(journal.GraphConfig{Protocol: wrapped.protocol(m)})
			if e != nil {
				t.Fatal(e)
			}
			var scan func(context.Context, uint64, int, bool) (reconcile.ScanResult, error)
			switch kind {
			case "start":
				q, e := reconcile.NewStartScanWithGraphJournalPort(port, graph)
				if e != nil {
					t.Fatal(e)
				}
				scan = q.Scan
			case "signal":
				q, e := reconcile.NewSignalScanWithGraphJournalPort(port, graph)
				if e != nil {
					t.Fatal(e)
				}
				scan = q.Scan
			case "timer":
				q, e := reconcile.NewTimerScanWithGraphJournalPort(port, graph)
				if e != nil {
					t.Fatal(e)
				}
				scan = q.Scan
			case "suspended":
				q, e := reconcile.NewSuspendedScanWithGraphJournalPort(port, graph)
				if e != nil {
					t.Fatal(e)
				}
				scan = q.Scan
			}
			result, e := scan(ctx, 1, 2, false)
			if !errors.Is(e, journal.ErrUnknown) || result.RetrySequence != 2 || len(transport.Runs()) != 0 {
				t.Fatalf("uncertain second root certified or repaired: %+v %v", result, e)
			}
		})
	}
}

// Avoid scheduler faults that could hit another concurrently inspected root.
// This wrapper fails the selected authority read deterministically.
type faultRootPort struct {
	graphpublication.Port
	subject string
}

func (p *faultRootPort) ReadRoot(ctx context.Context, subject string) (graphpublication.Root, error) {
	if subject == p.subject {
		return graphpublication.Root{}, context.DeadlineExceeded
	}
	return p.Port.ReadRoot(ctx, subject)
}
func (p *faultRootPort) protocol(m *sim.GraphPublicationTransport) graphpublication.Protocol {
	q := m.Protocol()
	q.Port = p
	return q
}

func TestGraphReconcileRetriesPublishedUnboundStart(t *testing.T) {
	for _, kind := range []string{"timer", "suspended"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			schedule := sim.NewScheduler(17)
			model := sim.NewGraphPublicationTransport(schedule)
			graph, err := journal.NewGraphStore(journal.GraphConfig{Protocol: model.Protocol(), CanonicalStarts: true, CanonicalSignals: true})
			if err != nil {
				t.Fatal(err)
			}
			pending, err := graph.ReserveStart(ctx, journal.GraphStartRequest{Type: "test", ID: "pending"}, []byte(`7`))
			if err != nil {
				t.Fatal(err)
			}
			transport := sim.NewSignalTransport(schedule)
			seq, err := transport.PublishInvocation(ctx, &nats.Msg{Subject: identity.InvocationSubject("test", "pending"), Data: pending.Start.PointerBytes(), Header: nats.Header{journal.GraphStartTokenHeader: {pending.Start.Token}}})
			if err != nil {
				t.Fatal(err)
			}
			port := forbiddenGraphLegacyPort{transport}
			var scan func(context.Context, uint64, int, bool) (reconcile.ScanResult, error)
			if kind == "timer" {
				scanner, e := reconcile.NewTimerScanWithGraphJournalPort(port, graph)
				if e != nil {
					t.Fatal(e)
				}
				scan = scanner.Scan
			} else {
				scanner, e := reconcile.NewSuspendedScanWithGraphJournalPort(port, graph)
				if e != nil {
					t.Fatal(e)
				}
				scan = scanner.Scan
			}
			result, err := scan(ctx, 1, 2, false)
			if !errors.Is(err, journal.ErrUnknown) || !errors.Is(err, journal.ErrStale) || result.RetrySequence != 0 || len(transport.Runs()) != 0 {
				t.Fatalf("unbound generation certified or treated as fatal: %+v %v", result, err)
			}
			// A fresh binding makes the same source readable, with no legacy fallback,
			// journal initialization, wakeup or test-local suppression of the race.
			if err = graph.BindStart(ctx, "test", "pending", pending.Start.Token, seq); err != nil {
				t.Fatal(err)
			}
			result, err = scan(ctx, 1, 2, false)
			if err != nil || result.Inspected != 1 || result.NextSequence != 1 || len(transport.Runs()) != 0 {
				t.Fatalf("bound retry failed: %+v %v", result, err)
			}
		})
	}
}

type graphSchedulingProbe struct {
	forbiddenGraphLegacyPort
	blocked bool
	err     error
}

func (p *graphSchedulingProbe) GraphRepairBlocked(context.Context, string, string) (bool, error) {
	return p.blocked, p.err
}

func TestGraphReconcileDefersHistoryPinsWhileDeliveryOwnsLease(t *testing.T) {
	for _, kind := range []string{"timer", "suspended"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			scheduler := sim.NewScheduler(23)
			model := sim.NewGraphPublicationTransport(scheduler)
			graph, err := journal.NewGraphStore(journal.GraphConfig{Protocol: model.Protocol()})
			if err != nil {
				t.Fatal(err)
			}
			transport := sim.NewSignalTransport(scheduler)
			seq, err := transport.PublishInvocation(ctx, &nats.Msg{Subject: identity.InvocationSubject("test", "held")})
			if err != nil {
				t.Fatal(err)
			}
			tail, err := graph.Begin(ctx, "test", "held", seq)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = graph.Append(ctx, "test", "held", seq, journal.Entry{Kind: journal.Started}, tail, nil, nil); err != nil {
				t.Fatal(err)
			}
			probe := &graphSchedulingProbe{forbiddenGraphLegacyPort: forbiddenGraphLegacyPort{transport}, blocked: true}
			var scan func(context.Context, uint64, int, bool) (reconcile.ScanResult, error)
			if kind == "timer" {
				scanner, e := reconcile.NewTimerScanWithGraphJournalPort(probe, graph)
				if e != nil {
					t.Fatal(e)
				}
				scan = scanner.Scan
			} else {
				scanner, e := reconcile.NewSuspendedScanWithGraphJournalPort(probe, graph)
				if e != nil {
					t.Fatal(e)
				}
				scan = scanner.Scan
			}
			before := len(scheduler.Trace().Transport)
			if _, err = scan(ctx, 1, 2, false); err != nil {
				t.Fatal(err)
			}
			for _, event := range scheduler.Trace().Transport[before:] {
				if strings.HasPrefix(event.Operation, "graph_publication_") {
					t.Fatalf("held delivery caused graph read/pin: %s", event.Operation)
				}
			}
			probe.blocked = false
			probe.err = context.DeadlineExceeded
			result, err := scan(ctx, 1, 2, false)
			if !errors.Is(err, journal.ErrUnknown) || result.RetrySequence != 0 || len(transport.Runs()) != 0 {
				t.Fatalf("uncertain scheduling advanced or repaired: %+v %v", result, err)
			}
			probe.err = nil
			before = len(scheduler.Trace().Transport)
			if _, err = scan(ctx, 1, 2, false); err != nil {
				t.Fatal(err)
			}
			pinned := false
			for _, event := range scheduler.Trace().Transport[before:] {
				if event.Operation == "graph_publication_cas_root" {
					pinned = true
				}
			}
			if !pinned {
				t.Fatal("released delivery did not resume retained history inspection")
			}
		})
	}
}
