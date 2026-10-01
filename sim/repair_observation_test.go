package sim

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"js-wf/client"
	"js-wf/reconcile"
	"js-wf/worker"
)

type hiddenRepairAck struct {
	*SignalTransport
	fired bool
}

func (p *hiddenRepairAck) response(err error) error {
	if err != nil {
		return err
	}
	if !p.fired {
		p.fired = true
		return nats.ErrTimeout
	}
	return nil
}
func (p *hiddenRepairAck) EnqueueStart(ctx context.Context, typ, id string, seq uint64) error {
	return p.response(p.SignalTransport.EnqueueStart(ctx, typ, id, seq))
}
func (p *hiddenRepairAck) EnqueueSignal(ctx context.Context, typ, id string, seq uint64) error {
	return p.response(p.SignalTransport.EnqueueSignal(ctx, typ, id, seq))
}
func (p *hiddenRepairAck) EnqueueSuspended(ctx context.Context, typ, id string, seq uint64, window int64) error {
	return p.response(p.SignalTransport.EnqueueSuspended(ctx, typ, id, seq, window))
}

// A committed repair with a lost acknowledgement must be recorded as
// uncertain even when retry deduplication later establishes acknowledgement.
// Dry-run decisions and ineligible pages must not be called publications.
func TestRepairObservationDistinguishesDryUnknownAndAcknowledged(t *testing.T) {
	for _, kind := range []string{"start", "signal", "suspended"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			schedule := NewScheduler(1)
			model := NewSignalTransport(schedule)
			port := &hiddenRepairAck{SignalTransport: model}
			c := client.NewWithSignalPorts(model, model)
			input, err := c.Start(ctx, "test", "repair-observation", []byte(`null`))
			if err != nil {
				t.Fatal(err)
			}
			var source uint64 = input.InvSeq
			if kind == "signal" {
				source, err = c.Signal(ctx, "test", input.ID, "go", []byte(`42`), "repair-source")
				if err != nil {
					t.Fatal(err)
				}
			}
			if kind == "suspended" {
				records, err := suspendedFixture(input.InvSeq, "due_timer", time.Unix(0, 0), 0)
				if err != nil {
					t.Fatal(err)
				}
				model.SetJournal("test", input.ID, records)
			}
			var events []reconcile.RepairEvent
			observe := func(e reconcile.RepairEvent) { events = append(events, e) }
			var scan func(context.Context, uint64, int, bool) (reconcile.ScanResult, error)
			switch kind {
			case "start":
				s := reconcile.NewStartScanWithPort(port)
				s.Observe = observe
				scan = s.Scan
			case "signal":
				s := reconcile.NewSignalScanWithPort(port)
				s.Observe = observe
				scan = s.Scan
			case "suspended":
				s := reconcile.NewSuspendedScanWithPort(port)
				s.Observe = observe
				s.Now = func() time.Time { return time.Unix(30, 0) }
				scan = s.Scan
			}
			before := len(model.Runs())
			result, err := scan(ctx, source, 1, true)
			if err != nil || result.Reenqueued != 1 || len(model.Runs()) != before {
				t.Fatalf("dry scan=%+v runs=%d/%d err=%v", result, len(model.Runs()), before, err)
			}
			result, err = scan(ctx, source, 1, false)
			if !errors.Is(err, nats.ErrTimeout) || result.Reenqueued != 1 {
				t.Fatalf("hidden committed enqueue=%+v err=%v", result, err)
			}
			committed := len(model.Runs())
			result, err = scan(ctx, source, 1, false)
			if err != nil || result.Reenqueued != 1 || len(model.Runs()) != committed {
				t.Fatalf("deduplicated repair=%+v err=%v", result, err)
			}
			if len(events) != 3 {
				t.Fatalf("events=%+v", events)
			}
			for i, outcome := range []string{"dry_run", "uncertain", "acknowledged"} {
				e := events[i]
				if e.Outcome != outcome || e.Kind != kind || e.Type != input.Type || e.ID != input.ID || e.Reason == "" || e.At.IsZero() {
					t.Fatalf("unattributed event=%+v", e)
				}
				if (e.Error != "") != (outcome == "uncertain") {
					t.Fatalf("uncertainty erased: %+v", e)
				}
				if kind != "suspended" && (e.SourceSequence != source || e.InvocationSequence != input.InvSeq) {
					t.Fatalf("wrong source generation: %+v", e)
				}
				if kind == "suspended" && e.JournalSequence == 0 {
					t.Fatalf("missing journal decision: %+v", e)
				}
			}
			if kind == "start" {
				model.MarkJournal(input.Type, input.ID)
			} else {
				records, err := suspendedFixture(input.InvSeq, "terminal", time.Time{}, 0)
				if err != nil {
					t.Fatal(err)
				}
				model.SetJournal(input.Type, input.ID, records)
			}
			result, err = scan(ctx, source, 1, false)
			if err != nil || result.Reenqueued != 0 || len(events) != 3 {
				t.Fatalf("ineligible invocation produced evidence: result=%+v events=%+v err=%v", result, events, err)
			}
		})
	}
}

func TestFallbackTimerObservationDryRunAndZeroStep(t *testing.T) {
	ctx := context.Background()
	base := time.Unix(1_700_000_000, 0).UTC()
	model := NewTimerScheduleTransport(NewScheduler(7), base)
	if _, err := worker.ScheduleTimerWithPort(ctx, model, false, "test", "zero-step", 7, 0, base.Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	scan := reconcile.NewFallbackTimerScanWithPort(model, func(context.Context) (time.Time, error) { return base, nil })
	var events []reconcile.RepairEvent
	scan.Observe = func(e reconcile.RepairEvent) { events = append(events, e) }
	result, err := scan.Scan(ctx, 1, 1, true)
	if err != nil || result.Reenqueued != 1 || len(model.Runs()) != 0 || len(model.RetainedFallbackRecords()) != 1 || len(events) != 1 || events[0].Outcome != "dry_run" {
		t.Fatalf("dry scan=%+v events=%+v err=%v", result, events, err)
	}
	result, err = scan.Scan(ctx, 1, 1, false)
	if err != nil || result.Reenqueued != 1 || len(model.Runs()) != 1 || len(model.RetainedFallbackRecords()) != 0 || len(events) != 2 || events[1].Outcome != "acknowledged" {
		t.Fatalf("publish scan=%+v events=%+v err=%v", result, events, err)
	}
	for _, e := range events {
		if e.TimerStep == nil || *e.TimerStep != 0 || e.InvocationSequence != 7 || e.SourceSequence != 1 || e.FireAt == nil || !e.FireAt.Equal(base.Add(-time.Second)) {
			t.Fatalf("zero-step evidence=%+v", e)
		}
	}
}
