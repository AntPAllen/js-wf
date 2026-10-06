package sim

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"js-wf/reconcile"
)

// The terminal population is already complete. Only scan cursors continue
// changing the shared state bucket; no server replication is modeled here.
type auditCursorPort struct {
	reconcile.LoopPort
	state    *KVTransport
	saves    map[string]int
	baseline uint64
	stop     context.CancelFunc
}

func (p *auditCursorPort) SaveCursor(ctx context.Context, kind string, next, revision uint64) (uint64, error) {
	saved, err := p.LoopPort.SaveCursor(ctx, kind, next, revision)
	if err != nil {
		return saved, err
	}
	p.saves[kind]++
	allOnce, allThree := true, true
	for _, kind := range []string{"start", "signal", "suspended"} {
		allOnce = allOnce && p.saves[kind] >= 1
		allThree = allThree && p.saves[kind] >= 3
	}
	if allOnce && p.baseline == 0 {
		p.baseline = p.state.revision
	}
	if allThree {
		p.stop()
	}
	return saved, nil
}

func runAuditCursorQuiescence(seed int64, replay *Trace) (Trace, error) {
	schedule := NewScheduler(seed)
	if replay != nil {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	if err := schedule.SetWorkload("audit_cursor_quiescence_regression"); err != nil {
		return schedule.Trace(), err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	loopCtx, stop := context.WithCancel(ctx)
	defer stop()
	loop := NewLoopTransport(schedule)
	// Production repair cursors and terminal results share WF_STATE.
	if _, err := loop.cursors.Put(ctx, "test.completed", []byte("terminal")); err != nil {
		return schedule.Trace(), err
	}
	port := &auditCursorPort{LoopPort: loop, state: loop.cursors, saves: map[string]int{}, stop: stop}
	var actors []ReconcileLoopActor
	for _, kind := range []string{"start", "signal", "suspended"} {
		kind := kind
		actors = append(actors, ReconcileLoopActor{Name: kind, Run: func(_ context.Context, yielding reconcile.LoopPort, _ reconcile.StartScanPort) error {
			return reconcile.RunLoopWithPort(loopCtx, yielding, "audit-"+kind, kind, time.Second, 1,
				func(context.Context, uint64, int, bool) (reconcile.ScanResult, error) {
					return reconcile.ScanResult{NextSequence: 1}, nil
				})
		}})
	}
	// Return means all production loops (including lease cleanup) are joined.
	results, err := RunReconcileLoopActors(ctx, schedule, port, NewStartTransport(schedule), actors)
	if err != nil {
		return schedule.Trace(), err
	}
	for kind, err := range results {
		if err != nil {
			return schedule.Trace(), fmt.Errorf("%s: %w", kind, err)
		}
	}
	joined := loop.cursors.revision
	if port.baseline == 0 || joined <= port.baseline || len(loop.cursors.items) != 4 {
		return schedule.Trace(), fmt.Errorf("expected cursor-only drift: before=%d after=%d messages=%d", port.baseline, joined, len(loop.cursors.items))
	}
	if err := schedule.AdvanceMillis(10000); err != nil {
		return schedule.Trace(), err
	}
	terminal, err := loop.cursors.Get(ctx, "test.completed")
	if err != nil || string(terminal.Value) != "terminal" || loop.cursors.revision != joined || len(loop.cursors.items) != 4 {
		return schedule.Trace(), fmt.Errorf("joined source changed or terminal lost: %v", err)
	}
	if err := schedule.Finish(); err != nil {
		return schedule.Trace(), err
	}
	return schedule.Trace(), nil
}

func TestRepairCursorWritersMustJoinBeforeFrozenAudit(t *testing.T) {
	for seed := int64(1); seed <= 100; seed++ {
		trace, err := runAuditCursorQuiescence(seed, nil)
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		if seed <= 10 {
			replay, err := runAuditCursorQuiescence(seed, &trace)
			if err != nil || !reflect.DeepEqual(trace, replay) {
				t.Fatalf("seed %d replay: %v", seed, err)
			}
		}
	}
}
