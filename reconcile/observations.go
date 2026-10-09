package reconcile

import (
	"context"
	"fmt"
	"time"

	"js-wf/journal"

	"github.com/nats-io/nats.go/jetstream"
)

// RepairEvent records an actual repair decision and its enqueue result. An
// uncertain publication may have committed. It must not be counted as an
// acknowledged enqueue; dry runs do not publish. The observer is synchronous
// and must not block the scanner.
type RepairEvent struct {
	At                 time.Time  `json:"at"`
	Kind               string     `json:"kind"`
	Type               string     `json:"type"`
	ID                 string     `json:"id"`
	Reason             string     `json:"reason"`
	SourceSequence     uint64     `json:"source_sequence,omitempty"`
	InvocationSequence uint64     `json:"invocation_sequence,omitempty"`
	JournalSequence    uint64     `json:"journal_sequence,omitempty"`
	FireAt             *time.Time `json:"fire_at,omitempty"`
	ClockDomain        string     `json:"clock_domain,omitempty"`
	TimerStep          *uint64    `json:"timer_step,omitempty"`
	RetryWindow        int64      `json:"retry_window,omitempty"`
	Outcome            string     `json:"outcome"`
	Error              string     `json:"error,omitempty"`
}

func reportRepair(observe func(RepairEvent), event RepairEvent, dry bool, err error) {
	if observe == nil {
		return
	}
	event.At = time.Now().UTC()
	event.Outcome = "acknowledged"
	if dry {
		event.Outcome = "dry_run"
	} else if err != nil {
		event.Outcome = "uncertain"
		event.Error = err.Error()
	}
	observe(event)
}

// ScanEvent records cursor progress even when a scan makes no repair. Observers
// run synchronously and must not block the scanner.
type ScanEvent struct {
	Started  time.Time  `json:"started"`
	Finished time.Time  `json:"finished"`
	Kind     string     `json:"kind"`
	Cursor   uint64     `json:"cursor"`
	Result   ScanResult `json:"result"`
	Error    string     `json:"error,omitempty"`
}

// RunRepairLoopWithScanObserver observes the same production scan and fenced
// cursor loop, including unsuccessful passes through retained invocations.
func RunRepairLoopWithScanObserver(ctx context.Context, js jetstream.JetStream, workerID, kind string, interval time.Duration, budget int, observe func(RepairEvent), progress func(ScanEvent)) error {
	return runRepairLoopObserved(ctx, js, workerID, kind, interval, budget, observe, nil, progress, nil)
}

// RunRepairLoopObserved uses the unchanged fenced cursor loop and production
// scanner, recording each attempted start, signal, timer, fallback timer, or
// suspended-wait repair.
func RunRepairLoopObserved(ctx context.Context, js jetstream.JetStream, workerID, kind string, interval time.Duration, budget int, observe func(RepairEvent)) error {
	return RunRepairLoopWithClock(ctx, js, workerID, kind, interval, budget, observe, nil)
}

// RunRepairLoopWithClock configures the same domain clock used by tagged
// workers, retaining the production leader lease, cursor and repair observers.
func RunRepairLoopWithClock(ctx context.Context, js jetstream.JetStream, workerID, kind string, interval time.Duration, budget int, observe func(RepairEvent), clock TimerDomainClock) error {
	return runRepairLoopObserved(ctx, js, workerID, kind, interval, budget, observe, clock, nil, nil)
}

func runRepairLoopObserved(ctx context.Context, js jetstream.JetStream, workerID, kind string, interval time.Duration, budget int, observe func(RepairEvent), clock TimerDomainClock, progress func(ScanEvent), graph *journal.GraphStore) error {
	var scan scanFunc
	switch kind {
	case "graph-continuation":
		s, err := NewCanonicalContinuationScan(js, graph)
		if err != nil {
			return err
		}
		s.Observe = observe
		scan = s.Scan
	case "graph-terminal":
		s, err := NewCanonicalTerminalScan(ctx, js, graph)
		if err != nil {
			return err
		}
		s.Observe = observe
		scan = s.Scan
	case "graph-signal":
		s, err := NewCanonicalSignalScan(js, graph)
		if err != nil {
			return err
		}
		s.Observe = observe
		scan = s.Scan
	case "graph-start":
		s, err := NewCanonicalStartScan(js, graph)
		if err != nil {
			return err
		}
		s.Observe = observe
		scan = s.Scan
	case "start":
		s := NewStartScan(js)
		s.graph = graph
		s.Observe = observe
		scan = s.Scan
	case "signal":
		s := NewSignalScan(js)
		s.graph = graph
		s.Observe = observe
		scan = s.Scan
	case "timer":
		s := NewTimerScan(js)
		s.graph = graph
		s.DomainNow = clock
		s.Observe = observe
		scan = s.Scan
	case "fallback-timer":
		s := NewFallbackTimerScan(js)
		s.graph = graph
		s.DomainNow = clock
		s.Observe = observe
		scan = s.Scan
	case "suspended":
		s := NewSuspendedScan(js)
		s.graph = graph
		s.DomainNow = clock
		s.Observe = observe
		scan = s.Scan
	default:
		return fmt.Errorf("unsupported observed repair kind %q", kind)
	}
	if progress != nil {
		original := scan
		scan = func(ctx context.Context, cursor uint64, budget int, dry bool) (ScanResult, error) {
			event := ScanEvent{Started: time.Now().UTC(), Kind: kind, Cursor: cursor}
			result, err := original(ctx, cursor, budget, dry)
			event.Finished = time.Now().UTC()
			event.Result = result
			if err != nil {
				event.Error = err.Error()
			}
			progress(event)
			return result, err
		}
	}
	return runLoop(ctx, js, workerID, kind, interval, budget, scan)
}
