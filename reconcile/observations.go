package reconcile

import (
	"context"
	"fmt"
	"time"

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

// RunRepairLoopObserved uses the unchanged fenced cursor loop and production
// scanner, recording each attempted start, signal, timer, fallback timer, or
// suspended-wait repair.
func RunRepairLoopObserved(ctx context.Context, js jetstream.JetStream, workerID, kind string, interval time.Duration, budget int, observe func(RepairEvent)) error {
	var scan scanFunc
	switch kind {
	case "start":
		s := NewStartScan(js)
		s.Observe = observe
		scan = s.Scan
	case "signal":
		s := NewSignalScan(js)
		s.Observe = observe
		scan = s.Scan
	case "timer":
		s := NewTimerScan(js)
		s.Observe = observe
		scan = s.Scan
	case "fallback-timer":
		s := NewFallbackTimerScan(js)
		s.Observe = observe
		scan = s.Scan
	case "suspended":
		s := NewSuspendedScan(js)
		s.Observe = observe
		scan = s.Scan
	default:
		return fmt.Errorf("unsupported observed repair kind %q", kind)
	}
	return runLoop(ctx, js, workerID, kind, interval, budget, scan)
}
