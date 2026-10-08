package reconcile

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/journal"
)

type repairJournalPort interface {
	ReadJournal(context.Context, string, string) ([]journal.Record, error)
}

func readRepairJournal(ctx context.Context, graph *journal.GraphStore, legacy repairJournalPort, typ, id string, invocation uint64) ([]journal.Record, error) {
	if graph == nil {
		return legacy.ReadJournal(ctx, typ, id)
	}
	records, _, err := graph.ReadExisting(ctx, typ, id, invocation)
	return records, err
}

// Graph constructors select canonical graph history for every repair decision.
// They never fall back to WF_JRN or initialize/import a graph. Invocation/signal
// discovery and wakeup publication still use the supplied native or modeled
// ports. Use the same graph configuration as the workers.
func NewStartScanWithGraphJournal(js jetstream.JetStream, graph *journal.GraphStore) (*StartScan, error) {
	return NewStartScanWithGraphJournalPort(NewStartScan(js).port, graph)
}

func NewStartScanWithGraphJournalPort(port StartScanPort, graph *journal.GraphStore) (*StartScan, error) {
	if port == nil || graph == nil {
		return nil, fmt.Errorf("graph start scanner requires port and journal")
	}
	s := NewStartScanWithPort(port)
	s.graph = graph
	return s, nil
}

func NewSignalScanWithGraphJournal(js jetstream.JetStream, graph *journal.GraphStore) (*SignalScan, error) {
	return NewSignalScanWithGraphJournalPort(NewSignalScan(js).port, graph)
}

func NewSignalScanWithGraphJournalPort(port SignalScanPort, graph *journal.GraphStore) (*SignalScan, error) {
	if port == nil || graph == nil {
		return nil, fmt.Errorf("graph signal scanner requires port and journal")
	}
	s := NewSignalScanWithPort(port)
	s.graph = graph
	return s, nil
}

func NewTimerScanWithGraphJournal(js jetstream.JetStream, graph *journal.GraphStore) (*TimerScan, error) {
	return NewTimerScanWithGraphJournalPort(NewTimerScan(js).port, graph)
}

func NewTimerScanWithGraphJournalPort(port TimerScanPort, graph *journal.GraphStore) (*TimerScan, error) {
	if port == nil || graph == nil {
		return nil, fmt.Errorf("graph timer scanner requires port and journal")
	}
	s := NewTimerScanWithPort(port)
	s.graph = graph
	return s, nil
}

func NewSuspendedScanWithGraphJournal(js jetstream.JetStream, graph *journal.GraphStore) (*SuspendedScan, error) {
	return NewSuspendedScanWithGraphJournalPort(NewSuspendedScanPort(js), graph)
}

func NewSuspendedScanWithGraphJournalPort(port SuspendedScanPort, graph *journal.GraphStore) (*SuspendedScan, error) {
	if port == nil || graph == nil {
		return nil, fmt.Errorf("graph suspended scanner requires port and journal")
	}
	s := NewSuspendedScanWithPort(port)
	s.graph = graph
	return s, nil
}

// RunRepairLoopWithGraphJournal runs the existing fenced leader/cursor loop with
// graph history. Supported kinds are start, signal, timer and suspended; fallback
// timer/tombstone state and purge coordination require separate migration.
func RunRepairLoopWithGraphJournal(ctx context.Context, js jetstream.JetStream, workerID, kind string, interval time.Duration, budget int, graph *journal.GraphStore, observe func(RepairEvent), clock TimerDomainClock, progress func(ScanEvent)) error {
	if graph == nil {
		return fmt.Errorf("graph repair loop requires journal")
	}
	if kind != "start" && kind != "signal" && kind != "timer" && kind != "suspended" {
		return fmt.Errorf("unsupported graph repair kind %q", kind)
	}
	return runRepairLoopObserved(ctx, js, workerID, kind, interval, budget, observe, clock, progress, graph)
}
