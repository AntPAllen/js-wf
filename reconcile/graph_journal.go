package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/lease"
)

type repairJournalPort interface {
	ReadJournal(context.Context, string, string) ([]journal.Record, error)
}

// GraphRepairSchedulingPort provides a scheduling hint only. A live delivery
// already owns recovery; postpone history pins which could starve its append.
// Lease expiry/release makes a later scan eligible. This grants no write or
// payload authority and is used by timer, suspended and continuation discovery.
type GraphRepairSchedulingPort interface {
	GraphRepairBlocked(context.Context, string, string) (bool, error)
}

func graphRepairBlocked(ctx context.Context, js jetstream.JetStream, typ, id string) (bool, error) {
	kv, err := js.KeyValue(ctx, "WF_LEASE")
	if err != nil {
		return false, err
	}
	entry, err := kv.Get(ctx, identity.Key(typ, id))
	if errors.Is(err, jetstream.ErrKeyNotFound) || errors.Is(err, jetstream.ErrKeyDeleted) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var value lease.Value
	if err = json.Unmarshal(entry.Value(), &value); err != nil || value.Worker == "" || value.Epoch == 0 {
		return false, fmt.Errorf("invalid lease scheduling observation")
	}
	return true, nil
}

func readRepairJournal(ctx context.Context, graph *journal.GraphStore, legacy repairJournalPort, typ, id string, invocation uint64) ([]journal.Record, error) {
	if graph == nil {
		return legacy.ReadJournal(ctx, typ, id)
	}
	if scheduling, ok := legacy.(GraphRepairSchedulingPort); ok {
		blocked, err := scheduling.GraphRepairBlocked(ctx, typ, id)
		if err != nil {
			return nil, fmt.Errorf("%w: graph repair scheduling observation: %w", journal.ErrUnknown, err)
		}
		if blocked {
			return nil, nil
		}
	}
	records, _, err := graph.ReadExisting(ctx, typ, id, invocation)
	if errors.Is(err, journal.ErrStale) {
		// WF_INV can become visible before canonical Start binding, or the
		// generation can change while a reader acquires its pin. Neither
		// permits a legacy fallback or an empty-history repair decision.
		// Preserve the scanner's confirmed prefix and retry a fresh read.
		return nil, fmt.Errorf("%w: graph repair history generation changed: %w", journal.ErrUnknown, err)
	}
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
// graph history. Supported kinds are start, graph-start, graph-signal,
// graph-terminal, graph-continuation, signal, timer, fallback-timer and suspended. Tombstone state and purge
// coordination require separate migration.
func RunRepairLoopWithGraphJournal(ctx context.Context, js jetstream.JetStream, workerID, kind string, interval time.Duration, budget int, graph *journal.GraphStore, observe func(RepairEvent), clock TimerDomainClock, progress func(ScanEvent)) error {
	if graph == nil {
		return fmt.Errorf("graph repair loop requires journal")
	}
	if kind == "fallback-timer" && !graph.CanonicalStarts() {
		return fmt.Errorf("graph fallback requires canonical Start store")
	}
	if kind == "graph-continuation" && !graph.CheckpointIndex() {
		return fmt.Errorf("continuation repair requires v5 checkpoint index")
	}
	if kind != "graph-continuation" && kind != "start" && kind != "graph-start" && kind != "graph-signal" && kind != "graph-terminal" && kind != "signal" && kind != "timer" && kind != "fallback-timer" && kind != "suspended" {
		return fmt.Errorf("unsupported graph repair kind %q", kind)
	}
	return runRepairLoopObserved(ctx, js, workerID, kind, interval, budget, observe, clock, progress, graph)
}
