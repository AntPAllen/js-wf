package sim

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"js-wf/lease"
	"js-wf/reconcile"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// LoopTransport models the lease bucket, persistent cursor bucket, and
// cadence used by the production reconciler loop. The two KV buckets have
// independent revision spaces and only the lease bucket expires.
type LoopTransport struct {
	schedule *Scheduler
	leasing  *lease.Store
	cursors  *KVTransport
	waits    int
	stopAt   int
	stop     context.CancelFunc
	saves    int
	faultAt  int
	fault    KVFaultKind
}

var _ reconcile.LoopPort = (*LoopTransport)(nil)

func NewLoopTransport(schedule *Scheduler) *LoopTransport {
	return &LoopTransport{
		schedule: schedule,
		leasing:  lease.NewWithKVPort(NewKVTransport(schedule, 30*time.Second)),
		cursors:  NewKVTransport(schedule, 0),
	}
}

// StopAfterWaits ends a bounded loop run after its next completed scan cycle.
func (m *LoopTransport) StopAfterWaits(count int, stop context.CancelFunc) {
	m.stopAt, m.stop = m.waits+count, stop
}

// RejectCursorSaveAt injects one loss at the selected cursor CAS attempt.
func (m *LoopTransport) RejectCursorSaveAt(at int, kind KVFaultKind) error {
	if at < 1 || kind != KVDropBeforeCommit && kind != KVLoseAckAfterCommit {
		return fmt.Errorf("invalid cursor save fault at=%d kind=%s", at, kind)
	}
	m.faultAt, m.fault = m.saves+at, kind
	return nil
}

func (m *LoopTransport) Prepare(ctx context.Context) error { return ctx.Err() }

func (m *LoopTransport) Acquire(ctx context.Context, kind, workerID string) (reconcile.LoopLease, error) {
	return m.leasing.Acquire(ctx, "system", kind+"-reconciler", workerID)
}

func (m *LoopTransport) LoadCursor(ctx context.Context, kind string) (uint64, uint64, error) {
	entry, err := m.cursors.Get(ctx, "scan."+kind)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return 1, 0, nil
	}
	if err != nil {
		return 0, 0, err
	}
	sequence, err := strconv.ParseUint(string(entry.Value), 10, 64)
	if err != nil || sequence == 0 {
		return 0, 0, fmt.Errorf("invalid %s scan cursor", kind)
	}
	return sequence, entry.Revision, nil
}

func (m *LoopTransport) SaveCursor(ctx context.Context, kind string, next, revision uint64) (uint64, error) {
	if next == 0 {
		return 0, fmt.Errorf("zero scan cursor")
	}
	m.saves++
	if m.saves == m.faultAt {
		operation := "update"
		if revision == 0 {
			operation = "create"
		}
		if err := m.cursors.QueueFault(KVFault{Operation: operation, Kind: m.fault}); err != nil {
			return 0, err
		}
	}
	key := "scan." + kind
	data := []byte(strconv.FormatUint(next, 10))
	var saved uint64
	var err error
	if revision == 0 {
		saved, err = m.cursors.Create(ctx, key, data)
	} else {
		saved, err = m.cursors.Update(ctx, key, data, revision)
	}
	if errors.Is(err, jetstream.ErrKeyExists) || errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
		return 0, reconcile.ErrCursorStale
	}
	if errors.Is(err, ErrTransportLost) {
		// The real adapter sees a timeout after the same lost request or ack.
		return 0, nats.ErrTimeout
	}
	return saved, err
}

func (m *LoopTransport) Wait(ctx context.Context, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if delay < 0 || delay%time.Millisecond != 0 {
		return fmt.Errorf("invalid virtual reconcile delay %s", delay)
	}
	if err := m.schedule.AdvanceMillis(delay.Milliseconds()); err != nil {
		return err
	}
	m.schedule.RecordTransport(TransportEvent{Operation: "reconcile_wait", Outcome: fmt.Sprintf("%dms", delay.Milliseconds()), AtMillis: m.schedule.NowMillis()})
	m.waits++
	if m.stop != nil && m.waits >= m.stopAt {
		m.stop()
	}
	return nil
}
