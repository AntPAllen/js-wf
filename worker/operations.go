package worker

import (
	"context"
	"time"

	"js-wf/journal"
	"js-wf/lease"

	"github.com/nats-io/nats.go/jetstream"
)

// OperationEvent measures a worker call, including local waiting and retries.
// It does not distinguish server execution from network or client time. No
// invocation inputs, signal contents or step results are included.
type OperationEvent struct {
	At                   time.Time
	Duration             time.Duration
	Worker               string
	Type                 string
	ID                   string
	RunSequence          uint64
	Delivery             uint64
	Operation            string
	JournalIndex         uint64
	JournalKind          journal.Kind
	Error                string
	LeaseGateWait        time.Duration
	LeaseUpdateDuration  time.Duration
	LeaseUpdateAttempted bool
	// ServerTime is the physical clock lookup for legacy creation or native
	// hint translation, identified by Operation.
	ServerTime                     *time.Time `json:",omitempty"`
	ClockDomain                    string     `json:",omitempty"`
	ClockLower, ClockUpper         *time.Time `json:",omitempty"`
	TimerDeadline, TimerScheduleAt *time.Time `json:",omitempty"`
	TimerPublished                 *bool      `json:",omitempty"`
}

// WithOperationObserver enables optional per-call timings. The callback may
// run concurrently with heartbeats and must be concurrency-safe and quick.
func WithOperationObserver(observe func(OperationEvent)) Option {
	return func(w *Worker) error {
		w.operationObserver = observe
		return nil
	}
}

type deliveryOperations struct {
	base OperationEvent
	now  func() time.Time
	emit func(OperationEvent)
}

func (w *Worker) deliveryOperations(typ, id string, sequence, delivery uint64) *deliveryOperations {
	if w.operationObserver == nil {
		return nil
	}
	now := w.operationNow
	if now == nil {
		now = time.Now
	}
	return &deliveryOperations{base: OperationEvent{Worker: w.ID, Type: typ, ID: id, RunSequence: sequence, Delivery: delivery}, now: now, emit: w.operationObserver}
}

func (o *deliveryOperations) begin() time.Time {
	if o == nil {
		return time.Time{}
	}
	return o.now()
}

func (o *deliveryOperations) finish(start time.Time, name string, index uint64, kind journal.Kind, err error, renewals ...lease.RenewalTiming) {
	if o == nil {
		return
	}
	event := o.base
	event.At = o.now()
	event.Duration = event.At.Sub(start)
	event.Operation, event.JournalIndex, event.JournalKind = name, index, kind
	if err != nil {
		event.Error = err.Error()
	}
	if len(renewals) > 0 {
		event.LeaseGateWait = renewals[0].GateWait
		event.LeaseUpdateDuration = renewals[0].Update
		event.LeaseUpdateAttempted = renewals[0].UpdateAttempted
	}
	o.emit(event)
}

func (o *deliveryOperations) finishTimerClock(start time.Time, index uint64, serverTime time.Time, err error) {
	if o == nil {
		return
	}
	event := o.base
	event.At = o.now()
	event.Duration = event.At.Sub(start)
	event.Operation = "timer_clock"
	event.JournalIndex = index
	event.JournalKind = journal.StepRequested
	if err != nil {
		event.Error = err.Error()
	} else {
		event.ServerTime = &serverTime
	}
	o.emit(event)
}

func (o *deliveryOperations) finishDomainClock(start time.Time, index uint64, domain string, lower, upper time.Time, err error) {
	if o == nil {
		return
	}
	event := o.base
	event.At = o.now()
	event.Duration = event.At.Sub(start)
	event.Operation = "timer_domain_clock"
	event.JournalIndex = index
	event.ClockDomain = domain
	if err != nil {
		event.Error = err.Error()
	} else {
		event.ClockLower = &lower
		event.ClockUpper = &upper
	}
	o.emit(event)
}

type observedSignalDrain struct {
	SignalDrainPort
	operations *deliveryOperations
}

func (p observedSignalDrain) LastSignalSequence(ctx context.Context) (uint64, error) {
	started := p.operations.begin()
	sequence, err := p.SignalDrainPort.LastSignalSequence(ctx)
	p.operations.finish(started, "signal_info", 0, "", err)
	return sequence, err
}

func (p observedSignalDrain) NextSignal(ctx context.Context, from uint64, subject string) (*jetstream.RawStreamMsg, error) {
	started := p.operations.begin()
	message, err := p.SignalDrainPort.NextSignal(ctx, from, subject)
	p.operations.finish(started, "signal_read", 0, "", err)
	return message, err
}

func (p observedSignalDrain) SignalBlob(ctx context.Context, name string) ([]byte, error) {
	started := p.operations.begin()
	data, err := p.SignalDrainPort.SignalBlob(ctx, name)
	p.operations.finish(started, "signal_blob_read", 0, "", err)
	return data, err
}

func (o *deliveryOperations) renew(ctx context.Context, l *lease.Lease, minIdle time.Duration) (bool, lease.RenewalTiming, error) {
	if o != nil {
		return l.RenewTimed(ctx, minIdle)
	}
	updated, err := l.RenewIfIdle(ctx, minIdle)
	return updated, lease.RenewalTiming{}, err
}

// finishNativeHint records the exact translation submitted to the native timer
// port and its acknowledgment classification. A duplicate did not store a new
// hint; a failed publication is not proof that the translated hint committed.
func (o *deliveryOperations) finishNativeHint(start time.Time, index uint64, domain string, physical, lower, upper time.Time, deadline TimerDeadline, published bool, err error) {
	if o == nil {
		return
	}
	event := o.base
	event.At = o.now()
	event.Duration = event.At.Sub(start)
	event.Operation = "timer_native_hint"
	event.JournalIndex = index
	event.JournalKind = journal.StepRequested
	event.ClockDomain = domain
	event.ServerTime = &physical
	event.ClockLower = &lower
	event.ClockUpper = &upper
	event.TimerDeadline = &deadline.FireAt
	event.TimerScheduleAt = &deadline.ScheduleAt
	event.TimerPublished = &published
	if err != nil {
		event.Error = err.Error()
	}
	o.emit(event)
}
