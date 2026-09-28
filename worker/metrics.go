package worker

import (
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Metrics is a point-in-time view of one worker's dispatch activity.
// EnqueueToLease uses the JetStream message timestamp and the worker clock;
// operators should keep those clocks synchronized when comparing latencies.
type Metrics struct {
	LeaseAcquisitions     uint64        `json:"lease_acquisitions"`
	LeaseContentions      uint64        `json:"lease_contentions"`
	LeaseAcquireFailures  uint64        `json:"lease_acquire_failures"`
	FencingEvents         uint64        `json:"fencing_events"`
	Redeliveries          uint64        `json:"redeliveries"`
	EnqueueToLeaseSamples uint64        `json:"enqueue_to_lease_samples"`
	EnqueueToLeaseTotal   time.Duration `json:"enqueue_to_lease_total"`
	EnqueueToLeaseMaximum time.Duration `json:"enqueue_to_lease_maximum"`
	TimersScheduled       uint64        `json:"timers_scheduled"`
	TimersFired           uint64        `json:"timers_fired"`
	TimerLateTotal        time.Duration `json:"timer_late_total"`
	TimerLateMaximum      time.Duration `json:"timer_late_maximum"`
	// Buckets count lateness <=100ms, <=500ms, <=2s, <=10s, <=30s, and >30s.
	TimerLateBuckets    [6]uint64 `json:"timer_late_buckets"`
	CancelledTimerNoOps uint64    `json:"cancelled_timer_no_ops"`
}

type metricsCounters struct {
	leaseAcquisitions     atomic.Uint64
	leaseContentions      atomic.Uint64
	leaseAcquireFailures  atomic.Uint64
	fencingEvents         atomic.Uint64
	redeliveries          atomic.Uint64
	enqueueToLeaseSamples atomic.Uint64
	enqueueToLeaseTotal   atomic.Int64
	enqueueToLeaseMaximum atomic.Int64
	timersScheduled       atomic.Uint64
	timersFired           atomic.Uint64
	timerLateTotal        atomic.Int64
	timerLateMaximum      atomic.Int64
	timerLateBuckets      [6]atomic.Uint64
	cancelledTimerNoOps   atomic.Uint64
}

func (m *metricsCounters) recordTimerFired(fireAt, wakeupAt time.Time) {
	late := wakeupAt.Sub(fireAt)
	if late < 0 {
		late = 0
	}
	m.timersFired.Add(1)
	m.timerLateTotal.Add(int64(late))
	for previous := m.timerLateMaximum.Load(); int64(late) > previous; previous = m.timerLateMaximum.Load() {
		if m.timerLateMaximum.CompareAndSwap(previous, int64(late)) {
			break
		}
	}
	boundaries := [...]time.Duration{100 * time.Millisecond, 500 * time.Millisecond, 2 * time.Second, 10 * time.Second, 30 * time.Second}
	bucket := len(boundaries)
	for i, boundary := range boundaries {
		if late <= boundary {
			bucket = i
			break
		}
	}
	m.timerLateBuckets[bucket].Add(1)
}

func (m *metricsCounters) recordRedelivery(metadata *jetstream.MsgMetadata) {
	if metadata.NumDelivered > 1 {
		m.redeliveries.Add(1)
	}
}

func (m *metricsCounters) recordLeaseLatency(metadata *jetstream.MsgMetadata, acquiredAt time.Time) {
	if metadata.Timestamp.IsZero() {
		return
	}
	latency := acquiredAt.Sub(metadata.Timestamp)
	if latency < 0 {
		latency = 0
	}
	m.enqueueToLeaseSamples.Add(1)
	m.enqueueToLeaseTotal.Add(int64(latency))
	for previous := m.enqueueToLeaseMaximum.Load(); int64(latency) > previous; previous = m.enqueueToLeaseMaximum.Load() {
		if m.enqueueToLeaseMaximum.CompareAndSwap(previous, int64(latency)) {
			break
		}
	}
}

// Metrics returns counters accumulated since this Worker was created.
func (w *Worker) Metrics() Metrics {
	view := Metrics{
		LeaseAcquisitions:     w.metrics.leaseAcquisitions.Load(),
		LeaseContentions:      w.metrics.leaseContentions.Load(),
		LeaseAcquireFailures:  w.metrics.leaseAcquireFailures.Load(),
		FencingEvents:         w.metrics.fencingEvents.Load(),
		Redeliveries:          w.metrics.redeliveries.Load(),
		EnqueueToLeaseSamples: w.metrics.enqueueToLeaseSamples.Load(),
		EnqueueToLeaseTotal:   time.Duration(w.metrics.enqueueToLeaseTotal.Load()),
		EnqueueToLeaseMaximum: time.Duration(w.metrics.enqueueToLeaseMaximum.Load()),
		TimersScheduled:       w.metrics.timersScheduled.Load(),
		TimersFired:           w.metrics.timersFired.Load(),
		TimerLateTotal:        time.Duration(w.metrics.timerLateTotal.Load()),
		TimerLateMaximum:      time.Duration(w.metrics.timerLateMaximum.Load()),
		CancelledTimerNoOps:   w.metrics.cancelledTimerNoOps.Load(),
	}
	for i := range view.TimerLateBuckets {
		view.TimerLateBuckets[i] = w.metrics.timerLateBuckets[i].Load()
	}
	return view
}
