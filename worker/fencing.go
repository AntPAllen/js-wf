package worker

import "time"

// FencingEvent identifies each increment of the worker fencing counter.
// Reason identifies the failed ownership check; it does not assert a NATS
// server root cause. Observers must be fast and concurrency-safe.
type FencingEvent struct {
	At          time.Time
	Worker      string
	Type        string
	ID          string
	RunSequence uint64
	Delivery    uint64
	Epoch       uint64
	Reason      string
	Error       string
}

func WithFencingObserver(observe func(FencingEvent)) Option {
	return func(w *Worker) error { w.fencingObserver = observe; return nil }
}

func (w *Worker) recordFencing(event FencingEvent, cause error) {
	w.metrics.fencingEvents.Add(1)
	if w.fencingObserver != nil {
		event.At, event.Worker = time.Now().UTC(), w.ID
		if cause != nil {
			event.Error = cause.Error()
		}
		w.fencingObserver(event)
	}
}
