package worker

import "time"

// PartitionEvent records local dispatch progress before invocation identity is
// available. Begin/end pairs share Attempt and Operation. SlotsReserved includes
// the reservation for the current pull; it is not an active-handler count.
// A pull ends after its batch closes or cancellation, not when FetchOne returns.
// These observations do not prove broker processing or message availability.
type PartitionEvent struct {
	At            time.Time
	Duration      time.Duration
	Worker        string
	Partition     uint32
	Attempt       uint64
	Operation     string
	Phase         string
	SlotsReserved int
	Concurrency   int
	Error         string
}

// WithPartitionObserver enables slot-wait and full pull-lifetime observations.
// The callback must be quick and safe for concurrent RunPartition calls.
func WithPartitionObserver(observe func(PartitionEvent)) Option {
	return func(w *Worker) error {
		w.partitionObserver = observe
		return nil
	}
}

func partitionObservation(observe func(PartitionEvent), worker string, partition uint32, attempt uint64, operation string, concurrency int, slots chan struct{}) func(error) {
	if observe == nil {
		return func(error) {}
	}
	start := time.Now()
	event := PartitionEvent{At: start, Worker: worker, Partition: partition, Attempt: attempt, Operation: operation, Phase: "begin", SlotsReserved: len(slots), Concurrency: concurrency}
	observe(event)
	return func(err error) {
		event.At = time.Now()
		event.Duration = event.At.Sub(start)
		event.Phase = "end"
		event.SlotsReserved = len(slots)
		if err != nil {
			event.Error = err.Error()
		}
		observe(event)
	}
}
