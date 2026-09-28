package sim

import "time"

// WorkerTransport shares committed WF_RUN messages between the start/signal
// model and the durable consumer used by production Worker.handle.
type WorkerTransport struct {
	*SignalTransport
	Dispatch *DispatchTransport
}

func NewWorkerTransport(schedule *Scheduler, ackWait time.Duration) *WorkerTransport {
	signals := NewSignalTransport(schedule)
	dispatch := NewDispatchTransport(schedule, ackWait)
	signals.StartTransport.OnRunCommit(func(run Message) {
		dispatch.PublishRun(run.Subject, run.Data)
	})
	return &WorkerTransport{SignalTransport: signals, Dispatch: dispatch}
}
