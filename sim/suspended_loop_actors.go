package sim

import (
	"context"
	"fmt"

	"js-wf/journal"
	"js-wf/reconcile"

	"github.com/nats-io/nats.go/jetstream"
)

// SuspendedLoopActor runs a production suspended scanner through yielding ports.
type SuspendedLoopActor struct {
	Name string
	Run  func(context.Context, reconcile.LoopPort, reconcile.SuspendedScanPort) error
}

type yieldingSuspendedScanPort struct {
	yield YieldFunc
	port  reconcile.SuspendedScanPort
}

var _ reconcile.SuspendedScanPort = yieldingSuspendedScanPort{}

func (p yieldingSuspendedScanPort) LastInvocationSequence(ctx context.Context) (uint64, error) {
	var sequence uint64
	var operationErr error
	if err := p.yield(ctx, "invocation_stream_info", func() { sequence, operationErr = p.port.LastInvocationSequence(ctx) }); err != nil {
		return 0, err
	}
	return sequence, operationErr
}

func (p yieldingSuspendedScanPort) GetInvocation(ctx context.Context, sequence uint64) (*jetstream.RawStreamMsg, error) {
	var message *jetstream.RawStreamMsg
	var operationErr error
	if err := p.yield(ctx, "invocation_get", func() { message, operationErr = p.port.GetInvocation(ctx, sequence) }); err != nil {
		return nil, err
	}
	return message, operationErr
}

func (p yieldingSuspendedScanPort) ReadJournal(ctx context.Context, typ, id string) ([]journal.Record, error) {
	var records []journal.Record
	var operationErr error
	if err := p.yield(ctx, "journal_read", func() { records, operationErr = p.port.ReadJournal(ctx, typ, id) }); err != nil {
		return nil, err
	}
	return records, operationErr
}

func (p yieldingSuspendedScanPort) GetSignalAfter(ctx context.Context, subject string, sequence uint64) (*jetstream.RawStreamMsg, error) {
	var message *jetstream.RawStreamMsg
	var operationErr error
	if err := p.yield(ctx, "signal_get_after", func() { message, operationErr = p.port.GetSignalAfter(ctx, subject, sequence) }); err != nil {
		return nil, err
	}
	return message, operationErr
}

func (p yieldingSuspendedScanPort) EnqueueSuspended(ctx context.Context, typ, id string, sequence uint64) error {
	var operationErr error
	if err := p.yield(ctx, "suspended_enqueue", func() { operationErr = p.port.EnqueueSuspended(ctx, typ, id, sequence) }); err != nil {
		return err
	}
	return operationErr
}

// RunSuspendedLoopActors interleaves production suspended loops at lease,
// cursor, cadence, journal, signal, and wakeup operations. Callers use a
// one-record scan budget so each actor offers one transport turn at a time.
func RunSuspendedLoopActors(ctx context.Context, schedule *Scheduler, loop reconcile.LoopPort, suspended reconcile.SuspendedScanPort, actors []SuspendedLoopActor) (map[string]error, error) {
	if loop == nil || suspended == nil {
		return nil, fmt.Errorf("simulation needs loop and suspended scanner transports")
	}
	cooperative := make([]CooperativeActor, 0, len(actors))
	for _, actor := range actors {
		if actor.Run == nil {
			return nil, fmt.Errorf("simulation actor %q has no run function", actor.Name)
		}
		actor := actor
		cooperative = append(cooperative, CooperativeActor{Name: actor.Name, Run: func(ctx context.Context, yield YieldFunc) error {
			return actor.Run(ctx,
				yieldingLoopPort{actorCtx: ctx, yield: yield, port: loop},
				yieldingSuspendedScanPort{yield: yield, port: suspended})
		}})
	}
	return RunCooperative(ctx, schedule, cooperative)
}
