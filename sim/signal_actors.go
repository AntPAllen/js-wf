package sim

import (
	"context"
	"fmt"

	"js-wf/journal"
	"js-wf/reconcile"

	"github.com/nats-io/nats.go/jetstream"
)

// SignalActor can run a production scanner through a yielding port and use
// YieldFunc for fixture events such as a committed signal publish.
type SignalActor struct {
	Name string
	Run  func(context.Context, YieldFunc, reconcile.SignalScanPort) error
}

type yieldingSignalScanPort struct {
	yield     YieldFunc
	transport reconcile.SignalScanPort
}

var _ reconcile.SignalScanPort = yieldingSignalScanPort{}

func (p yieldingSignalScanPort) GetSignal(ctx context.Context, sequence uint64) (*jetstream.RawStreamMsg, error) {
	var message *jetstream.RawStreamMsg
	var operationErr error
	if err := p.yield(ctx, "get_signal", func() { message, operationErr = p.transport.GetSignal(ctx, sequence) }); err != nil {
		return nil, err
	}
	return message, operationErr
}

func (p yieldingSignalScanPort) LastSignalSequence(ctx context.Context) (uint64, error) {
	var sequence uint64
	var operationErr error
	if err := p.yield(ctx, "signal_stream_info", func() { sequence, operationErr = p.transport.LastSignalSequence(ctx) }); err != nil {
		return 0, err
	}
	return sequence, operationErr
}

func (p yieldingSignalScanPort) ReadJournal(ctx context.Context, typ, id string) ([]journal.Record, error) {
	var records []journal.Record
	var operationErr error
	if err := p.yield(ctx, "read_journal", func() { records, operationErr = p.transport.ReadJournal(ctx, typ, id) }); err != nil {
		return nil, err
	}
	return records, operationErr
}

func (p yieldingSignalScanPort) LastInvocation(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	var message *jetstream.RawStreamMsg
	var operationErr error
	if err := p.yield(ctx, "last_invocation", func() { message, operationErr = p.transport.LastInvocation(ctx, subject) }); err != nil {
		return nil, err
	}
	return message, operationErr
}

func (p yieldingSignalScanPort) EnqueueSignal(ctx context.Context, typ, id string, sequence uint64) error {
	var operationErr error
	if err := p.yield(ctx, "enqueue_signal", func() { operationErr = p.transport.EnqueueSignal(ctx, typ, id, sequence) }); err != nil {
		return err
	}
	return operationErr
}

// RunSignalActors interleaves production signal-scan calls with fixture
// events at explicit transport boundaries.
func RunSignalActors(ctx context.Context, schedule *Scheduler, transport reconcile.SignalScanPort, actors []SignalActor) (map[string]error, error) {
	if transport == nil {
		return nil, fmt.Errorf("simulation needs a signal scan transport")
	}
	cooperative := make([]CooperativeActor, 0, len(actors))
	for _, actor := range actors {
		if actor.Run == nil {
			return nil, fmt.Errorf("simulation actor %q has no run function", actor.Name)
		}
		actor := actor
		cooperative = append(cooperative, CooperativeActor{Name: actor.Name, Run: func(ctx context.Context, yield YieldFunc) error {
			return actor.Run(ctx, yield, yieldingSignalScanPort{yield: yield, transport: transport})
		}})
	}
	return RunCooperative(ctx, schedule, cooperative)
}
