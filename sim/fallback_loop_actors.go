package sim

import (
	"context"
	"fmt"

	"js-wf/reconcile"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// FallbackLoopActor runs a production fallback scanner through yielding ports.
type FallbackLoopActor struct {
	Name string
	Run  func(context.Context, reconcile.LoopPort, reconcile.FallbackTimerScanPort) error
}

type yieldingFallbackTimerPort struct {
	yield YieldFunc
	port  reconcile.FallbackTimerScanPort
}

var _ reconcile.FallbackTimerScanPort = yieldingFallbackTimerPort{}

func (p yieldingFallbackTimerPort) LastTimerSequence(ctx context.Context) (uint64, error) {
	var sequence uint64
	var operationErr error
	if err := p.yield(ctx, "timer_stream_info", func() { sequence, operationErr = p.port.LastTimerSequence(ctx) }); err != nil {
		return 0, err
	}
	return sequence, operationErr
}

func (p yieldingFallbackTimerPort) GetTimer(ctx context.Context, sequence uint64) (*jetstream.RawStreamMsg, error) {
	var message *jetstream.RawStreamMsg
	var operationErr error
	if err := p.yield(ctx, "timer_get", func() { message, operationErr = p.port.GetTimer(ctx, sequence) }); err != nil {
		return nil, err
	}
	return message, operationErr
}

func (p yieldingFallbackTimerPort) StateValue(ctx context.Context, key string) ([]byte, error) {
	var value []byte
	var operationErr error
	if err := p.yield(ctx, "timer_state_get", func() { value, operationErr = p.port.StateValue(ctx, key) }); err != nil {
		return nil, err
	}
	return value, operationErr
}

func (p yieldingFallbackTimerPort) PublishWakeup(ctx context.Context, message *nats.Msg, messageID string) error {
	var operationErr error
	if err := p.yield(ctx, "timer_wakeup_publish", func() { operationErr = p.port.PublishWakeup(ctx, message, messageID) }); err != nil {
		return err
	}
	return operationErr
}

func (p yieldingFallbackTimerPort) DeleteTimer(ctx context.Context, sequence uint64) error {
	var operationErr error
	if err := p.yield(ctx, "timer_delete", func() { operationErr = p.port.DeleteTimer(ctx, sequence) }); err != nil {
		return err
	}
	return operationErr
}

// RunFallbackLoopActors interleaves production fallback loops at lease,
// cursor, cadence, and timer stream operations.
func RunFallbackLoopActors(ctx context.Context, schedule *Scheduler, loop reconcile.LoopPort, timers reconcile.FallbackTimerScanPort, actors []FallbackLoopActor) (map[string]error, error) {
	if loop == nil || timers == nil {
		return nil, fmt.Errorf("simulation needs loop and timer transports")
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
				yieldingFallbackTimerPort{yield: yield, port: timers})
		}})
	}
	return RunCooperative(ctx, schedule, cooperative)
}
