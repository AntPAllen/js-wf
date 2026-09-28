package sim

import (
	"context"
	"fmt"
	"time"

	"js-wf/journal"
)

type actorResult struct {
	name string
	err  error
}

// AppendActor runs production append logic through a cooperative transport.
// Its function must yield through AppendPort for shared-state operations.
type AppendActor struct {
	Name string
	Run  func(context.Context, journal.AppendPort) error
}

type yieldingAppendPort struct {
	yield     YieldFunc
	transport journal.AppendPort
}

var _ journal.AppendPort = yieldingAppendPort{}

func (p yieldingAppendPort) Last(ctx context.Context, subject string) (journal.AppendTail, error) {
	var tail journal.AppendTail
	var operationErr error
	if err := p.yield(ctx, "last", func() { tail, operationErr = p.transport.Last(ctx, subject) }); err != nil {
		return journal.AppendTail{}, err
	}
	return tail, operationErr
}

func (p yieldingAppendPort) Publish(ctx context.Context, subject string, data []byte, expected uint64) (uint64, error) {
	var sequence uint64
	var operationErr error
	payload := append([]byte(nil), data...)
	if err := p.yield(ctx, "publish", func() { sequence, operationErr = p.transport.Publish(ctx, subject, payload, expected) }); err != nil {
		return 0, err
	}
	return sequence, operationErr
}

func (p yieldingAppendPort) Wait(ctx context.Context, delay time.Duration) error {
	var operationErr error
	if err := p.yield(ctx, "wait", func() { operationErr = p.transport.Wait(ctx, delay) }); err != nil {
		return err
	}
	return operationErr
}

// RunAppendActors schedules production journal appends at transport calls.
func RunAppendActors(ctx context.Context, schedule *Scheduler, transport journal.AppendPort, actors []AppendActor) (map[string]error, error) {
	if transport == nil {
		return nil, fmt.Errorf("simulation needs an append transport")
	}
	cooperative := make([]CooperativeActor, 0, len(actors))
	for _, actor := range actors {
		if actor.Run == nil {
			return nil, fmt.Errorf("simulation actor %q has no run function", actor.Name)
		}
		actor := actor
		cooperative = append(cooperative, CooperativeActor{Name: actor.Name, Run: func(ctx context.Context, yield YieldFunc) error {
			return actor.Run(ctx, yieldingAppendPort{yield: yield, transport: transport})
		}})
	}
	return RunCooperative(ctx, schedule, cooperative)
}
