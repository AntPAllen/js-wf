package sim

import (
	"context"
	"fmt"
	"time"

	"js-wf/reconcile"
)

type ReconcileLoopActor struct {
	Name string
	Run  func(context.Context, reconcile.LoopPort, reconcile.StartScanPort) error
}

type yieldingLoopPort struct {
	actorCtx context.Context
	yield    YieldFunc
	port     reconcile.LoopPort
}

var _ reconcile.LoopPort = yieldingLoopPort{}

func (p yieldingLoopPort) Prepare(ctx context.Context) error {
	var operationErr error
	if err := p.yield(ctx, "loop_prepare", func() { operationErr = p.port.Prepare(ctx) }); err != nil {
		return err
	}
	return operationErr
}

func (p yieldingLoopPort) Acquire(ctx context.Context, kind, workerID string) (reconcile.LoopLease, error) {
	var acquired reconcile.LoopLease
	var operationErr error
	if err := p.yield(ctx, "loop_acquire", func() { acquired, operationErr = p.port.Acquire(ctx, kind, workerID) }); err != nil {
		return nil, err
	}
	if operationErr != nil {
		return nil, operationErr
	}
	return yieldingLoopLease{actorCtx: p.actorCtx, yield: p.yield, lease: acquired}, nil
}

func (p yieldingLoopPort) LoadCursor(ctx context.Context, kind string) (uint64, uint64, error) {
	var sequence, revision uint64
	var operationErr error
	if err := p.yield(ctx, "cursor_load", func() { sequence, revision, operationErr = p.port.LoadCursor(ctx, kind) }); err != nil {
		return 0, 0, err
	}
	return sequence, revision, operationErr
}

func (p yieldingLoopPort) SaveCursor(ctx context.Context, kind string, next, revision uint64) (uint64, error) {
	var saved uint64
	var operationErr error
	if err := p.yield(ctx, "cursor_save", func() { saved, operationErr = p.port.SaveCursor(ctx, kind, next, revision) }); err != nil {
		return 0, err
	}
	return saved, operationErr
}

func (p yieldingLoopPort) Wait(ctx context.Context, delay time.Duration) error {
	var operationErr error
	if err := p.yield(ctx, "loop_wait", func() { operationErr = p.port.Wait(ctx, delay) }); err != nil {
		return err
	}
	return operationErr
}

type yieldingLoopLease struct {
	actorCtx context.Context
	yield    YieldFunc
	lease    reconcile.LoopLease
}

var _ reconcile.LoopLease = yieldingLoopLease{}

func (l yieldingLoopLease) Renew(ctx context.Context) error {
	var operationErr error
	if err := l.yield(ctx, "lease_renew", func() { operationErr = l.lease.Renew(ctx) }); err != nil {
		return err
	}
	return operationErr
}

func (l yieldingLoopLease) Release(ctx context.Context) error {
	var operationErr error
	// The loop releases with a fresh cleanup context after its run context is
	// cancelled. Keep that release schedulable until the actor itself exits.
	if err := l.yield(l.actorCtx, "lease_release", func() { operationErr = l.lease.Release(ctx) }); err != nil {
		return err
	}
	return operationErr
}

// RunReconcileLoopActors interleaves two or more production scanner loops at
// lease, cursor, cadence, and start-scan transport operations.
func RunReconcileLoopActors(ctx context.Context, schedule *Scheduler, loop reconcile.LoopPort, starts reconcile.StartScanPort, actors []ReconcileLoopActor) (map[string]error, error) {
	if loop == nil || starts == nil {
		return nil, fmt.Errorf("simulation needs loop and scanner transports")
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
				yieldingStartScanPort{yield: yield, transport: starts})
		}})
	}
	return RunCooperative(ctx, schedule, cooperative)
}
