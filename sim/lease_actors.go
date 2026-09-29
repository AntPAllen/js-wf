package sim

import (
	"context"
	"fmt"
	"time"

	"js-wf/lease"
)

type LeaseActor struct {
	Name            string
	WallClockOffset time.Duration
	Run             func(context.Context, lease.KVPort) error
}

type yieldingKVPort struct {
	yield     YieldFunc
	transport lease.KVPort
	offset    time.Duration
}

var _ lease.KVPort = yieldingKVPort{}

func (p yieldingKVPort) Create(ctx context.Context, key string, value []byte) (uint64, error) {
	var revision uint64
	var operationErr error
	data := append([]byte(nil), value...)
	if err := p.yield(ctx, "kv_create", func() { revision, operationErr = p.transport.Create(ctx, key, data) }); err != nil {
		return 0, err
	}
	return revision, operationErr
}

func (p yieldingKVPort) Get(ctx context.Context, key string) (lease.KVEntry, error) {
	var entry lease.KVEntry
	var operationErr error
	if err := p.yield(ctx, "kv_get", func() { entry, operationErr = p.transport.Get(ctx, key) }); err != nil {
		return lease.KVEntry{}, err
	}
	return entry, operationErr
}

func (p yieldingKVPort) Update(ctx context.Context, key string, value []byte, expected uint64) (uint64, error) {
	var revision uint64
	var operationErr error
	data := append([]byte(nil), value...)
	if err := p.yield(ctx, "kv_update", func() { revision, operationErr = p.transport.Update(ctx, key, data, expected) }); err != nil {
		return 0, err
	}
	return revision, operationErr
}

func (p yieldingKVPort) Delete(ctx context.Context, key string, expected uint64) error {
	var operationErr error
	if err := p.yield(ctx, "kv_delete", func() { operationErr = p.transport.Delete(ctx, key, expected) }); err != nil {
		return err
	}
	return operationErr
}

func (p yieldingKVPort) Now() time.Time { return p.transport.Now().Add(p.offset) }

// RunLeaseActors schedules production lease operations at KV boundaries.
func RunLeaseActors(ctx context.Context, schedule *Scheduler, transport lease.KVPort, actors []LeaseActor) (map[string]error, error) {
	if transport == nil {
		return nil, fmt.Errorf("simulation needs a KV transport")
	}
	cooperative := make([]CooperativeActor, 0, len(actors))
	for _, actor := range actors {
		if actor.Run == nil {
			return nil, fmt.Errorf("simulation actor %q has no run function", actor.Name)
		}
		actor := actor
		if actor.WallClockOffset != 0 {
			schedule.RecordTransport(TransportEvent{Operation: "actor_clock_offset", Subject: actor.Name, Outcome: actor.WallClockOffset.String(), AtMillis: schedule.NowMillis()})
		}
		cooperative = append(cooperative, CooperativeActor{Name: actor.Name, Run: func(ctx context.Context, yield YieldFunc) error {
			return actor.Run(ctx, yieldingKVPort{yield: yield, transport: transport, offset: actor.WallClockOffset})
		}})
	}
	return RunCooperative(ctx, schedule, cooperative)
}
