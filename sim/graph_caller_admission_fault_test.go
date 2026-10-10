package sim

import (
	"context"

	"js-wf/internal/graphpublication"
)

// This actor-local fault rejects publication before commitment. It affects
// only the second caller's finite admission attempts, leaving other clients,
// discovery, workers and the subsequent fresh caller on ordinary adapters.
type callerAdmissionConflictPort struct {
	yieldingGraphPort
	scheduler *Scheduler
}

func (p callerAdmissionConflictPort) CASRoot(ctx context.Context, key string, head uint64, next graphpublication.Root) (graphpublication.Root, error) {
	return graphActorCall(ctx, p.yield, "graph_cas_root", func() (graphpublication.Root, error) {
		p.scheduler.RecordTransport(TransportEvent{Operation: "graph_caller_admission_conflict", Subject: key, Expected: head, Outcome: "reject_before_commit"})
		return graphpublication.Root{}, graphpublication.ErrConflict
	})
}
