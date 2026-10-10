package worker

import (
	"context"
	"time"

	"js-wf/lease"
)

// ExecuteGraphContinuationForTest keeps experimental registration confined to
// test binaries while external tests supply the seeded in-memory transports.
func ExecuteGraphContinuationForTest(ctx context.Context, w *Worker, typ, id string, owner *lease.Lease, stages map[string]ContinuationHandler) error {
	w.continuations = map[string]map[string]ContinuationHandler{typ: stages}
	var noOp bool
	return w.execute(ctx, typ, id, owner, time.Time{}, timerWakeup{}, &noOp, w.deliveryOperations(typ, id, 0, 0))
}

// ExecuteGraphContinuationBatchesForTest changes only the private verification
// record budget. Public continuation admission and publication deadlines stay
// unchanged while external model tests force several maintenance batches.
func ExecuteGraphContinuationBatchesForTest(ctx context.Context, w *Worker, typ, id string, owner *lease.Lease, stages map[string]ContinuationHandler, budget uint64) error {
	w.continuationVerifyBatch = budget
	return ExecuteGraphContinuationForTest(ctx, w, typ, id, owner, stages)
}
