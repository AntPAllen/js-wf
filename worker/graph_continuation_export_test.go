package worker

import (
	"context"
	"time"

	"js-wf/journal"
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

// GraphCompactionDeliveryReleaseForTest exposes only release/cleanup closures for
// uncertain-reader-release controls; graph delivery internals stay private.
func GraphCompactionDeliveryReleaseForTest(view *journal.GraphView) (func(context.Context) error, func(context.Context) error, func() bool) {
	g := &graphDelivery{view: view}
	return g.releaseForCompaction, g.close, func() bool { return g.compactionReleaseAttempted }
}
