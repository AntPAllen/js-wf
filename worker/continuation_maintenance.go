package worker

import (
	"context"
	"fmt"
	"time"

	"js-wf/journal"
	"js-wf/lease"
)

const continuationVerificationRecords = uint64(128)

// publishGraphCheckpointBatches uses the delivery's existing publication parent.
// Each batch rechecks lease ownership and has a bounded request context. The
// outer handoff deadline is unchanged; this does not persist scan progress or
// extend a delivery after parent cancellation, lease loss or an unknown mutation.
func (w *Worker) publishGraphCheckpointBatches(ctx context.Context, typ, id string, owner *lease.Lease, runtime journal.RuntimeCheckpoint, tail uint64, ops *deliveryOperations) (err error) {
	openCtx, stopOpen := context.WithTimeout(ctx, 15*time.Second)
	publication, err := w.graphJournal.BeginCheckpointPublication(openCtx, typ, id, runtime, tail)
	stopOpen()
	if err != nil {
		return err
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer stop()
		if closeErr := publication.Close(cleanup); err == nil && closeErr != nil {
			err = fmt.Errorf("%w: checkpoint maintenance release: %w", journal.ErrUnknown, closeErr)
		}
	}()
	budget := w.continuationVerifyBatch
	if budget == 0 {
		budget = continuationVerificationRecords
	}
	for {
		if err = ctx.Err(); err != nil {
			return err
		}
		started := ops.begin()
		renewCtx, stopRenew := context.WithTimeout(ctx, 3*time.Second)
		_, timing, renewErr := ops.renew(renewCtx, owner, 0)
		stopRenew()
		ops.finish(started, "lease_renew_checkpoint_batch", publication.NextIndex(), journal.StepCompleted, renewErr, timing)
		if renewErr != nil {
			return renewErr
		}
		started = ops.begin()
		batchCtx, stopBatch := context.WithTimeout(ctx, 15*time.Second)
		done, advanceErr := publication.Advance(batchCtx, budget)
		stopBatch()
		ops.finish(started, "continuation_checkpoint_verify_batch", publication.NextIndex(), journal.StepCompleted, advanceErr)
		if advanceErr != nil {
			return advanceErr
		}
		if done {
			return nil
		}
	}
}

func (w *Worker) compactGraphCheckpointBatches(ctx context.Context, typ, id string, owner *lease.Lease, runtime journal.RuntimeCheckpoint, tail uint64, ops *deliveryOperations) (err error) {
	openCtx, stopOpen := context.WithTimeout(ctx, 15*time.Second)
	operation, err := w.graphJournal.BeginCheckpointCompaction(openCtx, typ, id, runtime, tail)
	stopOpen()
	if err != nil {
		return err
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer stop()
		if closeErr := operation.Close(cleanup); err == nil && closeErr != nil {
			err = fmt.Errorf("%w: archive maintenance release: %w", journal.ErrUnknown, closeErr)
		}
	}()
	budget := w.continuationVerifyBatch
	if budget == 0 {
		budget = continuationVerificationRecords
	}
	for {
		if err = ctx.Err(); err != nil {
			return err
		}
		started := ops.begin()
		renewCtx, stopRenew := context.WithTimeout(ctx, 3*time.Second)
		_, timing, renewErr := ops.renew(renewCtx, owner, 0)
		stopRenew()
		ops.finish(started, "lease_renew_archive_batch", runtime.Index, journal.StepCompleted, renewErr, timing)
		if renewErr != nil {
			return renewErr
		}
		started = ops.begin()
		phase := "continuation_archive_" + operation.Phase() + "_batch"
		batchCtx, stopBatch := context.WithTimeout(ctx, 15*time.Second)
		done, advanceErr := operation.Advance(batchCtx, budget, 2*budget)
		stopBatch()
		ops.finish(started, phase, runtime.Index, journal.StepCompleted, advanceErr)
		if advanceErr != nil {
			return advanceErr
		}
		if done {
			return nil
		}
	}
}
