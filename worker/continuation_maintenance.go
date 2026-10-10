package worker

import (
	"context"
	"errors"

	"fmt"
	"github.com/nats-io/nats.go/jetstream"
	"time"

	"js-wf/journal"
	"js-wf/lease"
)

const continuationVerificationRecords = uint64(128)

// Durable maintenance uses the delivery's cancellation/deadline. Its individual
// batches and requests remain bounded below, and each batch renews ownership.
// Legacy handoffs retain their existing whole-publication deadline.
func (w *Worker) continuationPublicationContext(parent context.Context) (context.Context, context.CancelFunc) {
	if w.graphJournal != nil && w.graphJournal.ArchiveCheckpoints() && w.graphJournal.HasCompactionCheckpointStorage() {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, 15*time.Second)
}

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
	stored := w.graphJournal.HasCompactionCheckpointStorage()
	var revision uint64
	var operation *journal.CheckpointCompaction
	openCtx, stopOpen := context.WithTimeout(ctx, 15*time.Second)
	if stored {
		renewCtx, stopRenew := context.WithTimeout(openCtx, 3*time.Second)
		err = owner.Renew(renewCtx)
		stopRenew()
		if err == nil {
			operation, revision, err = w.graphJournal.ResumeStoredCheckpointCompaction(openCtx, typ, id, runtime, tail)
		}
	}
	if err == nil && operation == nil {
		operation, err = w.graphJournal.BeginCheckpointCompaction(openCtx, typ, id, runtime, tail)
	}
	stopOpen()
	if err != nil {
		// Explicitly discard an observed obsolete descriptor, then stop. The
		// next delivery can capture a fresh stage; unknown/corrupt input stays.
		if stored && revision != 0 && errors.Is(err, journal.ErrStale) {
			if cleanupErr := w.deleteGraphCompactionCheckpoint(ctx, typ, id, owner, runtime, tail, revision, ops); cleanupErr != nil {
				return cleanupErr
			}
		}
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
		intentCtx, stopIntent := context.WithTimeout(ctx, 3*time.Second)
		intentStarted, intentErr := operation.BeginIntentRenewalIfNeeded(intentCtx)
		stopIntent()
		if intentStarted || intentErr != nil {
			ops.finish(started, "continuation_archive_renew_begin", runtime.Index, journal.StepCompleted, intentErr)
		}
		if intentErr != nil {
			return intentErr
		}
		if stored && intentStarted {
			if revision, err = w.saveGraphCompactionCheckpoint(ctx, owner, operation, runtime, revision, ops); err != nil {
				return err
			}
		}
		started = ops.begin()
		before := operation.Phase()
		phase := "continuation_archive_" + before + "_batch"
		batchCtx, stopBatch := context.WithTimeout(ctx, 15*time.Second)
		done, advanceErr := operation.Advance(batchCtx, budget, 2*budget)
		stopBatch()
		ops.finish(started, phase, runtime.Index, journal.StepCompleted, advanceErr)
		if advanceErr != nil {
			return advanceErr
		}
		if done {
			if stored && revision != 0 {
				return w.deleteGraphCompactionCheckpoint(ctx, typ, id, owner, runtime, tail, revision, ops)
			}
			return nil
		}
		if stored && (before == "confirm" && operation.Phase() == "stage" || before == "stage" || before == "renew" && operation.Phase() != "renew") {
			if revision, err = w.saveGraphCompactionCheckpoint(ctx, owner, operation, runtime, revision, ops); err != nil {
				return err
			}
		}
	}
}

func (w *Worker) saveGraphCompactionCheckpoint(ctx context.Context, owner *lease.Lease, operation *journal.CheckpointCompaction, runtime journal.RuntimeCheckpoint, revision uint64, ops *deliveryOperations) (uint64, error) {
	started := ops.begin()
	renewCtx, stopRenew := context.WithTimeout(ctx, 3*time.Second)
	_, timing, err := ops.renew(renewCtx, owner, 0)
	stopRenew()
	ops.finish(started, "lease_renew_archive_checkpoint_save", runtime.Index, journal.StepCompleted, err, timing)
	if err != nil {
		return 0, err
	}
	started = ops.begin()
	saveCtx, stopSave := context.WithTimeout(ctx, 3*time.Second)
	next, err := operation.SaveCheckpoint(saveCtx, revision)
	stopSave()
	ops.finish(started, "continuation_archive_checkpoint_save", runtime.Index, journal.StepCompleted, err)
	return next, err
}

func (w *Worker) deleteGraphCompactionCheckpoint(ctx context.Context, typ, id string, owner *lease.Lease, runtime journal.RuntimeCheckpoint, tail, revision uint64, ops *deliveryOperations) error {
	started := ops.begin()
	renewCtx, stopRenew := context.WithTimeout(ctx, 3*time.Second)
	_, timing, err := ops.renew(renewCtx, owner, 0)
	stopRenew()
	ops.finish(started, "lease_renew_archive_checkpoint_delete", runtime.Index, journal.StepCompleted, err, timing)
	if err != nil {
		return err
	}
	started = ops.begin()
	deleteCtx, stopDelete := context.WithTimeout(ctx, 3*time.Second)
	err = w.graphJournal.DeleteCompactionCheckpoint(deleteCtx, typ, id, runtime, tail, revision)
	stopDelete()
	ops.finish(started, "continuation_archive_checkpoint_delete", runtime.Index, journal.StepCompleted, err)
	return err
}

// Recover before opening the delivery reader, which would invalidate the saved
// original head. A repaired archive dispatches a fresh delivery before execution.
func (w *Worker) recoverStoredGraphCompaction(ctx context.Context, typ, id string, owner *lease.Lease, input *jetstream.RawStreamMsg, ops *deliveryOperations) (bool, error) {
	lookupCtx, stopLookup := context.WithTimeout(ctx, 5*time.Second)
	handoff, err := w.graphJournal.InspectCheckpointCompactionHandoff(lookupCtx, typ, id, input)
	stopLookup()
	if err != nil || handoff == nil {
		return false, err
	}
	workCtx, stopWork := w.continuationPublicationContext(ctx)
	defer stopWork()
	renewCtx, stopRenew := context.WithTimeout(workCtx, 3*time.Second)
	err = owner.Renew(renewCtx)
	stopRenew()
	if err != nil {
		return false, err
	}
	if handoff.Complete {
		// Canonical publication is already complete. An observed descriptor is
		// obsolete even if its bytes are malformed; never use it for execution.
		readCtx, stopRead := context.WithTimeout(workCtx, 3*time.Second)
		_, revision, readErr := w.graphJournal.ResumeStoredCheckpointCompaction(readCtx, typ, id, handoff.Runtime, handoff.Tail)
		stopRead()
		if readErr != nil && !(revision != 0 && (errors.Is(readErr, journal.ErrStale) || errors.Is(readErr, journal.ErrGap))) {
			return false, readErr
		}
		if revision != 0 {
			if err := w.deleteGraphCompactionCheckpoint(workCtx, typ, id, owner, handoff.Runtime, handoff.Tail, revision, ops); err != nil {
				return false, err
			}
		}
		return false, nil
	}
	if err := w.compactGraphCheckpointBatches(workCtx, typ, id, owner, handoff.Runtime, handoff.Tail, ops); err != nil {
		return true, err
	}
	renewCtx, stopRenew = context.WithTimeout(workCtx, 3*time.Second)
	err = owner.Renew(renewCtx)
	stopRenew()
	if err != nil {
		return true, err
	}
	dispatchCtx, stopDispatch := context.WithTimeout(workCtx, 5*time.Second)
	defer stopDispatch()
	return true, w.client.Enqueue(dispatchCtx, typ, id, "")
}
