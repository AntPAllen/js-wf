package graphpublication

import (
	"context"
	"time"
)

// CompactionStageRenewal owns a frozen staging handle until every publication
// scope has been renewed. It exposes no prepared plan or verification proof.
// Save the staging checkpoint and requested expiry before beginning: uncertain
// updates require fresh Resume and renewal with that same requested expiry.
// Use the staging and renewal handles from one goroutine, without other writers
// under their token. Namespace enumeration has the underlying renewal's limits.
type CompactionStageRenewal struct {
	stage     *CompactionStage
	operation *CompactionIntentRenewal
	done      bool
	err       error
}

func (s *CompactionStage) BeginIntentRenewal(ctx context.Context, now func() time.Time, expires time.Time) (*CompactionStageRenewal, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.renewal != nil {
		return nil, ErrConflict
	}
	operation, err := s.protocol.BeginCompactionIntentRenewal(ctx, s.result(), now, expires)
	if err != nil {
		return nil, err
	}
	r := &CompactionStageRenewal{stage: s, operation: operation}
	s.renewal = r
	return r, nil
}

func (r *CompactionStageRenewal) ExaminedScopes() uint64 { return r.operation.ExaminedScopes() }
func (r *CompactionStageRenewal) RenewedScopes() uint64  { return r.operation.RenewedScopes() }

// Advance blocks staging/checkpoint emission until full renewal succeeds.
// An in-batch failure also blocks the old stage permanently; a saved checkpoint
// can be resumed and exactly matching updates reconciled before the old expiry.
// Upfront cancellation or invalid budgets preserve the pending operation.
func (r *CompactionStageRenewal) Advance(ctx context.Context, maxScopes uint64) (bool, error) {
	if r.err != nil {
		return false, r.err
	}
	if r.done {
		return true, nil
	}
	if r.stage.renewal != r {
		return false, ErrConflict
	}
	_, done, err := r.operation.Advance(ctx, maxScopes)
	if err != nil {
		if r.operation.err != nil {
			r.err = err
			r.stage.err = err
		}
		return false, err
	}
	if !done {
		return false, nil
	}
	r.stage.prepared.expires = r.operation.expires
	r.stage.renewal = nil
	r.done = true
	return true, nil
}
