package graphpublication

import (
	"context"
	"reflect"
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
	commit    *CompactionCommit
	operation *CompactionIntentRenewal
	done      bool
	err       error
}

// BeginIntentRenewal freezes this verifier and its matching completed stage.
// Only the verifier's authority port renews grants. Successful expiry extension
// preserves private record/node progress: no object, generation or location is
// changed, and conforming collection must still fence the original source head.
// No verification progress is serialized. Use both handles from one goroutine.
func (c *CompactionCommit) BeginIntentRenewal(ctx context.Context, stage *CompactionStage, now func() time.Time, expires time.Time) (*CompactionStageRenewal, error) {
	if c.err != nil {
		return nil, c.err
	}
	if c.done || c.renewal != nil || stage == nil || stage.err != nil || stage.renewal != nil || !reflect.DeepEqual(c.prepared, stage.result()) {
		return nil, ErrConflict
	}
	operation, err := c.protocol.BeginCompactionIntentRenewal(ctx, c.prepared, now, expires)
	if err != nil {
		return nil, err
	}
	r := &CompactionStageRenewal{stage: stage, commit: c, operation: operation}
	stage.renewal = r
	c.renewal = r
	return r, nil
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
			if r.commit != nil {
				r.commit.err = err
			}
		}
		return false, err
	}
	if !done {
		return false, nil
	}
	r.stage.prepared.expires = r.operation.expires
	r.stage.renewal = nil
	if r.commit != nil {
		r.commit.prepared.expires = r.operation.expires
		r.commit.renewal = nil
	}
	r.done = true
	return true, nil
}
