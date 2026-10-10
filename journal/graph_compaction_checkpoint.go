package journal

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"js-wf/internal/graphpublication"
)

const CompactionCheckpointSchema = "js-wf-journal-compaction-stage-v1"
const MaxCompactionCheckpointBytes = graphpublication.MaxCompactionCheckpointBytes + 8192

type compactionCheckpoint struct {
	Schema  string            `json:"schema"`
	Type    string            `json:"type"`
	ID      string            `json:"id"`
	Runtime RuntimeCheckpoint `json:"runtime"`
	Tail    uint64            `json:"tail"`
	Stage   json.RawMessage   `json:"stage"`
	RenewTo *time.Time        `json:"renew_to,omitempty"`
}

// Checkpoint returns portable staging input, never a verified-prefix proof.
// During renewal it retains the pre-renewal input and exact requested expiry,
// including after uncertain updates. Persist it before advancing renewal.
// Confirmation and completed publication have no resumable staging descriptor.
func (c *CheckpointCompaction) Checkpoint() ([]byte, error) {
	var stage []byte
	var err error
	if c.phase == "renew" {
		stage = bytes.Clone(c.renewInput)
	} else if c.phase == "stage" || c.phase == "verify" {
		stage, err = c.stage.Checkpoint()
	} else {
		return nil, fmt.Errorf("%w: compaction staging unavailable", ErrGap)
	}
	if err != nil {
		return nil, graphMutationError(err)
	}
	if len(stage) == 0 {
		return nil, ErrGap
	}
	v := compactionCheckpoint{CompactionCheckpointSchema, c.typ, c.id, c.runtime, c.tail, stage, c.renewTo}
	data, err := json.Marshal(v)
	if err != nil || len(data) > MaxCompactionCheckpointBytes {
		return nil, fmt.Errorf("%w: compaction checkpoint byte limit", ErrGap)
	}
	return data, nil
}

// BeginIntentRenewalIfNeeded schedules renewal with one third of IntentTTL left.
// It uses the store's authority clock and leaves confirmation and active renewal
// alone. An expired intent is rejected by BeginIntentRenewal, never revived.
// The caller must still advance renewal in bounded batches under its lease.
func (c *CheckpointCompaction) BeginIntentRenewalIfNeeded(ctx context.Context) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if c.err != nil {
		return false, c.err
	}
	if c.phase != "stage" && c.phase != "verify" {
		return false, nil
	}
	now := c.store.cfg.Now()
	if c.stage.IntentExpiry().Sub(now) > c.store.cfg.IntentTTL/3 {
		return false, nil
	}
	if err := c.BeginIntentRenewal(ctx, now.Add(c.store.cfg.IntentTTL)); err != nil {
		return false, err
	}
	return true, nil
}

// BeginIntentRenewal freezes staging or verification before grant updates.
// Save Checkpoint's requested expiry before calling Advance. Renewal errors
// require fresh Resume from that input; the old operation remains failed.
func (c *CheckpointCompaction) BeginIntentRenewal(ctx context.Context, expires time.Time) error {
	if c.err != nil {
		return c.err
	}
	if c.phase != "stage" && c.phase != "verify" {
		return fmt.Errorf("%w: compaction cannot renew in this phase", ErrStale)
	}
	input, err := c.stage.Checkpoint()
	if err != nil {
		return graphMutationError(err)
	}
	var renewal *graphpublication.CompactionStageRenewal
	if c.phase == "verify" {
		renewal, err = c.commit.BeginIntentRenewal(ctx, c.stage, c.store.cfg.Now, expires)
	} else {
		renewal, err = c.stage.BeginIntentRenewal(ctx, c.store.cfg.Now, expires)
	}
	if err != nil {
		return graphMutationError(err)
	}
	c.renewal = renewal
	c.renewInput = input
	expires = expires.UTC()
	c.renewTo = &expires
	c.renewReturn = c.phase
	c.phase = "renew"
	return nil
}

// ResumeCheckpointCompaction binds saved staging input to the caller's expected
// identity and the freshly observed canonical checkpoint/archive transition.
// It does not acquire a reader (which would change the captured source head).
// The canonical source remains protected by its root; publication still uses
// the original-head CAS and independently verifies the entire relocation.
// Private verification progress is discarded on resume.
func (s *GraphStore) ResumeCheckpointCompaction(ctx context.Context, typ, id string, runtime RuntimeCheckpoint, tail uint64, data []byte) (*CheckpointCompaction, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !s.cfg.ArchiveCheckpoints || len(data) == 0 || len(data) > MaxCompactionCheckpointBytes {
		return nil, ErrGap
	}
	var v compactionCheckpoint
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&v); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrGap, err)
	}
	canonical, err := json.Marshal(v)
	if err != nil || d.Decode(new(any)) != io.EOF || !bytes.Equal(data, canonical) || v.Schema != CompactionCheckpointSchema || v.Type != typ || v.ID != id || v.Runtime != runtime || v.Tail != tail || len(v.Stage) == 0 {
		return nil, ErrGap
	}
	destination, root, cursor, err := s.observe(ctx, typ, id)
	if err != nil {
		return nil, err
	}
	if cursor == nil || cursor.Invocation != runtime.InvSeq || cursor.Retired || cursor.Purging || cursor.Kind == Completed || cursor.Kind == Failed || cursor.Base+cursor.Count != tail || cursor.Checkpoint == nil || cursor.Checkpoint.Runtime != runtime || cursor.RetainedFrom >= cursor.Checkpoint.RequestIndex {
		return nil, ErrStale
	}
	next := *cursor
	next.RetainedFrom = cursor.Checkpoint.RequestIndex
	application, err := json.Marshal(next)
	if err != nil {
		return nil, err
	}
	stage, err := s.cfg.Protocol.ResumePrefixCompaction(ctx, v.Stage)
	if err != nil {
		return nil, graphMutationError(err)
	}
	if !stage.MatchesBinding(destination, root.Head, next.RetainedFrom-cursor.RetainedFrom, max(s.cfg.PayloadReadLimit, MaxGraphEntryBytes), application) {
		return nil, ErrGap
	}
	c := &CheckpointCompaction{store: s, stage: stage, typ: typ, id: id, runtime: runtime, tail: tail, phase: "stage", released: true, releaseAttempted: true}
	if v.RenewTo != nil {
		if err := c.BeginIntentRenewal(ctx, *v.RenewTo); err != nil {
			return nil, err
		}
	}
	return c, nil
}
