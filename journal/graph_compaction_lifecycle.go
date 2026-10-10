package journal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"js-wf/internal/graphpublication"
)

// CheckpointCompaction owns confirmation, unpublished staging and private final
// verification for one exact checkpoint. Advance performs one phase batch.
// Verification progress is process-local; portable staging input is bound on
// resumption and never trusted as a serialized verification result.
// Close on abandonment. Explicit renewal freezes staging until completion.
type CheckpointCompaction struct {
	store            *GraphStore
	view             *GraphView
	scan             *CheckpointScan
	stage            *graphpublication.CompactionStage
	commit           *graphpublication.CompactionCommit
	renewal          *graphpublication.CompactionStageRenewal
	renewInput       []byte
	renewTo          *time.Time
	renewReturn      string
	typ, id          string
	runtime          RuntimeCheckpoint
	tail             uint64
	phase            string
	released         bool
	releaseAttempted bool
	releaseErr       error
	err              error
}

func (s *GraphStore) BeginCheckpointCompaction(ctx context.Context, typ, id string, runtime RuntimeCheckpoint, tail uint64) (*CheckpointCompaction, error) {
	if !s.cfg.ArchiveCheckpoints {
		return nil, fmt.Errorf("checkpoint compaction requires the explicit v6 archive cursor")
	}
	view, err := s.OpenExisting(ctx, typ, id, runtime.InvSeq)
	if err != nil {
		return nil, err
	}
	if view == nil {
		return nil, ErrStale
	}
	scan, err := view.NewCheckpointScan(ctx, typ, id)
	if err != nil {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer stop()
		_ = view.Close(cleanup)
		return nil, err
	}
	return &CheckpointCompaction{store: s, view: view, scan: scan, typ: typ, id: id, runtime: runtime, tail: tail, phase: "confirm"}, nil
}

func (c *CheckpointCompaction) Phase() string { return c.phase }

func (c *CheckpointCompaction) Close(ctx context.Context) error {
	if c.releaseAttempted {
		return c.releaseErr
	}
	// A failed release may have committed. Cleanup must preserve that uncertain
	// outcome rather than issuing a second mutation behind the caller's back.
	c.releaseAttempted = true
	if err := c.view.Close(ctx); err != nil {
		c.releaseErr = err
		return err
	}
	c.released = true
	return nil
}

func (c *CheckpointCompaction) Advance(ctx context.Context, maxRecords, maxNodes uint64) (done bool, err error) {
	if c.err != nil {
		return false, c.err
	}
	if maxRecords == 0 || maxNodes == 0 {
		return false, fmt.Errorf("positive checkpoint compaction budgets required")
	}
	if err = ctx.Err(); err != nil {
		return false, err
	}
	if c.phase == "done" {
		return true, nil
	}
	if c.released && c.phase == "confirm" {
		return false, ErrStale
	}
	defer func() {
		if err != nil {
			c.err = err
		}
	}()
	switch c.phase {
	case "confirm":
		verified, complete, err := c.scan.Advance(ctx, maxRecords)
		if err != nil {
			return false, err
		}
		if !complete {
			return false, nil
		}
		if verified == nil || verified.Runtime != c.runtime {
			return false, ErrGap
		}
		if verified.Tail != c.tail {
			return false, ErrStale
		}
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		err = c.Close(cleanup)
		stop()
		if err != nil {
			return false, fmt.Errorf("%w: checkpoint reader release: %w", ErrUnknown, err)
		}
		destination, root, cursor, err := c.store.observe(ctx, c.typ, c.id)
		if err != nil {
			return false, err
		}
		if cursor == nil || cursor.Invocation != c.runtime.InvSeq || cursor.Retired || cursor.Purging || cursor.Kind == Completed || cursor.Kind == Failed || cursor.Base+cursor.Count != c.tail || cursor.Checkpoint == nil || cursor.Checkpoint.Runtime != c.runtime || cursor.Checkpoint.RequestIndex != verified.Request.Index {
			return false, ErrStale
		}
		if cursor.RetainedFrom == verified.Request.Index {
			c.phase = "done"
			return true, nil
		}
		if cursor.RetainedFrom > verified.Request.Index {
			return false, ErrStale
		}
		next := *cursor
		next.RetainedFrom = verified.Request.Index
		data, err := json.Marshal(next)
		if err != nil {
			return false, err
		}
		limit := max(c.store.cfg.PayloadReadLimit, MaxGraphEntryBytes)
		c.stage, err = c.store.cfg.Protocol.BeginPrefixCompaction(ctx, destination, root.Head, next.RetainedFrom-cursor.RetainedFrom, limit, c.store.cfg.Now().Add(c.store.cfg.IntentTTL), data)
		if err != nil {
			return false, graphMutationError(err)
		}
		c.phase = "stage"
		return false, nil
	case "stage":
		prepared, complete, err := c.stage.Advance(ctx, maxRecords)
		if err != nil {
			return false, graphMutationError(err)
		}
		if !complete {
			return false, nil
		}
		c.commit, err = c.store.cfg.Protocol.BeginCompactionCommit(ctx, prepared)
		if err != nil {
			return false, graphMutationError(err)
		}
		c.phase = "verify"
		return false, nil
	case "verify":
		_, complete, err := c.commit.Advance(ctx, maxRecords, maxNodes)
		if err != nil {
			return false, graphMutationError(err)
		}
		if complete {
			c.phase = "done"
		}
		return complete, nil
	case "renew":
		complete, err := c.renewal.Advance(ctx, maxNodes)
		if err != nil {
			return false, graphMutationError(err)
		}
		if complete {
			c.renewal = nil
			c.renewInput = nil
			c.renewTo = nil
			c.phase = c.renewReturn
			c.renewReturn = ""
		}
		return false, nil
	default:
		return false, errors.New("invalid checkpoint compaction phase")
	}
}
