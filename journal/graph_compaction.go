package journal

import (
	"context"
	"encoding/json"
	"fmt"
)

// CompactCheckpoint archives records before the verified checkpoint request.
// The request, completion and continuation suffix remain live, with absolute
// logical indices and sequence numbers unchanged. Old views retain their exact
// original forests until release/expiry. Archive bytes remain owned for audit.
// This requires a new isolated v6 store and an already published exact pointer.
// It does not activate collection or public continuation registration.
func (s *GraphStore) CompactCheckpoint(ctx context.Context, typ, id string, runtime RuntimeCheckpoint, tail uint64) error {
	if !s.cfg.ArchiveCheckpoints {
		return fmt.Errorf("checkpoint compaction requires the explicit v6 archive cursor")
	}
	verified, err := s.confirmCheckpoint(ctx, typ, id, runtime, tail)
	if err != nil {
		return err
	}
	destination, root, cursor, err := s.observe(ctx, typ, id)
	if err != nil {
		return err
	}
	if cursor == nil || cursor.Invocation != runtime.InvSeq || cursor.Retired || cursor.Purging || cursor.Kind == Completed || cursor.Kind == Failed || cursor.Base+cursor.Count != tail || cursor.Checkpoint == nil || cursor.Checkpoint.Runtime != runtime || cursor.Checkpoint.RequestIndex != verified.Request.Index {
		return ErrStale
	}
	if cursor.RetainedFrom == verified.Request.Index {
		return nil
	}
	if cursor.RetainedFrom > verified.Request.Index {
		return ErrStale
	}
	next := *cursor
	next.RetainedFrom = verified.Request.Index
	data, err := json.Marshal(next)
	if err != nil {
		return err
	}
	limit := s.cfg.PayloadReadLimit
	if limit < MaxGraphEntryBytes {
		limit = MaxGraphEntryBytes
	}
	prepared, err := s.cfg.Protocol.PreparePrefixCompaction(ctx, destination, root.Head, next.RetainedFrom-cursor.RetainedFrom, limit, s.cfg.Now().Add(s.cfg.IntentTTL), data)
	if err != nil {
		return graphMutationError(err)
	}
	_, err = s.cfg.Protocol.CommitPrefixCompaction(ctx, prepared)
	return graphMutationError(err)
}
