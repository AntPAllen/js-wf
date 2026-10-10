package journal

import (
	"context"
	"fmt"
	"time"
)

// CompactCheckpoint archives records before the verified checkpoint request.
// The request, completion and continuation suffix remain live, with absolute
// logical indices and sequence numbers unchanged. Old views retain their exact
// original forests until release/expiry. Archive bytes remain owned for audit.
// This requires a new isolated v6 store and an already published exact pointer.
// It does not activate collection or public continuation registration.
func (s *GraphStore) CompactCheckpoint(ctx context.Context, typ, id string, runtime RuntimeCheckpoint, tail uint64) (err error) {
	operation, err := s.BeginCheckpointCompaction(ctx, typ, id, runtime, tail)
	if err != nil {
		return err
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer stop()
		if closeErr := operation.Close(cleanup); err == nil && closeErr != nil {
			err = fmt.Errorf("%w: checkpoint reader release: %w", ErrUnknown, closeErr)
		}
	}()
	for {
		done, err := operation.Advance(ctx, max(uint64(1), operation.view.Count()), ^uint64(0))
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}
}
