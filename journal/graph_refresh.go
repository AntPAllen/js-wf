package journal

import (
	"context"
	"errors"

	"js-wf/internal/graphpublication"
)

// Refresh replaces this view's exact pin with the current matching generation
// in one root CAS. It updates the cursor only on confirmed success. An expired
// or revoked view needs a fresh Open; unknown outcomes must not be retried.
// Like Renew and Close, this mutates the view and requires exclusive access.
func (v *GraphView) Refresh(ctx context.Context, typ, id string) error {
	destination, err := graphDestination(typ, id)
	if err != nil {
		return err
	}
	if destination != v.destination {
		return ErrStale
	}
	for attempt := 0; attempt < 16; attempt++ {
		if err = v.alive(); err != nil {
			return err
		}
		_, root, cursor, err := v.store.observe(ctx, typ, id)
		if err != nil {
			return err
		}
		if cursor == nil || cursor.Invocation != v.cursor.Invocation || cursor.Retired || cursor.Purging {
			return ErrStale
		}
		if err = v.alive(); err != nil {
			return err
		}
		now := v.store.cfg.Now()
		expires := now.Add(v.store.cfg.PinTTL)
		if expires.Before(v.expires) {
			expires = v.expires
		}
		reader, _, err := v.store.cfg.Protocol.ReplaceReader(ctx, v.reader, root.Head, v.store.cfg.Now, expires)
		if err == nil {
			v.reader, v.cursor, v.expires = reader, *cursor, expires
			return nil
		}
		if !errors.Is(err, graphpublication.ErrConflict) || ctx.Err() != nil {
			return err
		}
	}
	return graphpublication.ErrConflict
}
