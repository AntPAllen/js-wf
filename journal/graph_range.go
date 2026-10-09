package journal

import (
	"context"

	"js-wf/internal/graphpublication"
	"js-wf/internal/retainedgraph"
)

// ReadRange visits [first,end) in order in this exact pinned generation. Each
// entry receives the same validation as Read; callbacks preceding an error are
// partial results. It keeps no object cache and holds only a tree traversal
// spine. Keep the view alive while using any returned payload receipts.
func (v *GraphView) ReadRange(ctx context.Context, first, end uint64, visit func(GraphRecord) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := v.alive(); err != nil {
		return err
	}
	if first > end || end > v.cursor.Count || visit == nil {
		return ErrGap
	}
	run := func(stream string, start, stop, offset uint64) error {
		if start == stop {
			return ctx.Err()
		}
		return v.store.cfg.Protocol.ReadRetainedRange(ctx, v.reader, stream, start, stop, v.store.cfg.Now,
			func(index uint64, raw retainedgraph.Record) error {
				if err := v.alive(); err != nil {
					return err
				}
				record, err := v.decodeRecord(ctx, index+offset, raw)
				if err != nil {
					return err
				}
				return visit(record)
			})
	}
	if first < v.cursor.RetainedFrom {
		if err := run(graphpublication.PrefixArchiveStream, first, min(end, v.cursor.RetainedFrom), 0); err != nil {
			return err
		}
	}
	if end > v.cursor.RetainedFrom {
		if err := run("", max(first, v.cursor.RetainedFrom)-v.cursor.RetainedFrom, end-v.cursor.RetainedFrom, v.cursor.RetainedFrom); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return v.alive()
}
