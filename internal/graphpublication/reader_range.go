package graphpublication

import (
	"context"
	"errors"
	"time"

	"js-wf/internal/retainedgraph"
)

// ReadRetainedRange traverses one pinned forest without repeated branch GETs.
// It witnesses the exact live pin before traversal and again before delivering
// each leaf. now must share the collector's clock. No node cache survives this
// call; bytes alone never authorize a callback after release or expiry.
func (p Protocol) ReadRetainedRange(ctx context.Context, reader Reader, stream string, first, end uint64, now func() time.Time, visit func(uint64, retainedgraph.Record) error) error {
	if p.Port == nil || now == nil || visit == nil || stream != "" && !validStream(stream) {
		return errors.New("invalid retained range")
	}
	check := func() error {
		root, err := p.readRoot(ctx, reader.destination)
		if err != nil {
			return err
		}
		i, err := readerIndex(root, reader)
		if err != nil {
			return err
		}
		current := now()
		if current.IsZero() || !current.Before(root.Readers[i].Expires) {
			return ErrRevoked
		}
		return nil
	}
	if err := check(); err != nil {
		return err
	}
	return retainedgraph.ReadRange(ctx, stageStore{protocol: p}, selectGraph(reader.graph, reader.streams, stream), first, end,
		func(index uint64, record retainedgraph.Record) error {
			if err := check(); err != nil {
				return err
			}
			return visit(index, record)
		})
}
