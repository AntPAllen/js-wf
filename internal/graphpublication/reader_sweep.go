package graphpublication

import (
	"context"
	"errors"
	"math"
	"time"
	"unicode/utf8"
)

const MaxReaderSweepBatch = 256

// ReaderSweepCursor can be persisted between batches. Through is the captured
// authority watermark, not a root head or a global collection barrier.
type ReaderSweepCursor struct {
	Next    uint64
	Through uint64
}

type ReaderSweepResult struct {
	Cursor    ReaderSweepCursor
	Inspected int
	Complete  bool
}

// BeginReaderSweep captures a bounded catalog pass. Roots moved or created
// beyond its watermark are revisited by the next periodic pass.
func (p Protocol) BeginReaderSweep(ctx context.Context) (ReaderSweepCursor, error) {
	if err := ctx.Err(); err != nil {
		return ReaderSweepCursor{}, err
	}
	if _, ok := p.Port.(RootScanPort); !ok {
		return ReaderSweepCursor{}, errors.New("graph port has no bounded root scan")
	}
	watermark, ok := p.Port.(RootCatalogWatermarkPort)
	if !ok {
		return ReaderSweepCursor{}, errors.New("graph port has no root catalog watermark")
	}
	through, err := watermark.RootCatalogHighWater(ctx)
	if err != nil {
		return ReaderSweepCursor{}, err
	}
	if through == math.MaxUint64 {
		return ReaderSweepCursor{}, errors.New("root catalog watermark exhausted")
	}
	return ReaderSweepCursor{Next: 1, Through: through}, nil
}

// ExpireReaderBatch fences expired reader pins at at most budget destinations.
// On uncertainty it returns the last confirmed cursor, so callers can retry
// the uncertain destination. It never deletes objects. Complete witnesses only
// this catalog pass; concurrent publication prevents using it as permission
// for global object collection. SweepWithReaders remains the combined sweep.
func (p Protocol) ExpireReaderBatch(ctx context.Context, cursor ReaderSweepCursor, budget int, now time.Time) (ReaderSweepResult, error) {
	result := ReaderSweepResult{Cursor: cursor}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if cursor.Through == math.MaxUint64 || cursor.Next == 0 || cursor.Next > cursor.Through+1 || budget < 1 || budget > MaxReaderSweepBatch || now.IsZero() {
		return result, errors.New("invalid reader sweep batch")
	}
	scan, ok := p.Port.(RootScanPort)
	if !ok {
		return result, errors.New("graph port has no bounded root scan")
	}
	for result.Inspected < budget && result.Cursor.Next <= cursor.Through {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		entry, err := scan.NextRoot(ctx, result.Cursor.Next)
		if err != nil {
			return result, err
		}
		if entry == nil {
			result.Cursor.Next = cursor.Through + 1
			break
		}
		if entry.Sequence < result.Cursor.Next || entry.Sequence == math.MaxUint64 || entry.Destination == "" || !utf8.ValidString(entry.Destination) || len(entry.Destination) > 256 {
			return result, errors.New("invalid graph root scan entry")
		}
		if entry.Sequence > cursor.Through {
			result.Cursor.Next = cursor.Through + 1
			break
		}
		if err := p.expireCatalogRoot(ctx, entry.Destination, now); err != nil {
			return result, err
		}
		result.Cursor.Next = entry.Sequence + 1
		result.Inspected++
	}
	result.Complete = result.Cursor.Next > cursor.Through
	return result, nil
}
