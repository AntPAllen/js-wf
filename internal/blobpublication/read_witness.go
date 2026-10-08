package blobpublication

import (
	"context"
	"errors"
)

var ErrInvalidWitness = errors.New("invalid authority witness acknowledgment")

// ReadWithWitness is the shared native/model authority read decision. snapshot
// bytes are tentative until witness conditionally reaffirms the exact value at
// its physical sequence and returns a strictly later acknowledged sequence.
// A conflicting witness reloads the snapshot, at most16 times in the caller's
// original context. Every other failure returns zero data, including a lost
// acknowledgment after commitment; another GET cannot confirm that outcome.
// The callbacks own transport/envelope validation and must preserve logical
// heads and generations. The witness must be a quorum-acknowledged conditional
// write at the snapshot's actual destination, not a side-index marker.
func ReadWithWitness[T any](ctx context.Context, snapshot func(context.Context) (T, uint64, error), witness func(context.Context, uint64, T) (uint64, error)) (T, uint64, error) {
	var zero T
	if snapshot == nil || witness == nil {
		return zero, 0, errors.New("authority snapshot and witness required")
	}
	for attempt := 0; attempt < 16; attempt++ {
		if err := ctx.Err(); err != nil {
			return zero, 0, err
		}
		value, sequence, err := snapshot(ctx)
		if err != nil {
			return zero, 0, err
		}
		if err := ctx.Err(); err != nil {
			return zero, 0, err
		}
		confirmed, err := witness(ctx, sequence, value)
		if errors.Is(err, ErrConflict) {
			continue
		}
		if err != nil {
			return zero, 0, err
		}
		if confirmed <= sequence {
			return zero, 0, ErrInvalidWitness
		}
		return value, confirmed, nil
	}
	return zero, 0, ErrConflict
}
