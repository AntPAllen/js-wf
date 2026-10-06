package integrity

import (
	"context"
	"errors"

	"github.com/nats-io/nats.go/jetstream"
)

// WalkRetainedWithChunkedReads visits retained records in stream sequence order
// through the explicit R1 memory-cursor scanner. The source's replication is
// unchanged. cutoff is a captured inclusive high-water mark; gaps, ordering,
// source identity, recovery and cleanup use the existing audit scanner.
//
// The caller must supply a deadline, a quiescent source and a non-nil visitor.
// The visitor owns any retained data and must discard its partial state on error.
// This transport walk alone does not establish the workflow invariants.
func WalkRetainedWithChunkedReads(ctx context.Context, stream jetstream.Stream, cutoff uint64, visit func(*jetstream.RawStreamMsg) error) error {
	if _, ok := ctx.Deadline(); !ok || stream == nil || visit == nil {
		return errors.New("retained walk requires deadline, stream and visitor")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return scanSingleReplicaChunkedThrough(ctx, stream, &cutoff, visit)
}
