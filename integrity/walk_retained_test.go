package integrity

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestRetainedWalkAdmissionAndCancellation(t *testing.T) {
	visit := func(*jetstream.RawStreamMsg) error { t.Fatal("invalid walk invoked visitor"); return nil }
	if err := WalkRetainedWithChunkedReads(context.Background(), nil, 0, visit); err == nil {
		t.Fatal("unbounded walk admitted")
	}
	ctx, stop := context.WithTimeout(t.Context(), time.Second)
	defer stop()
	if err := WalkRetainedWithChunkedReads(ctx, nil, 0, visit); err == nil {
		t.Fatal("nil stream admitted")
	}
	stream := singleReplicaChunkedAuditStream{}
	if err := WalkRetainedWithChunkedReads(ctx, stream, 0, nil); err == nil {
		t.Fatal("nil visitor admitted")
	}
	stop()
	if err := WalkRetainedWithChunkedReads(ctx, stream, 0, visit); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled walk entered transport: %v", err)
	}
}
