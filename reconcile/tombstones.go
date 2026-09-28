package reconcile

import (
	"context"
	"fmt"
	"time"

	"js-wf/retention"

	"github.com/nats-io/nats.go/jetstream"
)

// RunTombstoneLoop elects one scanner and persists its WF_STATE stream cursor.
// It needs a budget of at least two: each cursor save adds one KV stream
// sequence, so a one-sequence page could chase its own writes forever.
func RunTombstoneLoop(ctx context.Context, js jetstream.JetStream, workerID string, interval time.Duration, budget int) error {
	if budget < 2 {
		return fmt.Errorf("tombstone scan budget must be at least two")
	}
	scanner := retention.NewTombstoneScan(js)
	return runLoop(ctx, js, workerID, "tombstone", interval, budget, func(ctx context.Context, next uint64, budget int, dryRun bool) (ScanResult, error) {
		page, err := scanner.Scan(ctx, next, budget, time.Now().UTC(), dryRun)
		return ScanResult{NextSequence: page.NextSequence, Inspected: page.Inspected, Removed: page.Deleted}, err
	})
}
