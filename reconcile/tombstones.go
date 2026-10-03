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
	if interval <= 0 || interval > 10*time.Second {
		return fmt.Errorf("invalid reconcile cadence or budget")
	}
	port := &jetStreamLoopPort{js: js, ticker: time.NewTicker(interval)}
	defer port.ticker.Stop()
	return RunTombstoneLoopWithPorts(ctx, port, retention.NewTombstoneScanPort(js), workerID, interval, budget, time.Now)
}

// RunTombstoneLoopWithPorts runs the production tombstone scan and cursor loop
// against real or deterministic retained-state and lease transports.
func RunTombstoneLoopWithPorts(ctx context.Context, loop LoopPort, scans retention.TombstoneScanPort, workerID string, interval time.Duration, budget int, now func() time.Time) error {
	if budget < 2 {
		return fmt.Errorf("tombstone scan budget must be at least two")
	}
	if scans == nil || now == nil {
		return fmt.Errorf("missing tombstone scan transport or clock")
	}
	scanner := retention.NewTombstoneScanWithPort(scans)
	return RunLoopWithPort(ctx, loop, workerID, "tombstone", interval, budget, func(ctx context.Context, next uint64, budget int, dryRun bool) (ScanResult, error) {
		page, err := scanner.Scan(ctx, next, budget, now().UTC(), dryRun)
		return ScanResult{RetrySequence: page.RetrySequence, NextSequence: page.NextSequence, Inspected: page.Inspected, Removed: page.Deleted + page.MarkersDeleted}, err
	})
}
