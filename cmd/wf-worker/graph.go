package main

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/internal/graphcli"
	"js-wf/journal"
	"js-wf/reconcile"
)

type workerGraphSelection struct {
	config journal.NativeGraphConfig
	store  *journal.GraphStore
}

func selectWorkerGraph(authority, prefix, bucket string, replicas int, encoding journal.Encoding, timerBackend, retentionType string, version int) (*workerGraphSelection, error) {
	cfg, err := graphcli.Select(authority, prefix, bucket, replicas, encoding, version)
	if err != nil || cfg == nil {
		return nil, err
	}
	if timerBackend != "native" && timerBackend != "fallback" {
		return nil, fmt.Errorf("graph runtime requires explicit -timer-backend native or fallback")
	}
	if retentionType != "" {
		return nil, fmt.Errorf("graph runtime requires graph-aware retention; retention-type selects the legacy handler")
	}
	return &workerGraphSelection{config: *cfg}, nil
}

func graphWorkerRepairLoops(js jetstream.JetStream, id string, interval time.Duration, budget int, graph *journal.GraphStore, observe func(reconcile.RepairEvent), clock reconcile.TimerDomainClock) []func(context.Context) error {
	kinds := []string{"graph-start", "graph-signal", "graph-terminal", "timer", "suspended"}
	if graph.CheckpointIndex() {
		kinds = append(kinds, "graph-continuation")
	}
	loops := make([]func(context.Context) error, 0, len(kinds))
	for _, kind := range kinds {
		loops = append(loops, func(ctx context.Context) error {
			if err := reconcile.RunRepairLoopWithGraphJournal(ctx, js, id, kind, interval, budget, graph, observe, clock, nil); err != nil {
				return fmt.Errorf("%s repair loop: %w", kind, err)
			}
			return nil
		})
	}
	return loops
}
