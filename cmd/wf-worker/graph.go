package main

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/internal/graphcli"
	"js-wf/internal/graphpublication"
	"js-wf/journal"
	"js-wf/reconcile"
)

type workerGraphSelection struct {
	config journal.NativeGraphConfig
	store  *journal.GraphStore
}

type terminalAuditConfig struct {
	interval time.Duration
	budget   int
}

type readerExpiryConfig struct {
	interval time.Duration
	budget   int
}

func selectReaderExpiry(enabled, repair bool, graph *workerGraphSelection, interval time.Duration, budget int) (*readerExpiryConfig, error) {
	if !enabled {
		if interval != 0 || budget != 0 {
			return nil, fmt.Errorf("graph-reader-expiry settings require enabled graph-reader-expiry")
		}
		return nil, nil
	}
	if graph == nil || !repair {
		return nil, fmt.Errorf("graph-reader-expiry requires a graph store and enabled repair loops")
	}
	if interval <= 0 || interval > 10*time.Second || budget < 1 || budget > graphpublication.MaxReaderSweepBatch {
		return nil, fmt.Errorf("graph-reader-expiry requires explicit interval in (0,10s] and budget in [1,%d]", graphpublication.MaxReaderSweepBatch)
	}
	return &readerExpiryConfig{interval, budget}, nil
}

func selectTerminalAudit(enabled, repair bool, graph *workerGraphSelection, interval time.Duration, budget int) (*terminalAuditConfig, error) {
	if !enabled {
		if interval != 0 || budget != 0 {
			return nil, fmt.Errorf("graph-terminal-audit settings require enabled graph-terminal-audit")
		}
		return nil, nil
	}
	if graph == nil || !repair {
		return nil, fmt.Errorf("graph-terminal-audit requires a graph store and enabled repair loops")
	}
	if interval <= 0 || interval > 10*time.Second || budget < 1 {
		return nil, fmt.Errorf("graph-terminal-audit requires explicit interval in (0,10s] and positive budget")
	}
	return &terminalAuditConfig{interval, budget}, nil
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
	return graphWorkerRepairLoopsWithAudit(js, id, interval, budget, graph, observe, clock, nil)
}

func graphWorkerRepairLoopsWithAudit(js jetstream.JetStream, id string, interval time.Duration, budget int, graph *journal.GraphStore, observe func(reconcile.RepairEvent), clock reconcile.TimerDomainClock, audit *terminalAuditConfig) []func(context.Context) error {
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
	if audit != nil {
		loops = append(loops, func(ctx context.Context) error {
			return reconcile.RunRepairLoopWithGraphJournal(ctx, js, id, "graph-terminal-audit", audit.interval, audit.budget, graph, observe, clock, nil)
		})
	}
	return loops
}
