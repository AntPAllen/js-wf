package main

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/journal"
	"js-wf/reconcile"
)

type workerGraphSelection struct {
	config journal.NativeGraphConfig
	store  *journal.GraphStore
}

func selectWorkerGraph(authority, prefix, bucket string, replicas int, encoding journal.Encoding, timerBackend, retentionType string) (*workerGraphSelection, error) {
	if authority == "" && prefix == "" && bucket == "" {
		return nil, nil
	}
	if authority == "" || prefix == "" || bucket == "" {
		return nil, fmt.Errorf("graph runtime requires graph-authority-stream, graph-authority-prefix and graph-object-bucket together")
	}
	if timerBackend == "fallback" {
		return nil, fmt.Errorf("graph runtime fallback timer migration is incomplete; select native timers")
	}
	if timerBackend != "native" {
		return nil, fmt.Errorf("graph runtime requires explicit -timer-backend native on a fully upgraded cluster")
	}
	if retentionType != "" {
		return nil, fmt.Errorf("graph runtime requires graph-aware retention; retention-type selects the legacy handler")
	}
	cfg := journal.NativeGraphConfig{AuthorityStream: authority, AuthorityPrefix: prefix, ObjectBucket: bucket, ExpectedReplicas: replicas, Encoding: encoding, CanonicalStarts: true, CanonicalSignals: true}
	// Reuse namespace/config validation without provisioning either graph store.
	if _, err := journal.NativeGraphStreamConfigs(cfg, replicas); err != nil {
		return nil, err
	}
	return &workerGraphSelection{config: cfg}, nil
}

func graphWorkerRepairLoops(js jetstream.JetStream, id string, interval time.Duration, budget int, graph *journal.GraphStore, observe func(reconcile.RepairEvent), clock reconcile.TimerDomainClock) []func(context.Context) error {
	kinds := []string{"graph-start", "graph-signal", "graph-terminal", "timer", "suspended"}
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
