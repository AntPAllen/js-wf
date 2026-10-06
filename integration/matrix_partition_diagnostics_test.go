//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"js-wf/testcluster"
)

// Sampling runs alongside the original fault. Its context is the existing
// 35-second whole-fault bound; it never changes the recovery decision.
func startMatrixPartitionDiagnostics(ctx context.Context, cluster *testcluster.ProcessCluster, scheduled time.Time) (func() error, error) {
	if os.Getenv("WF_MATRIX_PARTITION_DIAGNOSTICS") != "1" {
		return func() error { return nil }, nil
	}
	prefix := os.Getenv("MATRIX_ARTIFACT_PREFIX")
	if prefix == "" {
		return nil, fmt.Errorf("partition diagnostics require MATRIX_ARTIFACT_PREFIX")
	}
	file, err := os.OpenFile(fmt.Sprintf("%s-partition-%d-peers.jsonl", prefix, scheduled.UnixNano()), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	observe, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		encoder := json.NewEncoder(file)
		var writeErr error
		defer func() {
			if err := file.Close(); writeErr == nil {
				writeErr = err
			}
			done <- writeErr
		}()
		type peer struct {
			Node       int             `json:"node"`
			ObservedAt time.Time       `json:"observed_at"`
			Routes     int             `json:"routes"`
			RouteError string          `json:"route_error,omitempty"`
			JetStream  json.RawMessage `json:"jetstream,omitempty"`
			Error      string          `json:"error,omitempty"`
		}
		round := 0
		for observe.Err() == nil {
			round++
			var peers [3]peer
			var joined sync.WaitGroup
			for node := range peers {
				joined.Add(1)
				go func(node int) {
					defer joined.Done()
					attempt, stop := context.WithTimeout(observe, time.Second)
					defer stop()
					p := peer{Node: node, ObservedAt: time.Now().UTC()}
					data, err := cluster.Diagnostic(attempt, node, "jetstream")
					if err == nil {
						p.JetStream = data
					} else {
						p.Error = err.Error()
					}
					p.Routes, err = cluster.RouteCount(attempt, node)
					if err != nil {
						p.RouteError = err.Error()
					}
					peers[node] = p
				}(node)
			}
			joined.Wait()
			if err := encoder.Encode(struct {
				Scheduled time.Time `json:"scheduled"`
				Round     int       `json:"round"`
				Peers     [3]peer   `json:"peers"`
			}{scheduled, round, peers}); err != nil {
				writeErr = err
				return
			}
			select {
			case <-observe.Done():
			case <-time.After(2 * time.Second):
			}
		}
	}()
	return func() error { cancel(); return <-done }, nil
}
