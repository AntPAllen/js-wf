//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// Snapshots have controller observation brackets. They are diagnostic evidence,
// not proof of recovery, quorum, physical drain, or the cause of a failed call.
func saveFiveContainerFaultDiagnostics(ctx context.Context, read func(context.Context, int, string) ([]byte, error), prefix string) []error {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var writeErrors []error
	for node := 0; node < 5; node++ {
		wg.Add(1)
		go func(node int) {
			defer wg.Done()
			for _, kind := range []string{"jetstream", "routes", "connections"} {
				snapshot := struct {
					Node   int             `json:"node"`
					Kind   string          `json:"kind"`
					Before time.Time       `json:"before"`
					After  time.Time       `json:"after"`
					Data   json.RawMessage `json:"data,omitempty"`
					Error  string          `json:"error,omitempty"`
				}{Node: node, Kind: kind, Before: time.Now().UTC()}
				attempt, stop := context.WithTimeout(ctx, 2*time.Second)
				data, err := read(attempt, node, kind)
				stop()
				snapshot.After = time.Now().UTC()
				if err != nil {
					snapshot.Error = err.Error()
				} else if !json.Valid(data) {
					snapshot.Error = "invalid monitoring JSON"
				} else {
					snapshot.Data = data
				}
				encoded, err := json.MarshalIndent(snapshot, "", "  ")
				if err == nil {
					err = os.WriteFile(fmt.Sprintf("%s-failure-node%d-%s.json", prefix, node, kind), encoded, 0600)
				}
				if err != nil {
					mu.Lock()
					writeErrors = append(writeErrors, err)
					mu.Unlock()
				}
			}
		}(node)
	}
	wg.Wait()
	return writeErrors
}
