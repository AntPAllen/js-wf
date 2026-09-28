package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/history"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/testcluster"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/anishathalye/porcupine"
)

// TestSignalsUnderRoutePartition proves the Phase 6 queue target while one
// signaller's server loses quorum. It is opt-in because the full proof writes
// and audits 10,000 signal and journal records.
func TestSignalsUnderRoutePartition(t *testing.T) {
	if os.Getenv("WF_SIGNAL_FAULT_SCALE") == "" {
		t.Skip("set WF_SIGNAL_FAULT_SCALE=1 for the 10,000-signal fault proof")
	}
	perWriter := 100
	if value := os.Getenv("WF_SIGNAL_FAULT_PER_WRITER"); value != "" {
		var err error
		perWriter, err = strconv.Atoi(value)
		if err != nil || perWriter < 1 || perWriter > 100 {
			t.Fatalf("invalid WF_SIGNAL_FAULT_PER_WRITER %q", value)
		}
	}
	const writers = 100
	const id = "signal-route-fault"
	cluster, err := testcluster.StartPartitionable(t.TempDir(), 3)
	if err != nil {
		t.Fatal(err)
	}
	all, cluster := setupCluster(t, cluster)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	if _, err := client.New(all[0]).Start(ctx, "test", id, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	type published struct {
		sequence uint64
		hash     string
	}
	results := make([]published, writers*perWriter)
	recorder := &history.Recorder{}
	var attemptsOnIsolated atomic.Int64
	var failedOnIsolated atomic.Int64
	var completedOnMajority atomic.Int64
	var retries atomic.Int64
	var isolated atomic.Bool
	mesh := cluster.RouteMesh()
	if err := mesh.PartitionNode(2); err != nil {
		t.Fatal(err)
	}
	partitionDeadline := time.Now().Add(5 * time.Second)
	for cluster.Servers[2].NumRoutes() != 0 || cluster.Servers[0].NumRoutes() < 1 || cluster.Servers[1].NumRoutes() < 1 {
		if time.Now().After(partitionDeadline) {
			t.Fatalf("route partition did not converge: routes=%d,%d,%d", cluster.Servers[0].NumRoutes(), cluster.Servers[1].NumRoutes(), cluster.Servers[2].NumRoutes())
		}
		time.Sleep(10 * time.Millisecond)
	}
	isolated.Store(true)
	defer mesh.Heal()
	start := make(chan struct{})
	errorsCh := make(chan error, writers)
	var wg sync.WaitGroup
	for writer := 0; writer < writers; writer++ {
		wg.Add(1)
		go func(writer int) {
			defer wg.Done()
			<-start
			c := client.NewObserved(all[writer%len(all)], recorder)
			for number := 0; number < perWriter; number++ {
				payload := []byte(fmt.Sprintf(`{"writer":%d,"number":%d}`, writer, number))
				key := fmt.Sprintf("writer-%d-number-%d", writer, number)
				for ctx.Err() == nil {
					attemptedWhileIsolated := isolated.Load()
					if writer%len(all) == 2 && attemptedWhileIsolated {
						attemptsOnIsolated.Add(1)
					}
					attempt, stop := context.WithTimeout(ctx, 3*time.Second)
					sequence, err := c.Signal(attempt, "test", id, "go", payload, key)
					stop()
					if err == nil || errors.Is(err, client.ErrEnqueueUnknown) && sequence != 0 {
						if writer%len(all) != 2 && attemptedWhileIsolated && isolated.Load() {
							completedOnMajority.Add(1)
						}
						digest := sha256.Sum256(payload)
						results[writer*perWriter+number] = published{sequence, hex.EncodeToString(digest[:])}
						break
					}
					if errors.Is(err, client.ErrSignalMismatch) {
						errorsCh <- fmt.Errorf("writer %d signal %d mismatched original payload", writer, number)
						return
					}
					if writer%len(all) == 2 && attemptedWhileIsolated {
						failedOnIsolated.Add(1)
					}
					retries.Add(1)
					time.Sleep(20 * time.Millisecond)
				}
				if results[writer*perWriter+number].sequence == 0 {
					errorsCh <- fmt.Errorf("writer %d signal %d: %v", writer, number, ctx.Err())
					return
				}
			}
		}(writer)
	}
	close(start)
	// Wait for a committed append on the majority and a failed call on the
	// isolated server before restoring the routes for same-key retries.
	faultDeadline := time.Now().Add(8 * time.Second)
	for (completedOnMajority.Load() == 0 || failedOnIsolated.Load() == 0) && time.Now().Before(faultDeadline) {
		time.Sleep(20 * time.Millisecond)
	}
	mesh.Heal()
	isolated.Store(false)
	wg.Wait()
	close(errorsCh)
	for err := range errorsCh {
		t.Error(err)
	}
	if t.Failed() {
		return
	}
	if attemptsOnIsolated.Load() == 0 || failedOnIsolated.Load() == 0 || completedOnMajority.Load() == 0 {
		t.Fatalf("route fault not exercised: isolated_attempts=%d isolated_failures=%d majority_completions=%d", attemptsOnIsolated.Load(), failedOnIsolated.Load(), completedOnMajority.Load())
	}
	for writer := 0; writer < writers; writer++ {
		c := client.New(all[writer%len(all)])
		payload := []byte(fmt.Sprintf(`{"writer":%d,"number":0}`, writer))
		key := fmt.Sprintf("writer-%d-number-0", writer)
		sequence, err := c.Signal(ctx, "test", id, "go", payload, key)
		if err != nil || sequence != results[writer*perWriter].sequence {
			t.Fatalf("writer %d same-key retry: sequence=%d err=%v", writer, sequence, err)
		}
		if _, err := c.Signal(ctx, "test", id, "go", []byte(`"changed"`), key); !errors.Is(err, client.ErrSignalMismatch) {
			t.Fatalf("writer %d changed-payload retry: %v", writer, err)
		}
	}
	// The unknown-outcome model has one global search partition. Checking the
	// completed appends gives a tractable linearizability proof; retained stream
	// and same-key retry checks below resolve every uncertain attempt.
	var completed []client.Operation
	for _, operation := range recorder.Snapshot() {
		if operation.Op != "signal" {
			continue
		}
		var result struct {
			Status string `json:"status"`
		}
		if err := json.Unmarshal(operation.Result, &result); err != nil {
			t.Fatal(err)
		}
		if result.Status == "signaled" || result.Status == "enqueue_unknown" {
			completed = append(completed, operation)
		}
	}
	if len(completed) != writers*perWriter {
		t.Fatalf("completed signal calls=%d, want %d", len(completed), writers*perWriter)
	}
	if result, err := history.CheckSignals(completed, time.Minute); err != nil || result != porcupine.Ok {
		t.Fatalf("signal queue history=%s err=%v", result, err)
	}
	stream, err := all[0].Stream(ctx, "WF_SIG")
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != uint64(len(results)) {
		t.Fatalf("retained signals=%d, want %d", info.State.Msgs, len(results))
	}
	seen := make(map[uint64]bool, len(results))
	for writer := 0; writer < writers; writer++ {
		var prior uint64
		for number := 0; number < perWriter; number++ {
			item := results[writer*perWriter+number]
			if item.sequence <= prior || seen[item.sequence] {
				t.Fatalf("duplicate or out-of-order signal sequence %d", item.sequence)
			}
			seen[item.sequence] = true
			prior = item.sequence
			message, err := stream.GetMsg(ctx, item.sequence)
			if err != nil {
				t.Fatal(err)
			}
			if message.Subject != "wf.sig.test."+id+".go" || message.Header.Get("Wf-Input-SHA256") != item.hash {
				t.Fatalf("signal %d subject or hash mismatch", item.sequence)
			}
		}
	}
	for sequence := uint64(1); sequence <= uint64(len(results)); sequence++ {
		if !seen[sequence] {
			t.Fatalf("gap at WF_SIG sequence %d", sequence)
		}
	}
	w, err := worker.New(ctx, all[1], "signal-fault-drain", map[string]worker.Handler{"test": func(_ *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`null`), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stopWorker := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.RunPartition(workerCtx, identity.Partition("test", id, provision.Partitions)) }()
	result, err := client.New(all[0]).Await(ctx, "test", id)
	stopWorker()
	if runErr := <-done; runErr != nil && !errors.Is(runErr, context.Canceled) {
		t.Fatalf("worker: %v", runErr)
	}
	if err != nil || string(result) != "null" {
		t.Fatalf("workflow result=%s err=%v", result, err)
	}
	entries, _, err := journal.New(all[0]).Read(ctx, "test", id)
	if err != nil {
		t.Fatal(err)
	}
	var consumed int
	for _, entry := range entries {
		if entry.Kind != journal.SignalConsumed {
			continue
		}
		var event struct {
			Sequence uint64 `json:"sig_seq"`
		}
		if err := json.Unmarshal(entry.Payload, &event); err != nil || event.Sequence != uint64(consumed+1) {
			t.Fatalf("journal signal %d: sequence=%d err=%v", consumed, event.Sequence, err)
		}
		consumed++
	}
	if consumed != len(results) {
		t.Fatalf("journal consumed %d/%d signals", consumed, len(results))
	}
	if _, err := integrity.Check(ctx, all[0]); err != nil {
		t.Fatalf("retained stream integrity: %v", err)
	}
	t.Logf("signals=%d retries=%d isolated_attempts=%d isolated_failures=%d majority_completions=%d", len(results), retries.Load(), attemptsOnIsolated.Load(), failedOnIsolated.Load(), completedOnMajority.Load())
}
