package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/provision"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go/jetstream"
)

// Run with WF_DISPATCH_SCALE=1. Six workers own all 64 partitions and process
// 10,000 short invocations. A separate KV key is held for each running handler;
// Create's CAS rejects any simultaneous execution of the same invocation.
func TestTenThousandShortInvocationsAcrossAllPartitions(t *testing.T) {
	if os.Getenv("WF_DISPATCH_SCALE") == "" {
		t.Skip("set WF_DISPATCH_SCALE=1 for the 10,000-invocation dispatch proof")
	}
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	running, err := all[0].CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "WF_RUNNING_TEST", Storage: jetstream.FileStorage, Replicas: 3, History: 1})
	if err != nil {
		t.Fatal(err)
	}
	var calls, collisions, deleteErrors atomic.Int64
	handler := func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
		var id string
		if err := json.Unmarshal(input, &id); err != nil {
			return nil, err
		}
		if _, err := running.Create(c.Context(), id, []byte("running")); err != nil {
			collisions.Add(1)
			return nil, fmt.Errorf("concurrent handler or KV create failure for %s: %w", id, err)
		}
		defer func() {
			if err := running.Delete(c.Context(), id); err != nil {
				deleteErrors.Add(1)
			}
		}()
		calls.Add(1)
		return json.RawMessage(input), nil
	}
	workerCtx, stopWorkers := context.WithCancel(ctx)
	defer stopWorkers()
	workerDone := make(chan error, 6)
	for index := 0; index < 6; index++ {
		w, err := worker.New(ctx, all[index%len(all)], fmt.Sprintf("dispatch-%d", index), map[string]worker.Handler{"short": handler})
		if err != nil {
			t.Fatal(err)
		}
		go func(index int, w *worker.Worker) { workerDone <- w.RunAssigned(workerCtx, index, 6) }(index, w)
	}
	run, err := all[0].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	for {
		info, err := run.Info(ctx)
		if err == nil && info.State.Consumers == int(provision.Partitions) {
			break
		}
		select {
		case workerErr := <-workerDone:
			t.Fatalf("worker exited during consumer creation: %v", workerErr)
		default:
		}
		if ctx.Err() != nil {
			t.Fatalf("64 consumers not ready: info=%+v err=%v", info, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	clients := [3]*client.Client{client.New(all[0]), client.New(all[1]), client.New(all[2])}
	startTime := time.Now()
	const count = 10000
	jobs := make(chan int, count)
	errorsFound := make(chan error, 1)
	var group sync.WaitGroup
	for caller := 0; caller < 64; caller++ {
		group.Add(1)
		go func(caller int) {
			defer group.Done()
			for index := range jobs {
				id := fmt.Sprintf("job-%05d", index)
				payload, _ := json.Marshal(id)
				_, err := clients[caller%len(clients)].Start(ctx, "short", id, payload)
				if err != nil {
					select {
					case errorsFound <- fmt.Errorf("start %s: %w", id, err):
					default:
					}
					return
				}
			}
		}(caller)
	}
	for index := 0; index < count; index++ {
		jobs <- index
	}
	close(jobs)
	group.Wait()
	select {
	case err := <-errorsFound:
		t.Fatal(err)
	default:
	}
	startDuration := time.Since(startTime)
	for {
		info, err := run.Info(ctx)
		if err == nil && info.State.Msgs == 0 && calls.Load() >= count {
			break
		}
		select {
		case workerErr := <-workerDone:
			t.Fatalf("worker exited while draining: %v", workerErr)
		default:
		}
		if ctx.Err() != nil {
			t.Fatalf("dispatch did not drain: run=%+v calls=%d collisions=%d err=%v", info, calls.Load(), collisions.Load(), err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	completionDuration := time.Since(startTime)
	if collisions.Load() != 0 || deleteErrors.Load() != 0 {
		t.Fatalf("handler markers: calls=%d collisions=%d delete_errors=%d", calls.Load(), collisions.Load(), deleteErrors.Load())
	}
	for _, name := range []string{"WF_INV", "WF_JRN"} {
		stream, err := all[1].Stream(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		info, err := stream.Info(ctx)
		wantMessages := uint64(count)
		if name == "WF_JRN" {
			wantMessages *= 2
		}
		if err != nil || info.State.Msgs != wantMessages || info.State.NumSubjects != count {
			t.Fatalf("%s after dispatch: info=%+v want_msgs=%d err=%v", name, info, wantMessages, err)
		}
	}
	resultJobs := make(chan int, count)
	resultErrors := make(chan error, 1)
	group = sync.WaitGroup{}
	for caller := 0; caller < 64; caller++ {
		group.Add(1)
		go func(caller int) {
			defer group.Done()
			for index := range resultJobs {
				id := fmt.Sprintf("job-%05d", index)
				result, err := clients[caller%len(clients)].Await(ctx, "short", id)
				var got string
				if err == nil {
					err = json.Unmarshal(result, &got)
				}
				if err != nil || got != id {
					select {
					case resultErrors <- fmt.Errorf("result %s: got=%q err=%v", id, got, err):
					default:
					}
					return
				}
			}
		}(caller)
	}
	for index := 0; index < count; index++ {
		resultJobs <- index
	}
	close(resultJobs)
	group.Wait()
	select {
	case err := <-resultErrors:
		t.Fatal(err)
	default:
	}
	stopWorkers()
	for range 6 {
		if err := <-workerDone; err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	}
	t.Logf("invocations=%d partitions=%d workers=6 start_time=%s drain_time=%s handler_calls=%d", count, provision.Partitions, startDuration, completionDuration, calls.Load())
}
