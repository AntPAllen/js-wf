package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go/jetstream"
)

func TestScheduledTimerSurvivesTwoFullClusterRestarts(t *testing.T) {
	all, cluster := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()
	const typ, id = "restart", "scheduled-timer"
	var calls atomic.Int32
	handler := func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		calls.Add(1)
		if err := wf.Sleep(c, "after-restart", 30*time.Second); err != nil {
			return nil, err
		}
		return json.RawMessage(`true`), nil
	}
	w, err := worker.New(ctx, all[1], "timer-before-restarts", map[string]worker.Handler{typ: handler})
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stopWorker := context.WithCancel(ctx)
	done := make(chan error, 1)
	part := identity.Partition(typ, id, provision.Partitions)
	go func() { done <- w.RunPartition(workerCtx, part) }()
	c := client.New(all[0])
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	j := journal.New(all[2])
	var fireAt time.Time
	for ctx.Err() == nil {
		records, _, err := j.Read(ctx, typ, id)
		if err != nil {
			t.Fatal(err)
		}
		if len(records) > 0 && records[len(records)-1].Kind == journal.Suspended {
			for _, record := range records {
				if record.Kind != journal.StepRequested {
					continue
				}
				var request struct {
					Kind   string    `json:"kind"`
					FireAt time.Time `json:"fire_at"`
				}
				if json.Unmarshal(record.Payload, &request) == nil && request.Kind == "timer" {
					fireAt = request.FireAt
				}
			}
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if fireAt.IsZero() || calls.Load() != 1 {
		t.Fatalf("timer did not suspend: fire_at=%s calls=%d err=%v", fireAt, calls.Load(), ctx.Err())
	}
	stopWorker()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	for restart := 0; restart < 2; restart++ {
		if !time.Now().Before(fireAt) {
			t.Fatalf("restart %d began after scheduled fire time %s", restart+1, fireAt)
		}
		for i := range cluster.Servers {
			cluster.KillNode(i)
		}
		for i := range cluster.Servers {
			if err := cluster.RestartNode(i); err != nil {
				t.Fatalf("restart %d node %d: %v", restart+1, i, err)
			}
		}
		all = make([]jetstream.JetStream, len(cluster.Clients))
		for i, nc := range cluster.Clients {
			all[i], err = jetstream.New(nc)
			if err != nil {
				t.Fatal(err)
			}
		}
		j = journal.New(all[2])
		var recovered bool
		var lastErr error
		for ctx.Err() == nil {
			attempt, done := context.WithTimeout(ctx, 3*time.Second)
			lastErr = func() error {
				if _, err := all[0].AccountInfo(attempt); err != nil {
					return err
				}
				for _, name := range []string{"WF_RUN", "WF_JRN", "KV_WF_LEASE", "KV_WF_STATE"} {
					stream, err := all[1].Stream(attempt, name)
					if err != nil {
						return err
					}
					info, err := stream.Info(attempt)
					if err != nil {
						return err
					}
					if info.Cluster == nil || info.Cluster.Leader == "" || len(info.Cluster.Replicas) != 2 {
						return fmt.Errorf("stream %s has no three-node leader", name)
					}
				}
				records, _, err := j.Read(attempt, typ, id)
				if err != nil {
					return err
				}
				if len(records) == 0 || records[len(records)-1].Kind != journal.Suspended {
					return fmt.Errorf("timer journal is not suspended after restart")
				}
				return nil
			}()
			done()
			if lastErr == nil {
				recovered = true
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if !recovered {
			t.Fatalf("cluster after restart %d: %v (context: %v)", restart+1, lastErr, ctx.Err())
		}
		if !time.Now().Before(fireAt) {
			t.Fatalf("restart %d ended after scheduled fire time %s", restart+1, fireAt)
		}
	}
	var lastWorkerErr error
	for ctx.Err() == nil {
		attempt, done := context.WithTimeout(ctx, 5*time.Second)
		w, lastWorkerErr = worker.New(attempt, all[1], "timer-after-restarts", map[string]worker.Handler{typ: handler})
		done()
		if lastWorkerErr == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if lastWorkerErr != nil || ctx.Err() != nil {
		t.Fatalf("worker after restarts: %v (context: %v)", lastWorkerErr, ctx.Err())
	}
	workerCtx, stopWorker = context.WithCancel(ctx)
	done = make(chan error, 1)
	go func() { done <- w.RunPartition(workerCtx, part) }()
	c = client.New(all[0])
	value, err := c.Await(ctx, typ, id)
	if err != nil || string(value) != "true" {
		t.Fatalf("scheduled result after restarts: value=%s err=%v", value, err)
	}
	stopWorker()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	records, _, err := journal.New(all[2]).Read(ctx, typ, id)
	if err != nil {
		t.Fatal(err)
	}
	expected := []journal.Kind{journal.Started, journal.StepRequested, journal.Suspended, journal.StepCompleted, journal.Completed}
	if len(records) != len(expected) {
		t.Fatalf("timer journal has %d entries, want %d: %+v", len(records), len(expected), records)
	}
	for i, record := range records {
		if record.Kind != expected[i] {
			t.Fatalf("timer journal entry %d kind=%s, want %s", i, record.Kind, expected[i])
		}
	}
	if calls.Load() < 2 {
		t.Fatalf("handler replay count=%d, want initial suspension and completion", calls.Load())
	}
	if calls.Load() > 2 {
		t.Logf("handler replayed %d times across restart redelivery; metrics=%+v", calls.Load(), w.Metrics())
	}
	if _, err := integrity.Check(ctx, all[0]); err != nil {
		t.Fatalf("post-restart integrity: %v", err)
	}
	if time.Now().Before(fireAt) {
		t.Fatalf("timer completed before fire time %s", fireAt)
	}
	t.Logf("scheduled timer survived two full restarts and fired at %s", fireAt.Format(time.RFC3339Nano))
}
