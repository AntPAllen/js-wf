package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/journal"
	"js-wf/reconcile"
	"js-wf/wf"
	"js-wf/worker"
)

// Each journal represents a worker stopped after the timer request append and
// before the scheduled publish. Run with WF_TIMER_RECONCILE_SCALE=1.
func TestTwoHundredMissingTimerSchedulesReconcile(t *testing.T) {
	if os.Getenv("WF_TIMER_RECONCILE_SCALE") == "" {
		t.Skip("set WF_TIMER_RECONCILE_SCALE=1 for the 200 missing-schedule proof")
	}
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	const typ, count = "timer-gap", 200
	c := client.New(all[0])
	j := journal.New(all[0])
	for index := 0; index < count; index++ {
		id := fmt.Sprintf("gap-%03d", index)
		if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
			t.Fatalf("start %s: %v", id, err)
		}
	}
	run, err := all[0].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	if err := run.Purge(ctx); err != nil {
		t.Fatal(err)
	}
	request, err := json.Marshal(struct {
		Kind          string    `json:"kind"`
		Name          string    `json:"name"`
		DurationNanos int64     `json:"duration_nanos"`
		FireAt        time.Time `json:"fire_at"`
	}{Kind: "timer", Name: "gap", DurationNanos: int64(time.Second), FireAt: time.Now().Add(-time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < count; index++ {
		id := fmt.Sprintf("gap-%03d", index)
		seq, err := j.Append(ctx, typ, id, journal.Entry{Index: 0, Kind: journal.Started}, 0)
		if err != nil {
			t.Fatalf("start journal %s: %v", id, err)
		}
		seq, err = j.Append(ctx, typ, id, journal.Entry{Index: 1, Kind: journal.StepRequested, Payload: request}, seq)
		if err != nil {
			t.Fatalf("request %s: %v", id, err)
		}
		if _, err := j.Append(ctx, typ, id, journal.Entry{Index: 2, Kind: journal.Suspended, Payload: json.RawMessage(`{"waiting_on":"timer:gap"}`)}, seq); err != nil {
			t.Fatalf("suspend %s: %v", id, err)
		}
	}
	info, err := run.Info(ctx)
	if err != nil || info.State.Msgs != 0 {
		t.Fatalf("missing schedules left run messages: info=%+v err=%v", info, err)
	}
	workersCtx, stopWorkers := context.WithCancel(ctx)
	var workers sync.WaitGroup
	workerErrors := make(chan error, 3)
	for index := 0; index < 3; index++ {
		w, err := worker.New(ctx, all[index], fmt.Sprintf("gap-worker-%d", index), map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
			if err := wf.Sleep(c, "gap", time.Second); err != nil {
				return nil, err
			}
			return json.RawMessage(`true`), nil
		}}, worker.WithPartitionConcurrency(16))
		if err != nil {
			t.Fatal(err)
		}
		workers.Add(1)
		go func(index int) {
			defer workers.Done()
			workerErrors <- w.RunAssigned(workersCtx, index, 3)
		}(index)
	}
	defer func() {
		stopWorkers()
		workers.Wait()
		close(workerErrors)
		for err := range workerErrors {
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("worker exited: %v", err)
			}
		}
	}()
	control, stopControl := context.WithTimeout(ctx, 500*time.Millisecond)
	_, err = c.Await(control, typ, "gap-000")
	stopControl()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("without reconciler a missing schedule must stall: %v", err)
	}
	loopCtx, stopLoop := context.WithCancel(ctx)
	loopDone := make(chan error, 1)
	go func() {
		loopDone <- reconcile.RunTimerLoop(loopCtx, all[2], "gap-reconciler", 100*time.Millisecond, 20)
	}()
	defer func() {
		stopLoop()
		if err := <-loopDone; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("reconciler exited: %v", err)
		}
	}()
	var results sync.WaitGroup
	resultErrors := make(chan error, count)
	for index := 0; index < count; index++ {
		results.Add(1)
		go func(index int) {
			defer results.Done()
			id := fmt.Sprintf("gap-%03d", index)
			value, err := c.Await(ctx, typ, id)
			if err != nil || string(value) != "true" {
				resultErrors <- fmt.Errorf("await %s: value=%s err=%v", id, value, err)
				return
			}
			records, _, err := j.Read(ctx, typ, id)
			if err != nil || len(records) < 4 || records[len(records)-1].Kind != journal.Completed {
				resultErrors <- fmt.Errorf("journal %s: entries=%d err=%v", id, len(records), err)
			}
		}(index)
	}
	results.Wait()
	close(resultErrors)
	for err := range resultErrors {
		t.Error(err)
	}
	if ctx.Err() != nil {
		t.Fatalf("200 timers did not recover before deadline: %v", ctx.Err())
	}
	if t.Failed() {
		return
	}
	t.Logf("reconciler completed all %d requests lacking scheduled publishes", count)
}
