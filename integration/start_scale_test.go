package integration_test

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/client"
)

// This runs the Phase 1 subject-cardinality proof by default. WF_SCALE_STARTS
// can lower the count when sizing a development host.
func TestDistinctStartsScale(t *testing.T) {
	value := os.Getenv("WF_SCALE_STARTS")
	if value == "" {
		value = "100000"
	}
	count, err := strconv.Atoi(value)
	if err != nil || count < 1 || count > 100000 {
		t.Fatalf("invalid WF_SCALE_STARTS %q", value)
	}
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	clients := [3]*client.Client{client.New(all[0]), client.New(all[1]), client.New(all[2])}
	jobs := make(chan int, 256)
	var wg sync.WaitGroup
	var completed atomic.Int64
	var firstErr string
	var firstErrorOnce sync.Once
	start := time.Now()
	for workerID := 0; workerID < 96; workerID++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			c := clients[workerID%len(clients)]
			for id := range jobs {
				workflowID := fmt.Sprintf("scale-%06d", id)
				handle, err := c.Start(ctx, "scale", workflowID, []byte(`null`))
				if err != nil || handle.InvSeq == 0 {
					firstErrorOnce.Do(func() { firstErr = fmt.Sprintf("start %s: handle=%+v err=%v", workflowID, handle, err) })
					continue
				}
				completed.Add(1)
			}
		}(workerID)
	}
	for id := 0; id < count; id++ {
		select {
		case jobs <- id:
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			t.Fatalf("start producer timed out after %d/%d successes: %v", completed.Load(), count, ctx.Err())
		}
	}
	close(jobs)
	wg.Wait()
	if firstErr != "" || completed.Load() != int64(count) {
		t.Fatalf("distinct starts completed=%d/%d first_error=%s context=%v", completed.Load(), count, firstErr, ctx.Err())
	}
	inv, err := all[0].Stream(ctx, "WF_INV")
	if err != nil {
		t.Fatal(err)
	}
	invInfo, err := inv.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	run, err := all[1].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	runInfo, err := run.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if invInfo.State.Msgs != uint64(count) || invInfo.State.NumSubjects != uint64(count) || runInfo.State.Msgs != uint64(count) {
		t.Fatalf("distinct start counts: inv_messages=%d inv_subjects=%d run_messages=%d want=%d", invInfo.State.Msgs, invInfo.State.NumSubjects, runInfo.State.Msgs, count)
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	t.Logf("distinct starts=%d elapsed=%s heap_alloc_before=%d heap_alloc_after=%d heap_alloc_delta=%d", count, time.Since(start), before.Alloc, after.Alloc, int64(after.Alloc)-int64(before.Alloc))
}
