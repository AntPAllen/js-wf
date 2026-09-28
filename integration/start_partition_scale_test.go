package integration_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/testcluster"
)

// TestDistinctStartsUnderRoutePartitions repeats the Phase 1 count proof while
// node 2 loses and regains every cluster route at 200 ms intervals.
func TestDistinctStartsUnderRoutePartitions(t *testing.T) {
	value := os.Getenv("WF_PARTITION_STARTS")
	if value == "" {
		value = "100000"
	}
	count, err := strconv.Atoi(value)
	if err != nil || count < 1 || count > 100000 {
		t.Fatalf("invalid WF_PARTITION_STARTS %q", value)
	}
	cluster, err := testcluster.StartPartitionable(t.TempDir(), 3)
	if err != nil {
		t.Fatal(err)
	}
	all, cluster := setupCluster(t, cluster)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	clients := [3]*client.Client{client.New(all[0]), client.New(all[1]), client.New(all[2])}
	jobs := make(chan int, 256)
	var wg sync.WaitGroup
	var completed atomic.Int64
	var retried atomic.Int64
	var isolated atomic.Bool
	var isolatedAttempts atomic.Int64
	var firstErr string
	var firstErrorOnce sync.Once
	if err := cluster.RouteMesh().PartitionNode(2); err != nil {
		t.Fatal(err)
	}
	isolated.Store(true)
	start := time.Now()
	for workerID := 0; workerID < 96; workerID++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			c := clients[workerID%len(clients)]
			for id := range jobs {
				workflowID := fmt.Sprintf("partition-%06d", id)
				for ctx.Err() == nil {
					if workerID%len(clients) == 2 && isolated.Load() {
						isolatedAttempts.Add(1)
					}
					attempt, stop := context.WithTimeout(ctx, 8*time.Second)
					handle, err := c.Start(attempt, "scale", workflowID, []byte(`null`))
					if err == nil {
						stop()
						completed.Add(1)
						break
					}
					if errors.Is(err, client.ErrAlreadyStarted) && handle.InvSeq != 0 {
						// A prior attempt may have committed WF_INV without
						// reaching WF_RUN. The enqueue ID is stable and deduped.
						err = c.Enqueue(attempt, "scale", workflowID, "start:scale."+workflowID+":"+strconv.FormatUint(handle.InvSeq, 10))
						if err == nil {
							stop()
							completed.Add(1)
							break
						}
					}
					stop()
					retried.Add(1)
					if ctx.Err() != nil {
						firstErrorOnce.Do(func() { firstErr = fmt.Sprintf("start %s: %v", workflowID, err) })
						break
					}
					time.Sleep(50 * time.Millisecond)
				}
			}
		}(workerID)
	}
	faultDone := make(chan error, 1)
	go func() {
		mesh := cluster.RouteMesh()
		defer mesh.Heal()
		for cycle := 0; cycle < 12; cycle++ {
			if cycle%2 == 0 {
				if err := mesh.PartitionNode(2); err != nil {
					faultDone <- err
					return
				}
				deadline := time.Now().Add(150 * time.Millisecond)
				for {
					if cluster.Servers[2].NumRoutes() == 0 && cluster.Servers[0].NumRoutes() >= 1 && cluster.Servers[1].NumRoutes() >= 1 {
						break
					}
					isolated.Store(true)
					if time.Now().After(deadline) {
						faultDone <- fmt.Errorf("route partition did not isolate node 2: routes=%d,%d,%d", cluster.Servers[0].NumRoutes(), cluster.Servers[1].NumRoutes(), cluster.Servers[2].NumRoutes())
						return
					}
					time.Sleep(5 * time.Millisecond)
				}
			} else {
				mesh.Heal()
				isolated.Store(false)
			}
			select {
			case <-ctx.Done():
				faultDone <- ctx.Err()
				return
			case <-time.After(200 * time.Millisecond):
			}
		}
		faultDone <- nil
	}()
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
	if err := <-faultDone; err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if firstErr != "" || completed.Load() != int64(count) {
		t.Fatalf("partitioned starts completed=%d/%d retries=%d first_error=%s context=%v", completed.Load(), count, retried.Load(), firstErr, ctx.Err())
	}
	if isolatedAttempts.Load() == 0 {
		t.Fatal("no start attempt began on the isolated node")
	}
	routeDeadline := time.Now().Add(10 * time.Second)
	for cluster.Servers[0].NumRoutes() != 2 || cluster.Servers[1].NumRoutes() != 2 || cluster.Servers[2].NumRoutes() != 2 {
		if time.Now().After(routeDeadline) {
			t.Fatalf("routes did not heal: %d,%d,%d", cluster.Servers[0].NumRoutes(), cluster.Servers[1].NumRoutes(), cluster.Servers[2].NumRoutes())
		}
		time.Sleep(20 * time.Millisecond)
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
		t.Fatalf("partitioned start counts: inv_messages=%d inv_subjects=%d run_messages=%d want=%d", invInfo.State.Msgs, invInfo.State.NumSubjects, runInfo.State.Msgs, count)
	}
	t.Logf("partitioned starts=%d elapsed=%s retries=%d attempts_started_on_isolated_node=%d", count, time.Since(start), retried.Load(), isolatedAttempts.Load())
}
