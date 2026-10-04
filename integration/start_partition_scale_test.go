package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
	storeRoot := t.TempDir()
	if parent := os.Getenv("WF_PARTITION_START_STORE_PARENT"); parent != "" {
		if err := os.MkdirAll(parent, 0700); err != nil {
			t.Fatal(err)
		}
		storeRoot, err = os.MkdirTemp(parent, "partition-start-")
		if err != nil {
			t.Fatal(err)
		}
		storeRoot, err = filepath.Abs(storeRoot)
		if err != nil {
			t.Fatal(err)
		}
	}
	cluster, err := testcluster.StartPartitionable(storeRoot, 3)
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
	var events []partitionStartFaultEvent
	applyPartition := func(attempt context.Context, partitioned bool) error {
		at := time.Now().UTC()
		mesh := cluster.RouteMesh()
		if partitioned {
			if err := mesh.PartitionNode(2); err != nil {
				return err
			}
			deadline := time.NewTimer(150 * time.Millisecond)
			defer deadline.Stop()
			poll := time.NewTicker(5 * time.Millisecond)
			defer poll.Stop()
			for cluster.Servers[2].NumRoutes() != 0 || cluster.Servers[0].NumRoutes() < 1 || cluster.Servers[1].NumRoutes() < 1 {
				select {
				case <-attempt.Done():
					return attempt.Err()
				case <-deadline.C:
					return fmt.Errorf("route partition did not isolate node 2: routes=%d,%d,%d", cluster.Servers[0].NumRoutes(), cluster.Servers[1].NumRoutes(), cluster.Servers[2].NumRoutes())
				case <-poll.C:
				}
			}
		} else {
			mesh.Heal()
		}
		isolated.Store(partitioned)
		events = append(events, partitionStartFaultEvent{RequestedAt: at, ObservedAt: time.Now().UTC(), Partitioned: partitioned, Completed: completed.Load(), Routes: [3]int{cluster.Servers[0].NumRoutes(), cluster.Servers[1].NumRoutes(), cluster.Servers[2].NumRoutes()}})
		return nil
	}
	if err := applyPartition(ctx, true); err != nil {
		t.Fatal(err)
	}
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
	workloadDone := make(chan struct{})
	faultStopped := make(chan struct{})
	var faultErr error
	ticks := time.NewTicker(200 * time.Millisecond)
	go func() {
		defer close(faultStopped)
		defer ticks.Stop()
		defer cluster.RouteMesh().Heal()
		faultErr = runPartitionStartFaults(ctx, workloadDone, ticks.C, applyPartition)
		if faultErr != nil {
			cancel()
		}
	}()
	var invMessages, invSubjects, runMessages uint64
	var completedAt time.Time
	var healConfirmedAt time.Time
	defer func() {
		cancel()
		<-faultStopped
		if path := os.Getenv("WF_PARTITION_START_REPORT"); path != "" {
			data, err := json.MarshalIndent(struct {
				Requested        int                        `json:"requested"`
				Completed        int64                      `json:"completed"`
				Retries          int64                      `json:"retries"`
				IsolatedAttempts int64                      `json:"isolated_attempts"`
				StoreRoot        string                     `json:"store_root"`
				StartedAt        time.Time                  `json:"started_at"`
				CompletedAt      time.Time                  `json:"completed_at"`
				HealConfirmedAt  time.Time                  `json:"heal_confirmed_at"`
				Events           []partitionStartFaultEvent `json:"events"`
				InvMessages      uint64                     `json:"inv_messages"`
				InvSubjects      uint64                     `json:"inv_subjects"`
				RunMessages      uint64                     `json:"run_messages"`
				Failed           bool                       `json:"failed"`
			}{count, completed.Load(), retried.Load(), isolatedAttempts.Load(), storeRoot, start, completedAt, healConfirmedAt, events, invMessages, invSubjects, runMessages, t.Failed()}, "", "  ")
			if err == nil {
				err = os.WriteFile(path, data, 0600)
			}
			if err != nil {
				t.Error(err)
			}
		}
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
	wg.Wait()
	completedAt = time.Now().UTC()
	close(workloadDone)
	<-faultStopped
	if faultErr != nil {
		t.Fatal(faultErr)
	}
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
	healConfirmedAt = time.Now().UTC()
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
	invMessages, invSubjects, runMessages = invInfo.State.Msgs, invInfo.State.NumSubjects, runInfo.State.Msgs
	if invInfo.State.Msgs != uint64(count) || invInfo.State.NumSubjects != uint64(count) || runInfo.State.Msgs != uint64(count) {
		t.Fatalf("partitioned start counts: inv_messages=%d inv_subjects=%d run_messages=%d want=%d", invInfo.State.Msgs, invInfo.State.NumSubjects, runInfo.State.Msgs, count)
	}
	t.Logf("partitioned starts=%d elapsed=%s retries=%d attempts_started_on_isolated_node=%d route_changes=%d", count, time.Since(start), retried.Load(), isolatedAttempts.Load(), len(events))
}

// Routes keep toggling until every producer finishes, rather than only the first
// twelve ticks. A supplied tick channel makes the lifecycle contract testable
// without real sleeps or another cluster campaign.
func runPartitionStartFaults(ctx context.Context, workloadDone <-chan struct{}, ticks <-chan time.Time, apply func(context.Context, bool) error) error {
	partitioned := true // The initial isolation is confirmed before producers start.
	for {
		select {
		case <-workloadDone:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-ticks:
			if !ok {
				return fmt.Errorf("partition fault ticks closed before workload completion")
			}
			partitioned = !partitioned
			if err := apply(ctx, partitioned); err != nil {
				return err
			}
		}
	}
}

type partitionStartFaultEvent struct {
	RequestedAt time.Time `json:"requested_at"`
	ObservedAt  time.Time `json:"observed_at"`
	Partitioned bool      `json:"partitioned"`
	Completed   int64     `json:"completed"`
	Routes      [3]int    `json:"routes"`
}
