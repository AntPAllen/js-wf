package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"sync"
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
)

// Every workflow records server time before deriving its duration. Replay
// therefore requests the same duration even after the common deadline passes.
func TestThreeThousandTimersDueInOneSecond(t *testing.T) {
	if os.Getenv("WF_TIMER_FANIN_SCALE") != "1" {
		t.Skip("set WF_TIMER_FANIN_SCALE=1 for the 3,000-timer fan-in proof")
	}
	runTimerFanIn(t, 3000)
}

func TestTenThousandTimersDueInOneSecond(t *testing.T) {
	if os.Getenv("WF_TIMER_FANIN_10000") != "1" {
		t.Skip("set WF_TIMER_FANIN_10000=1 for the 10,000-timer fan-in proof")
	}
	runTimerFanIn(t, 10000)
}

func runTimerFanIn(t *testing.T, count int) {
	t.Helper()
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Minute)
	defer cancel()
	const typ, workerCount = "timer-fanin", 6
	// The next whole second keeps the expected due times away from a second
	// boundary while leaving enough time to schedule and audit up to 10,000.
	target := time.Now().UTC().Add(180 * time.Second).Truncate(time.Second).Add(time.Second)
	type input struct {
		Index  int       `json:"index"`
		Target time.Time `json:"target"`
	}
	completedAt := make([]atomic.Int64, count)
	handler := func(c *wf.Context, raw json.RawMessage) (json.RawMessage, error) {
		var job input
		if err := json.Unmarshal(raw, &job); err != nil {
			return nil, err
		}
		if job.Index < 0 || job.Index >= count || !job.Target.Equal(target) {
			return nil, fmt.Errorf("invalid fan-in input: %+v", job)
		}
		now, err := wf.Now(c)
		if err != nil {
			return nil, err
		}
		if err := wf.Sleep(c, "aligned", job.Target.Sub(now)); err != nil {
			return nil, err
		}
		completedAt[job.Index].CompareAndSwap(0, time.Now().UnixNano())
		return json.Marshal(job.Index)
	}
	workerCtx, stopWorkers := context.WithCancel(ctx)
	defer stopWorkers()
	workers := make([]*worker.Worker, workerCount)
	workerDone := make(chan error, workerCount)
	for index := range workers {
		w, err := worker.New(ctx, all[index%len(all)], fmt.Sprintf("fanin-worker-%d", index), map[string]worker.Handler{typ: handler}, worker.WithPartitionConcurrency(32))
		if err != nil {
			t.Fatal(err)
		}
		workers[index] = w
		go func(index int, w *worker.Worker) { workerDone <- w.RunAssigned(workerCtx, index, workerCount) }(index, w)
	}
	run, err := all[0].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	for ctx.Err() == nil {
		info, err := run.Info(ctx)
		if err == nil && info.State.Consumers == int(provision.Partitions) {
			break
		}
		select {
		case workerErr := <-workerDone:
			t.Fatalf("worker exited while creating consumers: %v", workerErr)
		default:
		}
		time.Sleep(50 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatalf("consumers not ready: %v", ctx.Err())
	}
	clients := [3]*client.Client{client.New(all[0]), client.New(all[1]), client.New(all[2])}
	jobs := make(chan int, count)
	startErrors := make(chan error, 1)
	var starters sync.WaitGroup
	for caller := 0; caller < 64; caller++ {
		starters.Add(1)
		go func(caller int) {
			defer starters.Done()
			for index := range jobs {
				id := fmt.Sprintf("fanin-%04d", index)
				payload, _ := json.Marshal(input{Index: index, Target: target})
				if _, err := clients[caller%len(clients)].Start(ctx, typ, id, payload); err != nil {
					select {
					case startErrors <- fmt.Errorf("start %s: %w", id, err):
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
	starters.Wait()
	select {
	case err := <-startErrors:
		t.Fatal(err)
	default:
	}
	for ctx.Err() == nil {
		var scheduled uint64
		for _, w := range workers {
			scheduled += w.Metrics().TimersScheduled
		}
		if scheduled == uint64(count) {
			break
		}
		select {
		case workerErr := <-workerDone:
			t.Fatalf("worker exited while scheduling: %v", workerErr)
		default:
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatalf("timers not all scheduled before deadline: %v", ctx.Err())
	}
	fireAt := make([]time.Time, count)
	j := journal.New(all[1])
	for index := 0; index < count; index++ {
		id := fmt.Sprintf("fanin-%04d", index)
		records, _, err := j.Read(ctx, typ, id)
		if err != nil {
			t.Fatalf("journal %s: %v", id, err)
		}
		for _, record := range records {
			if record.Kind != journal.StepRequested {
				continue
			}
			var request struct {
				Kind   string    `json:"kind"`
				FireAt time.Time `json:"fire_at"`
			}
			if json.Unmarshal(record.Payload, &request) == nil && request.Kind == "timer" {
				fireAt[index] = request.FireAt
			}
		}
		if fireAt[index].IsZero() {
			t.Fatalf("journal %s lacks timer fire_at: %+v", id, records)
		}
	}
	var earliest, latest time.Time
	for _, due := range fireAt {
		if earliest.IsZero() || due.Before(earliest) {
			earliest = due
		}
		if due.After(latest) {
			latest = due
		}
	}
	if !time.Now().Before(earliest) {
		t.Fatalf("earliest timer due before fan-in audit: %s", earliest)
	}
	if !earliest.Truncate(time.Second).Equal(latest.Truncate(time.Second)) {
		t.Fatalf("journaled timer deadlines do not share one UTC second: first=%s last=%s span=%s", earliest, latest, latest.Sub(earliest))
	}
	maxPerPartition := 0
	partitionCounts := make([]int, provision.Partitions)
	for index := 0; index < count; index++ {
		part := identity.Partition(typ, fmt.Sprintf("fanin-%04d", index), provision.Partitions)
		partitionCounts[part]++
	}
	for part, partitionCount := range partitionCounts {
		if partitionCount > maxPerPartition {
			maxPerPartition = partitionCount
		}
		consumer, err := run.Consumer(ctx, fmt.Sprintf("WF_P_%02d", part))
		if err != nil {
			t.Fatalf("partition %d consumer: %v", part, err)
		}
		info, err := consumer.Info(ctx)
		if err != nil {
			t.Fatalf("partition %d consumer info: %v", part, err)
		}
		if info.Config.MaxAckPending < partitionCount {
			t.Fatalf("partition %d MaxAckPending=%d for %d timers: err=%v", part, info.Config.MaxAckPending, partitionCount, err)
		}
	}
	resultJobs := make(chan int, count)
	resultErrors := make(chan error, 1)
	var readers sync.WaitGroup
	for reader := 0; reader < 64; reader++ {
		readers.Add(1)
		go func(reader int) {
			defer readers.Done()
			c := client.New(all[reader%len(all)])
			for index := range resultJobs {
				id := fmt.Sprintf("fanin-%04d", index)
				result, err := c.Await(ctx, typ, id)
				if err != nil || string(result) != fmt.Sprint(index) {
					select {
					case resultErrors <- fmt.Errorf("result %s=%s: %w", id, result, err):
					default:
					}
					return
				}
			}
		}(reader)
	}
	for index := 0; index < count; index++ {
		resultJobs <- index
	}
	close(resultJobs)
	readers.Wait()
	select {
	case err := <-resultErrors:
		t.Fatal(err)
	default:
	}
	lateness := make([]time.Duration, count)
	completedFirst, completedLast := time.Time{}, time.Time{}
	for index, due := range fireAt {
		when := completedAt[index].Load()
		if when == 0 {
			t.Fatalf("timer %d returned no handler completion", index)
		}
		completed := time.Unix(0, when)
		if completed.Before(due) {
			t.Fatalf("timer %d completed at %s before fire_at %s", index, completed, due)
		}
		if completedFirst.IsZero() || completed.Before(completedFirst) {
			completedFirst = completed
		}
		if completed.After(completedLast) {
			completedLast = completed
		}
		lateness[index] = completed.Sub(due)
	}
	sort.Slice(lateness, func(i, j int) bool { return lateness[i] < lateness[j] })
	p99 := lateness[int(math.Ceil(.99*float64(count)))-1]
	if p99 >= 30*time.Second || lateness[count-1] >= 5*time.Minute {
		t.Fatalf("fan-in liveness: p99=%s max=%s, want p99 <30s and no completion >=5m late", p99, lateness[count-1])
	}
	for ctx.Err() == nil {
		info, err := run.Info(ctx)
		if err == nil && info.State.Msgs == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatalf("run queue did not drain: %v", ctx.Err())
	}
	stopWorkers()
	for range workers {
		if err := <-workerDone; err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("worker exited: %v", err)
		}
	}
	report, err := integrity.Check(ctx, all[0])
	if err != nil || report.Invocations != count || report.Journals != count || report.Terminal != count {
		t.Fatalf("fan-in integrity=%+v err=%v", report, err)
	}
	deliveryWindow := completedLast.Sub(earliest)
	t.Logf("timers=%d due_spread=%s max_partition=%d first_lateness=%s p99_lateness=%s max_lateness=%s delivery_window=%s effective_rate=%.1f/s integrity=%+v", count, latest.Sub(earliest), maxPerPartition, completedFirst.Sub(earliest), p99, lateness[count-1], deliveryWindow, float64(count)/deliveryWindow.Seconds(), report)
}
