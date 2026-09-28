package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/wf"
	"js-wf/worker"
)

// Run with WF_TIMER_SLEEP_SCALE=1. The fixed seed gives 10,000 distinct
// invocations a reproducible random sleep of 1–60 seconds. Handler completion
// is measured against each timer's fire_at recorded in its journal.
func TestTenThousandRandomSleeps(t *testing.T) {
	if os.Getenv("WF_TIMER_SLEEP_SCALE") == "" {
		t.Skip("set WF_TIMER_SLEEP_SCALE=1 for the 10,000-sleep timer proof")
	}
	count := timerScaleOption(t, "WF_TIMER_SLEEP_COUNT", 10000, 1, 10000)
	maxSeconds := timerScaleOption(t, "WF_TIMER_SLEEP_MAX_SECONDS", 60, 1, 60)
	partitionConcurrency := timerScaleOption(t, "WF_TIMER_SLEEP_PARTITION_CONCURRENCY", 32, 1, 32)
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	const typ = "sleep-scale"
	type input struct {
		Index   int `json:"index"`
		Seconds int `json:"seconds"`
	}
	completedAt := make([]atomic.Int64, count)
	running := make([]atomic.Int32, count)
	handler := func(c *wf.Context, raw json.RawMessage) (json.RawMessage, error) {
		var job input
		if err := json.Unmarshal(raw, &job); err != nil {
			return nil, err
		}
		if job.Index < 0 || job.Index >= count || job.Seconds < 1 || job.Seconds > maxSeconds {
			return nil, fmt.Errorf("invalid sleep input: %+v", job)
		}
		if !running[job.Index].CompareAndSwap(0, 1) {
			return nil, fmt.Errorf("concurrent handler for invocation %d", job.Index)
		}
		defer running[job.Index].Store(0)
		if err := wf.Sleep(c, "delay", time.Duration(job.Seconds)*time.Second); err != nil {
			return nil, err
		}
		completedAt[job.Index].CompareAndSwap(0, time.Now().UnixNano())
		return json.Marshal(job.Index)
	}
	workerCtx, stopWorkers := context.WithCancel(ctx)
	const workerCount = 6
	workers := make([]*worker.Worker, workerCount)
	workerDone := make(chan error, workerCount)
	startedWorkers := 0
	completedWorkers := 0
	var cleanupOnce sync.Once
	cleanup := func() {
		cleanupOnce.Do(func() {
			stopWorkers()
			for i := completedWorkers; i < startedWorkers; i++ {
				if err := <-workerDone; err != nil && !errors.Is(err, context.Canceled) {
					t.Errorf("worker exited: %v", err)
				}
			}
		})
	}
	defer cleanup()
	for index := range workers {
		w, err := worker.New(ctx, all[index%len(all)], fmt.Sprintf("sleep-scale-%d", index), map[string]worker.Handler{typ: handler}, worker.WithPartitionConcurrency(partitionConcurrency))
		if err != nil {
			t.Fatal(err)
		}
		workers[index] = w
		go func(index int) { workerDone <- w.RunAssigned(workerCtx, index, workerCount) }(index)
		startedWorkers++
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
			completedWorkers++
			t.Fatalf("worker exited during consumer creation: %v", workerErr)
		default:
		}
		if ctx.Err() != nil {
			t.Fatalf("consumers not ready: info=%+v err=%v", info, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	clients := [3]*client.Client{client.New(all[0]), client.New(all[1]), client.New(all[2])}
	rng := rand.New(rand.NewSource(20260928))
	durations := make([]int, count)
	for index := range durations {
		durations[index] = 1 + rng.Intn(maxSeconds)
	}
	startedAt := time.Now()
	jobs := make(chan int, count)
	errorsFound := make(chan error, 1)
	var group sync.WaitGroup
	for caller := 0; caller < 64; caller++ {
		group.Add(1)
		go func(caller int) {
			defer group.Done()
			for index := range jobs {
				id := fmt.Sprintf("sleep-%05d", index)
				payload, _ := json.Marshal(input{Index: index, Seconds: durations[index]})
				if _, err := clients[caller%len(clients)].Start(ctx, typ, id, payload); err != nil {
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
	startDuration := time.Since(startedAt)
	fireAt := make([]time.Time, count)
	results := make(chan int, count)
	for index := 0; index < count; index++ {
		results <- index
	}
	close(results)
	group = sync.WaitGroup{}
	for caller := 0; caller < 64; caller++ {
		group.Add(1)
		go func(caller int) {
			defer group.Done()
			j := journal.New(all[caller%len(all)])
			for index := range results {
				id := fmt.Sprintf("sleep-%05d", index)
				value, err := clients[caller%len(clients)].Await(ctx, typ, id)
				if err != nil || string(value) != strconv.Itoa(index) {
					select {
					case errorsFound <- fmt.Errorf("result %s: %s, %v", id, value, err):
					default:
					}
					return
				}
				records, _, err := j.Read(ctx, typ, id)
				if err != nil || len(records) == 0 || records[len(records)-1].Kind != journal.Completed {
					select {
					case errorsFound <- fmt.Errorf("journal %s: entries=%d, %v", id, len(records), err):
					default:
					}
					return
				}
				for _, record := range records {
					if record.Kind != journal.StepRequested {
						continue
					}
					var request struct {
						Kind   string    `json:"kind"`
						FireAt time.Time `json:"fire_at"`
					}
					if err := json.Unmarshal(record.Payload, &request); err == nil && request.Kind == "timer" {
						fireAt[index] = request.FireAt
					}
				}
				if fireAt[index].IsZero() {
					select {
					case errorsFound <- fmt.Errorf("journal %s lacks fire_at", id):
					default:
					}
					return
				}
			}
		}(caller)
	}
	group.Wait()
	select {
	case err := <-errorsFound:
		t.Fatal(err)
	default:
	}
	for {
		info, err := run.Info(ctx)
		if err == nil && info.State.Msgs == 0 {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("run queue did not drain: info=%+v err=%v", info, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	completionDuration := time.Since(startedAt)
	var early, stuck int
	lateness := make([]time.Duration, count)
	for index, due := range fireAt {
		when := completedAt[index].Load()
		if when == 0 || due.IsZero() {
			t.Fatalf("missing completion or fire time for %d", index)
		}
		completed := time.Unix(0, when)
		if completed.Before(due) {
			early++
		}
		if completed.After(due.Add(5 * time.Minute)) {
			stuck++
		}
		late := completed.Sub(due)
		if late < 0 {
			late = 0
		}
		lateness[index] = late
	}
	sort.Slice(lateness, func(i, j int) bool { return lateness[i] < lateness[j] })
	p50 := lateness[int(math.Ceil(0.50*float64(count)))-1]
	p99 := lateness[int(math.Ceil(0.99*float64(count)))-1]
	maximum := lateness[count-1]
	var scheduled, fired uint64
	var buckets [6]uint64
	for _, w := range workers {
		m := w.Metrics()
		scheduled += m.TimersScheduled
		fired += m.TimersFired
		for i, value := range m.TimerLateBuckets {
			buckets[i] += value
		}
	}
	t.Logf("invocations=%d workers=%d partition_concurrency=%d start=%s complete=%s handler_lateness_p50=%s p99=%s max=%s early=%d stuck=%d wakeup_buckets=%v scheduled=%d fired=%d", count, workerCount, partitionConcurrency, startDuration, completionDuration, p50, p99, maximum, early, stuck, buckets, scheduled, fired)
	if scheduled != uint64(count) || fired != uint64(count) {
		t.Fatalf("timer metrics: scheduled=%d fired=%d want=%d", scheduled, fired, count)
	}
	if p99 >= 2*time.Second {
		t.Errorf("handler completion p99 lateness=%s, want <2s", p99)
	}
	if early != 0 || stuck != 0 {
		t.Errorf("sleep timing: early=%d stuck_after_five_minutes=%d", early, stuck)
	}
	cleanup()
}

func timerScaleOption(t *testing.T, key string, fallback, minimum, maximum int) int {
	t.Helper()
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < minimum || parsed > maximum {
		t.Fatalf("invalid %s=%q", key, value)
	}
	return parsed
}
