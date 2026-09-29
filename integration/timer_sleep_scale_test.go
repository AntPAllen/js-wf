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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/reconcile"
	"js-wf/testcluster"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Run with WF_TIMER_SLEEP_SCALE=1. The fixed seed gives 10,000 distinct
// invocations a reproducible random sleep of 1–60 seconds. Handler completion
// is measured against each timer's fire_at recorded in its journal.
func TestTenThousandRandomSleeps(t *testing.T) {
	if os.Getenv("WF_TIMER_SLEEP_SCALE") == "" {
		t.Skip("set WF_TIMER_SLEEP_SCALE=1 for the 10,000-sleep timer proof")
	}
	runTenThousandRandomSleeps(t, false)
}

func TestTenThousandRandomSleepsDuringRouteFaults(t *testing.T) {
	if os.Getenv("WF_TIMER_SLEEP_CHAOS") == "" {
		t.Skip("set WF_TIMER_SLEEP_CHAOS=1 for the 10,000-sleep route-fault proof")
	}
	runTenThousandRandomSleeps(t, true)
}

func runTenThousandRandomSleeps(t *testing.T, routeFaults bool) {
	t.Helper()
	count := timerScaleOption(t, "WF_TIMER_SLEEP_COUNT", 10000, 1, 10000)
	maxSeconds := timerScaleOption(t, "WF_TIMER_SLEEP_MAX_SECONDS", 60, 1, 60)
	partitionConcurrency := timerScaleOption(t, "WF_TIMER_SLEEP_PARTITION_CONCURRENCY", 32, 1, 32)
	var all []jetstream.JetStream
	var cluster *testcluster.Cluster
	if routeFaults {
		var err error
		cluster, err = testcluster.StartPartitionable(t.TempDir(), 3)
		if err != nil {
			t.Fatal(err)
		}
		all, cluster = setupCluster(t, cluster)
	} else {
		all, _ = setup(t)
	}
	timeoutSeconds := timerScaleOption(t, "WF_TIMER_SLEEP_TIMEOUT_SECONDS", 600, 30, 600)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSeconds)*time.Second)
	defer cancel()
	if routeFaults && os.Getenv("WF_TIMER_SLEEP_SKIP_RECONCILER") == "" {
		loopCtx, stopLoop := context.WithCancel(ctx)
		loopDone := make(chan error, 1)
		go func() {
			loopDone <- reconcile.RunTimerLoop(loopCtx, all[0], "sleep-scale-reconciler", time.Second, 500)
		}()
		defer func() {
			stopLoop()
			if err := <-loopDone; err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("timer reconciler: %v", err)
			}
		}()
	}
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
		options := []worker.Option{worker.WithPartitionConcurrency(partitionConcurrency)}
		if routeFaults {
			options = append(options, worker.WithDispatchTiming(10*time.Second, 2*time.Second))
		}
		w, err := worker.New(ctx, all[index%len(all)], fmt.Sprintf("sleep-scale-%d", index), map[string]worker.Handler{typ: handler}, options...)
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
	var faultDone chan timerRouteFaultResult
	var healedAt time.Time
	if routeFaults {
		for ctx.Err() == nil {
			var scheduled, fired uint64
			for _, w := range workers {
				m := w.Metrics()
				scheduled += m.TimersScheduled
				fired += m.TimersFired
			}
			if scheduled == uint64(count) {
				if scheduled-fired < uint64(count/2) {
					t.Fatalf("only %d/%d timers remained active before route faults", scheduled-fired, count)
				}
				t.Logf("route faults starting with %d scheduled and %d fired timers", scheduled, fired)
				break
			}
			select {
			case workerErr := <-workerDone:
				completedWorkers++
				t.Fatalf("worker exited before route faults: %v", workerErr)
			default:
			}
			time.Sleep(20 * time.Millisecond)
		}
		if ctx.Err() != nil {
			t.Fatalf("timers were not all scheduled before route faults: %v", ctx.Err())
		}
		// Scheduling is counted before the initial handler releases its lease.
		// Let those handlers finish so the route cut exercises suspended sleeps.
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		faultDone = make(chan timerRouteFaultResult, 1)
		go func() { faultDone <- runTimerRouteFaults(ctx, cluster, all[0], typ, handler) }()
	}
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
			resultNode := caller % len(all)
			if routeFaults {
				resultNode = 0 // Keep result readers on the majority during both route cuts.
			}
			j := journal.New(all[resultNode])
			for index := range results {
				id := fmt.Sprintf("sleep-%05d", index)
				value, err := clients[resultNode].Await(ctx, typ, id)
				if err != nil || string(value) != strconv.Itoa(index) {
					select {
					case errorsFound <- fmt.Errorf("result %s: %s, %v", id, value, err):
					default:
					}
					return
				}
				records, err := readSleepJournal(ctx, j, typ, id)
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
	resultsDone := make(chan struct{})
	go func() {
		group.Wait()
		close(resultsDone)
	}()
	progress := time.NewTicker(10 * time.Second)
	defer progress.Stop()
	for waiting := true; waiting; {
		select {
		case <-resultsDone:
			waiting = false
		case workerErr := <-workerDone:
			completedWorkers++
			cancel()
			t.Fatalf("worker exited during timer workload: %v", workerErr)
		case <-progress.C:
			var completed int
			for index := range completedAt {
				if completedAt[index].Load() != 0 {
					completed++
				}
			}
			t.Logf("sleep progress: completed_handlers=%d/%d", completed, count)
		}
	}
	select {
	case err := <-errorsFound:
		if routeFaults {
			fields := strings.Fields(err.Error())
			failedID := ""
			if len(fields) > 1 {
				failedID = strings.TrimSuffix(fields[1], ":")
			}
			diagnoseSleepFault(t, all, typ, failedID, completedAt)
		}
		t.Fatal(err)
	default:
	}
	if faultDone != nil {
		fault := <-faultDone
		if fault.err != nil {
			t.Fatalf("route faults: %v", fault.err)
		}
		healedAt = fault.healedAt
		defer func() {
			fault.stop()
			for _, done := range fault.done {
				if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
					t.Errorf("standby worker: %v", err)
				}
			}
		}()
		workers = append(workers, fault.workers...)
		t.Logf("route fault sequence elapsed=%s", fault.elapsed)
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
	var recoveryLateness []time.Duration
	if routeFaults {
		recoveryLateness = make([]time.Duration, count)
	}
	var nodeLateness [3][]time.Duration
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
		if routeFaults {
			recoveryLateness[index] = sleepRecoveryLateness(completed, due, healedAt)
			partition := identity.Partition(typ, fmt.Sprintf("sleep-%05d", index), provision.Partitions)
			ownerNode := int(partition%workerCount) % len(nodeLateness)
			nodeLateness[ownerNode] = append(nodeLateness[ownerNode], late)
		}
	}
	sort.Slice(lateness, func(i, j int) bool { return lateness[i] < lateness[j] })
	p50 := lateness[int(math.Ceil(0.50*float64(count)))-1]
	p99 := lateness[int(math.Ceil(0.99*float64(count)))-1]
	maximum := lateness[count-1]
	var recoveryP99, recoveryMaximum time.Duration
	if routeFaults {
		sort.Slice(recoveryLateness, func(i, j int) bool { return recoveryLateness[i] < recoveryLateness[j] })
		recoveryP99 = recoveryLateness[int(math.Ceil(0.99*float64(count)))-1]
		recoveryMaximum = recoveryLateness[count-1]
	}
	var scheduled, fired, redeliveries, fences, contentions, acquireFailures uint64
	var enqueueMax time.Duration
	var buckets [6]uint64
	for _, w := range workers {
		m := w.Metrics()
		if routeFaults {
			t.Logf("worker=%s acquisitions=%d contentions=%d acquire_failures=%d redeliveries=%d fences=%d enqueue_to_lease_max=%s", w.ID, m.LeaseAcquisitions, m.LeaseContentions, m.LeaseAcquireFailures, m.Redeliveries, m.FencingEvents, m.EnqueueToLeaseMaximum)
		}
		scheduled += m.TimersScheduled
		fired += m.TimersFired
		redeliveries += m.Redeliveries
		fences += m.FencingEvents
		contentions += m.LeaseContentions
		acquireFailures += m.LeaseAcquireFailures
		if m.EnqueueToLeaseMaximum > enqueueMax {
			enqueueMax = m.EnqueueToLeaseMaximum
		}
		for i, value := range m.TimerLateBuckets {
			buckets[i] += value
		}
	}
	if routeFaults {
		for node, values := range nodeLateness {
			if len(values) == 0 {
				continue
			}
			sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
			late := len(values) - sort.Search(len(values), func(i int) bool { return values[i] > 30*time.Second })
			t.Logf("owner_node=%d invocations=%d p99_lateness=%s over_30s=%d", node, len(values), values[int(math.Ceil(0.99*float64(len(values))))-1], late)
		}
	}
	t.Logf("invocations=%d workers=%d partition_concurrency=%d start=%s complete=%s handler_lateness_p50=%s p99=%s max=%s early=%d stuck=%d wakeup_buckets=%v scheduled=%d fired=%d redeliveries=%d fences=%d lease_contentions=%d lease_acquire_failures=%d enqueue_to_lease_max=%s", count, len(workers), partitionConcurrency, startDuration, completionDuration, p50, p99, maximum, early, stuck, buckets, scheduled, fired, redeliveries, fences, contentions, acquireFailures, enqueueMax)
	if routeFaults {
		t.Logf("route_recovery_healed_at=%s p99=%s max=%s (measured from later of fire_at and final heal)", healedAt.Format(time.RFC3339Nano), recoveryP99, recoveryMaximum)
	}
	if scheduled != uint64(count) || fired > uint64(count) || !routeFaults && fired != uint64(count) {
		t.Fatalf("timer metrics: scheduled=%d fired=%d want=%d", scheduled, fired, count)
	}
	if routeFaults {
		if recoveryP99 >= 30*time.Second {
			t.Errorf("handler completion p99 after final route heal=%s, want <30s (raw fire_at p99=%s)", recoveryP99, p99)
		}
	} else if p99 >= 2*time.Second {
		t.Errorf("handler completion p99 lateness=%s, want <2s", p99)
	}
	if early != 0 || stuck != 0 {
		t.Errorf("sleep timing: early=%d stuck_after_five_minutes=%d", early, stuck)
	}
	cleanup()
}

func readSleepJournal(ctx context.Context, j *journal.Store, typ, id string) ([]journal.Record, error) {
	for ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, 5*time.Second)
		records, _, err := j.Read(attempt, typ, id)
		stop()
		if err == nil {
			return records, nil
		}
		var api *jetstream.APIError
		if !errors.As(err, &api) || api.ErrorCode != 10008 {
			if !errors.Is(err, jetstream.ErrNoStreamResponse) && !errors.Is(err, nats.ErrTimeout) && !errors.Is(err, nats.ErrNoResponders) && !errors.Is(err, nats.ErrDisconnected) && !errors.Is(err, nats.ErrConnectionReconnecting) && !errors.Is(err, context.DeadlineExceeded) {
				return nil, err
			}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return nil, ctx.Err()
}

type timerRouteFaultResult struct {
	elapsed  time.Duration
	healedAt time.Time
	workers  []*worker.Worker
	stop     context.CancelFunc
	done     []<-chan error
	err      error
}

func runTimerRouteFaults(ctx context.Context, cluster *testcluster.Cluster, majority jetstream.JetStream, typ string, handler worker.Handler) (result timerRouteFaultResult) {
	started := time.Now()
	defer func() { result.elapsed = time.Since(started) }()
	mesh := cluster.RouteMesh()
	defer mesh.Heal()
	backupCtx, stopBackups := context.WithCancel(ctx)
	result.stop = stopBackups
	defer func() {
		if result.err != nil {
			stopBackups()
			for _, done := range result.done {
				<-done
			}
		}
	}()
	standbys := map[int]*worker.Worker{}
	standbyPartitions := map[int][]uint32{}
	for _, node := range []int{2, 1} {
		for owner := 0; owner < 6; owner++ {
			if owner%3 != node {
				continue
			}
			assigned, err := worker.StaticPartitions(owner, 6)
			if err != nil {
				result.err = err
				return
			}
			standbyPartitions[node] = append(standbyPartitions[node], assigned...)
		}
		attempt, stop := context.WithTimeout(ctx, 5*time.Second)
		backup, err := worker.New(attempt, majority, fmt.Sprintf("sleep-standby-%d", node), map[string]worker.Handler{typ: handler}, worker.WithPartitionConcurrency(32), worker.WithDispatchTiming(10*time.Second, 2*time.Second))
		stop()
		if err != nil {
			result.err = err
			return
		}
		standbys[node] = backup
	}
	startedStandbys := map[int]bool{}
	for _, node := range []int{2, 1, 2} {
		if err := mesh.PartitionNode(node); err != nil {
			result.err = err
			return
		}
		if err := waitTimerRouteCounts(ctx, cluster, node); err != nil {
			result.err = err
			return
		}
		select {
		case <-time.After(2 * time.Second):
		case <-ctx.Done():
			result.err = ctx.Err()
			return
		}
		if !startedStandbys[node] {
			backup := standbys[node]
			partitions := standbyPartitions[node]
			result.workers = append(result.workers, backup)
			done := make(chan error, 1)
			result.done = append(result.done, done)
			go func() { done <- backup.RunPartitions(backupCtx, partitions) }()
			startedStandbys[node] = true
		}
		select {
		case <-time.After(6 * time.Second):
		case <-ctx.Done():
			result.err = ctx.Err()
			return
		}
		mesh.Heal()
		if err := waitTimerRouteCounts(ctx, cluster, -1); err != nil {
			result.err = err
			return
		}
	}
	result.healedAt = time.Now()
	return
}

func sleepRecoveryLateness(completed, due, healedAt time.Time) time.Duration {
	start := due
	if healedAt.After(start) {
		start = healedAt
	}
	if completed.Before(start) {
		return 0
	}
	return completed.Sub(start)
}

func waitTimerRouteCounts(ctx context.Context, cluster *testcluster.Cluster, isolated int) error {
	until := time.Now().Add(5 * time.Second)
	for ctx.Err() == nil && time.Now().Before(until) {
		ready := true
		for index, server := range cluster.Servers {
			want := 2
			if index == isolated {
				want = 0
			} else if isolated >= 0 {
				want = 1
			}
			if server.NumRoutes() != want {
				ready = false
			}
		}
		if ready {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("route counts after isolating %d: %d/%d/%d (ctx=%v)", isolated,
		cluster.Servers[0].NumRoutes(), cluster.Servers[1].NumRoutes(), cluster.Servers[2].NumRoutes(), ctx.Err())
}

func diagnoseSleepFault(t *testing.T, all []jetstream.JetStream, typ, failedID string, completedAt []atomic.Int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	j := journal.New(all[0])
	state, stateErr := all[0].KeyValue(ctx, "WF_STATE")
	inspect := func(id string) {
		records, _, journalErr := j.Read(ctx, typ, id)
		var kinds []journal.Kind
		for _, record := range records {
			kinds = append(kinds, record.Kind)
		}
		var resultErr error
		var resultValue []byte
		if stateErr == nil {
			entry, err := state.Get(ctx, identity.Key(typ, id))
			resultErr = err
			if err == nil {
				resultValue = entry.Value()
			}
		}
		t.Logf("inspect %s: journal=%v journal_err=%v result=%s result_err=%v", id, kinds, journalErr, resultValue, resultErr)
		if run, err := all[0].Stream(ctx, "WF_RUN"); err == nil {
			partition := identity.Partition(typ, id, provision.Partitions)
			name := fmt.Sprintf("WF_P_%02d", partition)
			if consumer, err := run.Consumer(ctx, name); err == nil {
				if info, err := consumer.Info(ctx); err == nil {
					t.Logf("consumer %s: pending=%d ack_pending=%d waiting=%d delivered=%d leader=%v", name, info.NumPending, info.NumAckPending, info.NumWaiting, info.Delivered.Stream, info.Cluster)
				}
			}
		}
	}
	if failedID != "" {
		inspect(failedID)
	}
	logged := 0
	for index := range completedAt {
		if completedAt[index].Load() != 0 {
			continue
		}
		id := fmt.Sprintf("sleep-%05d", index)
		if id == failedID {
			continue
		}
		inspect(id)
		logged++
		if logged >= 8 || ctx.Err() != nil {
			break
		}
	}
	run, err := all[0].Stream(ctx, "WF_RUN")
	if err == nil {
		info, infoErr := run.Info(ctx)
		if infoErr == nil {
			t.Logf("WF_RUN pending messages=%d consumers=%d", info.State.Msgs, info.State.Consumers)
		}
	}
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
