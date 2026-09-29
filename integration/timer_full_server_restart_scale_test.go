//go:build linux

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
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/testcluster"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// This proof uses real server processes and native scheduled messages. Every
// timer must be durably suspended before all three processes are killed.
func TestThousandTimersSurviveFullServerRestart(t *testing.T) {
	if os.Getenv("WF_FULL_RESTART_TIMERS") != "1" {
		t.Skip("set WF_FULL_RESTART_TIMERS=1 for the 1,000-timer process restart proof")
	}
	cluster, err := testcluster.StartPartitionableProcesses(t.TempDir(), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	js := make([]jetstream.JetStream, 3)
	for i, conn := range cluster.Clients {
		js[i], err = jetstream.New(conn)
		if err != nil {
			t.Fatal(err)
		}
	}
	until := time.Now().Add(30 * time.Second)
	for time.Now().Before(until) {
		attempt, stop := context.WithTimeout(ctx, 4*time.Second)
		err = provision.Ensure(attempt, js[0], 3)
		stop()
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("provision: %v", err)
	}
	const typ, count = "restarttimers", 1000
	const delay = 150 * time.Second
	completedAt := make([]atomic.Int64, count)
	handler := func(c *wf.Context, raw json.RawMessage) (json.RawMessage, error) {
		var index int
		if err := json.Unmarshal(raw, &index); err != nil {
			return nil, err
		}
		if index < 0 || index >= count {
			return nil, fmt.Errorf("timer index %d out of range", index)
		}
		if err := wf.Sleep(c, "after-restart", delay); err != nil {
			return nil, err
		}
		completedAt[index].CompareAndSwap(0, time.Now().UnixNano())
		return json.Marshal(index)
	}
	first, err := worker.New(ctx, js[1], "timers-before-restart", map[string]worker.Handler{typ: handler}, worker.WithPartitionConcurrency(32))
	if err != nil {
		t.Fatal(err)
	}
	firstCtx, stopFirst := context.WithCancel(ctx)
	firstDone := make(chan error, 1)
	go func() { firstDone <- first.RunAssigned(firstCtx, 0, 1) }()
	clients := [3]*client.Client{client.New(js[0]), client.New(js[1]), client.New(js[2])}
	jobs := make(chan int, count)
	startErrors := make(chan error, 1)
	var starters sync.WaitGroup
	for caller := 0; caller < 32; caller++ {
		starters.Add(1)
		go func(caller int) {
			defer starters.Done()
			for index := range jobs {
				id := fmt.Sprintf("timer-%04d", index)
				if _, err := clients[caller%len(clients)].Start(ctx, typ, id, []byte(fmt.Sprint(index))); err != nil {
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
	run, err := js[0].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	until = time.Now().Add(60 * time.Second)
	for time.Now().Before(until) && ctx.Err() == nil {
		info, infoErr := run.Info(ctx)
		if infoErr == nil && first.Metrics().TimersScheduled == count && info.State.Msgs == count {
			break
		}
		select {
		case err := <-firstDone:
			t.Fatalf("first worker exited before suspension: %v", err)
		default:
		}
		time.Sleep(50 * time.Millisecond)
	}
	if first.Metrics().TimersScheduled != count {
		t.Fatalf("scheduled %d/%d timers before restart (context=%v)", first.Metrics().TimersScheduled, count, ctx.Err())
	}
	stopFirst()
	if err := <-firstDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("first worker: %v", err)
	}
	info, err := run.Info(ctx)
	if err != nil || info.State.Msgs != count {
		t.Fatalf("initial run deliveries not replaced by %d scheduled sources: info=%+v err=%v", count, info, err)
	}
	fireAt := make([]time.Time, count)
	j := journal.New(js[2])
	for index := 0; index < count; index++ {
		id := fmt.Sprintf("timer-%04d", index)
		records, _, err := j.Read(ctx, typ, id)
		if err != nil || len(records) != 3 || records[0].Kind != journal.Started || records[1].Kind != journal.StepRequested || records[2].Kind != journal.Suspended {
			t.Fatalf("timer %s not durably suspended: records=%+v err=%v", id, records, err)
		}
		var request struct {
			Kind   string    `json:"kind"`
			FireAt time.Time `json:"fire_at"`
		}
		if err := json.Unmarshal(records[1].Payload, &request); err != nil || request.Kind != "timer" || request.FireAt.IsZero() {
			t.Fatalf("timer %s request=%s err=%v", id, records[1].Payload, err)
		}
		fireAt[index] = request.FireAt
		if !time.Now().Before(request.FireAt) {
			t.Fatalf("timer %s due before restart: %s", id, request.FireAt)
		}
	}
	for index, due := range fireAt {
		if !time.Now().Before(due) {
			t.Fatalf("timer %d due before process kill: %s", index, due)
		}
	}
	for i := range cluster.Commands {
		if err := cluster.KillNode(i); err != nil {
			t.Fatalf("kill node %d: %v", i, err)
		}
	}
	for i := range cluster.Commands {
		if err := cluster.RestartNode(i); err != nil {
			t.Fatalf("restart node %d: %v", i, err)
		}
	}
	for i, conn := range cluster.Clients {
		js[i], err = jetstream.New(conn)
		if err != nil {
			t.Fatal(err)
		}
	}
	until = time.Now().Add(30 * time.Second)
	var retained uint64
	for time.Now().Before(until) && ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, 3*time.Second)
		inv, lookupErr := js[2].Stream(attempt, "WF_INV")
		if lookupErr == nil {
			var invInfo *jetstream.StreamInfo
			invInfo, lookupErr = inv.Info(attempt)
			if lookupErr == nil && invInfo.Cluster != nil && invInfo.Cluster.Leader != "" && len(invInfo.Cluster.Replicas) == 2 {
				retained = invInfo.State.Msgs
			}
		}
		stop()
		if retained == count {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if retained != count {
		t.Fatalf("retained invocations after restart=%d, want %d (context=%v)", retained, count, ctx.Err())
	}
	run, err = js[0].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < count; index++ {
		id := fmt.Sprintf("timer-%04d", index)
		source := fmt.Sprintf("wf.schedule.%s.%s.0", typ, id)
		stored, err := run.GetLastMsgForSubject(ctx, source)
		if err != nil {
			t.Fatalf("timer %s schedule lost across restart: %v", id, err)
		}
		if got := stored.Header.Get(jetstream.ScheduleHeader); got != "@at "+fireAt[index].UTC().Format(time.RFC3339Nano) {
			t.Fatalf("timer %s schedule header=%q, fire_at=%s", id, got, fireAt[index])
		}
	}
	var successor *worker.Worker
	until = time.Now().Add(30 * time.Second)
	for time.Now().Before(until) && ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, 5*time.Second)
		successor, err = worker.New(attempt, js[2], "timers-after-restart", map[string]worker.Handler{typ: handler}, worker.WithPartitionConcurrency(32))
		stop()
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("successor worker: %v (context=%v)", err, ctx.Err())
	}
	workerCtx, stopWorker := context.WithCancel(ctx)
	workerDone := make(chan error, 1)
	go func() { workerDone <- successor.RunAssigned(workerCtx, 0, 1) }()
	resultJobs := make(chan int, count)
	resultErrors := make(chan error, 1)
	var readers sync.WaitGroup
	for reader := 0; reader < 32; reader++ {
		readers.Add(1)
		go func(reader int) {
			defer readers.Done()
			c := client.New(js[reader%len(js)])
			for index := range resultJobs {
				id := fmt.Sprintf("timer-%04d", index)
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
	for index := 0; index < count; index++ {
		completed := time.Unix(0, completedAt[index].Load())
		if completedAt[index].Load() == 0 || completed.Before(fireAt[index]) {
			t.Fatalf("timer %d completed at %s before fire_at %s", index, completed, fireAt[index])
		}
	}
	until = time.Now().Add(30 * time.Second)
	var pending uint64
	for time.Now().Before(until) && ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, 3*time.Second)
		info, infoErr := run.Info(attempt)
		stop()
		if infoErr == nil {
			pending = info.State.Msgs
			if pending == 0 {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if pending != 0 {
		t.Fatalf("run queue retained %d messages (context=%v)", pending, ctx.Err())
	}
	stopWorker()
	if err := <-workerDone; err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, nats.ErrConnectionClosed) {
		t.Fatalf("successor worker: %v", err)
	}
	report, err := integrity.Check(ctx, js[1])
	if err != nil || report.Invocations != count || report.Journals != count || report.Entries != 5*count || report.Terminal != count {
		t.Fatalf("full restart timer integrity=%+v err=%v", report, err)
	}
	t.Logf("%d native timers survived full process restart; schedules, results, due times, and journals verified; integrity=%+v", count, report)
}
