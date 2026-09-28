package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
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

// Run with WF_TIMER_CANCEL_SCALE=1. Four independent partitions cancel 1,000
// timers near their recorded due times, then wait for every late wakeup ack.
func TestThousandTimerCancellationsBeforeFire(t *testing.T) {
	if os.Getenv("WF_TIMER_CANCEL_SCALE") == "" {
		t.Skip("set WF_TIMER_CANCEL_SCALE=1 for the 1,000-timer cancellation proof")
	}
	count := 1000
	if value := os.Getenv("WF_TIMER_CANCEL_COUNT"); value != "" {
		var err error
		count, err = strconv.Atoi(value)
		if err != nil || count < 4 {
			t.Fatalf("invalid WF_TIMER_CANCEL_COUNT=%q", value)
		}
	}
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	const typ = "cancel-scale"
	var cancelledAt sync.Map
	handler := func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
		var id string
		if err := json.Unmarshal(input, &id); err != nil {
			return nil, err
		}
		timer, err := c.Timer("deadline", 650*time.Millisecond)
		if err != nil {
			return nil, err
		}
		choice, _, err := timer.SelectSignal("cancel")
		if err != nil {
			return nil, err
		}
		if choice != wf.SignalSelected {
			return nil, fmt.Errorf("timer won cancellation race: %s", choice)
		}
		if err := timer.Cancel(); err != nil {
			return nil, err
		}
		cancelledAt.Store(id, time.Now())
		if err := timer.Await(); !errors.Is(err, wf.ErrTimerCancelled) {
			return nil, fmt.Errorf("cancelled timer Await: %v", err)
		}
		return json.RawMessage(`true`), nil
	}
	var partitions []uint32
	ids := make([][]string, 4)
	for candidate := 0; candidate < count*64 && totalIDs(ids) < count; candidate++ {
		id := fmt.Sprintf("timer-%06d", candidate)
		partition := identity.Partition(typ, id, provision.Partitions)
		index := -1
		for i, existing := range partitions {
			if existing == partition {
				index = i
				break
			}
		}
		if index < 0 && len(partitions) < len(ids) {
			partitions = append(partitions, partition)
			index = len(partitions) - 1
		}
		if index >= 0 && len(ids[index]) < (count+len(ids)-1-index)/len(ids) {
			ids[index] = append(ids[index], id)
		}
	}
	if totalIDs(ids) != count || len(partitions) != len(ids) {
		t.Fatalf("generated %d/%d IDs on %d partitions", totalIDs(ids), count, len(partitions))
	}
	workerCtx, stopWorkers := context.WithCancel(ctx)
	defer stopWorkers()
	workers := make([]*worker.Worker, len(ids))
	workerDone := make(chan error, len(ids))
	for index, partition := range partitions {
		w, err := worker.New(ctx, all[index%len(all)], fmt.Sprintf("timer-cancel-%d", index), map[string]worker.Handler{typ: handler})
		if err != nil {
			t.Fatal(err)
		}
		workers[index] = w
		go func() { workerDone <- w.RunPartition(workerCtx, partition) }()
	}
	clients := make([]*client.Client, len(ids))
	for index := range clients {
		clients[index] = client.New(all[index%len(all)])
	}
	errorsFound := make(chan error, len(ids))
	var group sync.WaitGroup
	for index := range ids {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			j := journal.New(all[(index+1)%len(all)])
			for _, id := range ids[index] {
				input, _ := json.Marshal(id)
				if _, err := clients[index].Start(ctx, typ, id, input); err != nil {
					errorsFound <- fmt.Errorf("start %s: %w", id, err)
					return
				}
				fireAt, err := waitForCancellationTimer(ctx, j, typ, id)
				if err != nil {
					errorsFound <- fmt.Errorf("timer %s: %w", id, err)
					return
				}
				untilCancel := time.Until(fireAt.Add(-100 * time.Millisecond))
				if untilCancel <= 0 {
					errorsFound <- fmt.Errorf("timer %s had only %s until cancellation", id, time.Until(fireAt))
					return
				}
				select {
				case <-time.After(untilCancel):
				case <-ctx.Done():
					errorsFound <- ctx.Err()
					return
				}
				if _, err := clients[index].Signal(ctx, typ, id, "cancel", []byte(`true`), "cancel-once"); err != nil {
					errorsFound <- fmt.Errorf("signal %s: %w", id, err)
					return
				}
				if !time.Now().Before(fireAt) {
					errorsFound <- fmt.Errorf("signal %s was stored after its deadline", id)
					return
				}
				if value, err := clients[index].Await(ctx, typ, id); err != nil || string(value) != "true" {
					errorsFound <- fmt.Errorf("result %s: %s, %v", id, value, err)
					return
				}
				when, ok := cancelledAt.Load(id)
				if !ok || !when.(time.Time).Before(fireAt) {
					errorsFound <- fmt.Errorf("timer %s was cancelled at %v after deadline %v", id, when, fireAt)
					return
				}
			}
		}(index)
	}
	group.Wait()
	close(errorsFound)
	for err := range errorsFound {
		if err != nil {
			t.Fatal(err)
		}
	}
	for {
		var scheduled, fired, noops uint64
		for _, w := range workers {
			m := w.Metrics()
			scheduled += m.TimersScheduled
			fired += m.TimersFired
			noops += m.CancelledTimerNoOps
		}
		if fired != 0 || scheduled != uint64(count) {
			t.Fatalf("timer metrics: scheduled=%d fired=%d noops=%d want=%d", scheduled, fired, noops, count)
		}
		if noops == uint64(count) {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("only %d/%d cancelled wakeups were acked: %v", noops, count, ctx.Err())
		}
		time.Sleep(20 * time.Millisecond)
	}
	stopWorkers()
	for range workers {
		if err := <-workerDone; err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	}
	if _, err := integrity.Check(ctx, all[0]); err != nil {
		t.Fatalf("integrity: %v", err)
	}
	t.Logf("cancelled=%d scheduled=%d fired=0 late_noops=%d", count, count, count)
}

func totalIDs(groups [][]string) int {
	var total int
	for _, ids := range groups {
		total += len(ids)
	}
	return total
}

func waitForCancellationTimer(ctx context.Context, j *journal.Store, typ, id string) (time.Time, error) {
	for ctx.Err() == nil {
		records, _, err := j.Read(ctx, typ, id)
		if err != nil {
			return time.Time{}, err
		}
		var fireAt time.Time
		for _, record := range records {
			if record.Kind == journal.StepRequested {
				var request struct {
					Kind   string    `json:"kind"`
					FireAt time.Time `json:"fire_at"`
				}
				if err := json.Unmarshal(record.Payload, &request); err != nil {
					return time.Time{}, err
				}
				if request.Kind == "timer_start" {
					fireAt = request.FireAt
				}
			}
			if record.Kind == journal.Suspended && !fireAt.IsZero() {
				return fireAt, nil
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	return time.Time{}, ctx.Err()
}
