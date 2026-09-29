package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

func TestTimerWakeupNackedWhileSignalRunHoldsLease(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const typ, id = "timer-held-lease", "signal-then-timer"
	part := identity.Partition(typ, id, provision.Partitions)
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var effects atomic.Int32
	handler := func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		timer, err := c.Timer("deadline", 5*time.Second)
		if err != nil {
			return nil, err
		}
		if _, err := wf.AwaitSignal(c, "go"); err != nil {
			return nil, err
		}
		if _, err := wf.Run(c, "held", true, func(effectCtx context.Context) (bool, error) {
			if effects.Add(1) != 1 {
				return false, fmt.Errorf("effect executed more than once")
			}
			close(entered)
			select {
			case <-release:
				return true, nil
			case <-effectCtx.Done():
				return false, effectCtx.Err()
			}
		}); err != nil {
			return nil, err
		}
		if err := timer.Await(); err != nil {
			return nil, err
		}
		return json.RawMessage(`true`), nil
	}
	first, err := worker.New(ctx, all[0], "timer-held-first", map[string]worker.Handler{typ: handler})
	if err != nil {
		t.Fatal(err)
	}
	second, err := worker.New(ctx, all[1], "timer-held-second", map[string]worker.Handler{typ: handler})
	if err != nil {
		t.Fatal(err)
	}
	firstCtx, stopFirst := context.WithCancel(ctx)
	defer stopFirst()
	firstDone := make(chan error, 1)
	go func() { firstDone <- first.RunPartition(firstCtx, part) }()
	c := client.New(all[2])
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
				if json.Unmarshal(record.Payload, &request) == nil && request.Kind == "timer_start" {
					fireAt = request.FireAt
				}
			}
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if fireAt.IsZero() || first.Metrics().TimersScheduled != 1 || !time.Now().Before(fireAt) {
		t.Fatalf("timer did not suspend before due time: fire_at=%s metrics=%+v context=%v", fireAt, first.Metrics(), ctx.Err())
	}
	if _, err := c.Signal(ctx, typ, id, "go", []byte(`true`), "held-go"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("signal run did not enter held effect")
	}
	if !time.Now().Before(fireAt) {
		t.Fatalf("held effect began after timer due time %s", fireAt)
	}
	secondCtx, stopSecond := context.WithCancel(ctx)
	defer stopSecond()
	secondDone := make(chan error, 1)
	go func() { secondDone <- second.RunPartition(secondCtx, part) }()
	for ctx.Err() == nil {
		if second.Metrics().LeaseContentions > 0 && second.Metrics().Redeliveries > 0 {
			break
		}
		select {
		case err := <-secondDone:
			t.Fatalf("second worker exited before timer redelivery: %v", err)
		default:
		}
		time.Sleep(20 * time.Millisecond)
	}
	if second.Metrics().LeaseContentions == 0 || second.Metrics().Redeliveries == 0 {
		t.Fatalf("timer was not nacked and redelivered while lease held: metrics=%+v context=%v", second.Metrics(), ctx.Err())
	}
	releaseOnce.Do(func() { close(release) })
	value, err := c.Await(ctx, typ, id)
	if err != nil || string(value) != "true" {
		t.Fatalf("timer result=%s err=%v", value, err)
	}
	run, err := all[2].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	for ctx.Err() == nil {
		info, err := run.Info(ctx)
		if err == nil && info.State.Msgs == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatalf("run queue did not drain: %v", ctx.Err())
	}
	stopFirst()
	stopSecond()
	if err := <-firstDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("first worker: %v", err)
	}
	if err := <-secondDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("second worker: %v", err)
	}
	if effects.Load() != 1 {
		t.Fatalf("effect executions=%d, want one", effects.Load())
	}
	records, _, err := j.Read(ctx, typ, id)
	if err != nil {
		t.Fatal(err)
	}
	var timerAwaits, timerCompletions, terminal int
	for _, record := range records {
		switch record.Kind {
		case journal.StepRequested:
			var request struct {
				Kind string `json:"kind"`
			}
			if json.Unmarshal(record.Payload, &request) == nil && request.Kind == "timer_await" {
				timerAwaits++
			}
		case journal.StepCompleted:
			var completion struct {
				Cancelled bool `json:"cancelled"`
			}
			if timerAwaits > timerCompletions && json.Unmarshal(record.Payload, &completion) == nil && !completion.Cancelled {
				timerCompletions++
			}
		case journal.Completed:
			terminal++
		}
	}
	if timerAwaits != 1 || timerCompletions != 1 || terminal != 1 {
		t.Fatalf("timer journal: awaits=%d completions=%d terminal=%d records=%+v", timerAwaits, timerCompletions, terminal, records)
	}
	if _, err := integrity.Check(ctx, all[2]); err != nil {
		t.Fatal(err)
	}
	t.Logf("timer wakeup nacked under held lease and redelivered; first=%+v second=%+v", first.Metrics(), second.Metrics())
}
