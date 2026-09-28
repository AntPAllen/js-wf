package integration_test

import (
	"context"
	"encoding/json"
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

func TestSignalWakeupNackedWhileStartHoldsLease(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	const typ, id = "concurrent-wakeup", "signal-during-start"
	partition := identity.Partition(typ, id, provision.Partitions)
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var calls, active, collisions atomic.Int32
	handler := func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		if active.Add(1) != 1 {
			collisions.Add(1)
			active.Add(-1)
			return nil, fmt.Errorf("concurrent handler execution")
		}
		defer active.Add(-1)
		if calls.Add(1) == 1 {
			close(entered)
			select {
			case <-release:
			case <-c.Context().Done():
				return nil, c.Context().Err()
			}
		}
		value, err := wf.AwaitSignal(c, "go")
		return json.RawMessage(value), err
	}
	first, err := worker.New(ctx, all[0], "wakeup-first", map[string]worker.Handler{typ: handler})
	if err != nil {
		t.Fatal(err)
	}
	second, err := worker.New(ctx, all[1], "wakeup-second", map[string]worker.Handler{typ: handler})
	if err != nil {
		t.Fatal(err)
	}
	firstCtx, stopFirst := context.WithCancel(ctx)
	defer stopFirst()
	firstDone := make(chan error, 1)
	go func() { firstDone <- first.RunPartition(firstCtx, partition) }()
	c := client.New(all[2])
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("first worker did not enter handler")
	}
	secondCtx, stopSecond := context.WithCancel(ctx)
	defer stopSecond()
	secondDone := make(chan error, 1)
	go func() { secondDone <- second.RunPartition(secondCtx, partition) }()
	if _, err := c.Signal(ctx, typ, id, "go", []byte(`42`), "concurrent-go"); err != nil {
		t.Fatal(err)
	}
	run, err := all[2].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := run.Consumer(ctx, fmt.Sprintf("WF_P_%02d", partition))
	if err != nil {
		t.Fatal(err)
	}
	for {
		info, err := consumer.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if info.NumRedelivered > 0 {
			if collisions.Load() != 0 || calls.Load() != 1 {
				t.Fatalf("second wakeup entered held handler: calls=%d collisions=%d", calls.Load(), collisions.Load())
			}
			break
		}
		if ctx.Err() != nil {
			t.Fatal("second wakeup was not nacked and redelivered")
		}
		time.Sleep(20 * time.Millisecond)
	}
	releaseOnce.Do(func() { close(release) })
	value, err := c.Await(ctx, typ, id)
	if err != nil || string(value) != "42" {
		t.Fatalf("result=%s err=%v", value, err)
	}
	for {
		info, err := run.Info(ctx)
		if err == nil && info.State.Msgs == 0 {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("run queue did not drain: info=%+v err=%v", info, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	stopFirst()
	stopSecond()
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	if collisions.Load() != 0 || calls.Load() < 2 {
		t.Fatalf("handler calls=%d collisions=%d", calls.Load(), collisions.Load())
	}
	records, _, err := journal.New(all[2]).Read(ctx, typ, id)
	if err != nil {
		t.Fatal(err)
	}
	var started, suspended, consumed, completed int
	for _, record := range records {
		switch record.Kind {
		case journal.Started:
			started++
		case journal.Suspended:
			suspended++
		case journal.SignalConsumed:
			consumed++
		case journal.Completed:
			completed++
		}
	}
	if started != 1 || suspended != 1 || consumed != 1 || completed != 1 {
		t.Fatalf("journal kinds: started=%d suspended=%d consumed=%d completed=%d", started, suspended, consumed, completed)
	}
	if _, err := integrity.Check(ctx, all[2]); err != nil {
		t.Fatal(err)
	}
}
