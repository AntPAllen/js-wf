package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/retention"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go/jetstream"
)

func TestOldScheduledWakeupCannotRunReusedID(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Second)
	defer cancel()
	const typ, id = "timer", "generation-reuse"
	var freshCalls atomic.Int32
	w, err := worker.New(ctx, all[1], "timer-reuse-worker", map[string]worker.Handler{typ: func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
		if string(input) == `"old"` {
			if _, err := c.Timer("old-timer", 30*time.Second); err != nil {
				return nil, err
			}
			if _, err := wf.AwaitSignal(c, "finish"); err != nil {
				return nil, err
			}
			return json.RawMessage(`true`), nil
		}
		freshCalls.Add(1)
		if _, err := wf.AwaitSignal(c, "go"); err != nil {
			return nil, err
		}
		return json.RawMessage(`true`), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	part := identity.Partition(typ, id, provision.Partitions)
	done := make(chan error, 1)
	go func() { done <- w.RunPartition(workerCtx, part) }()
	c := client.New(all[0])
	old, err := c.Start(ctx, typ, id, []byte(`"old"`))
	if err != nil {
		t.Fatal(err)
	}
	j := journal.New(all[2])
	waitSuspended := func() {
		t.Helper()
		for ctx.Err() == nil {
			records, _, err := j.Read(ctx, typ, id)
			if err != nil {
				t.Fatal(err)
			}
			if len(records) > 0 && records[len(records)-1].Kind == journal.Suspended {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatal("invocation did not suspend")
	}
	waitSuspended()
	oldRecords, _, err := j.Read(ctx, typ, id)
	if err != nil {
		t.Fatal(err)
	}
	var fireAt time.Time
	for _, record := range oldRecords {
		if record.Kind != journal.StepRequested {
			continue
		}
		var request struct {
			Kind   string    `json:"kind"`
			FireAt time.Time `json:"fire_at"`
		}
		if json.Unmarshal(record.Payload, &request) == nil && request.Kind == "timer_start" {
			fireAt = request.FireAt
			break
		}
	}
	if fireAt.IsZero() {
		t.Fatal("old timer request has no fire time")
	}
	if _, err := c.Signal(ctx, typ, id, "finish", []byte(`true`), "finish-old"); err != nil {
		t.Fatal(err)
	}
	if value, err := c.Await(ctx, typ, id); err != nil || string(value) != "true" {
		t.Fatalf("old result=%s err=%v", value, err)
	}
	for ctx.Err() == nil {
		err := retention.Purge(ctx, all[0], typ, id, time.Minute)
		if !errors.Is(err, retention.ErrActive) {
			if err != nil {
				t.Fatal(err)
			}
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("old invocation was not purged")
	}
	fresh, err := c.Start(ctx, typ, id, []byte(`"new"`))
	if err != nil || fresh.InvSeq <= old.InvSeq {
		t.Fatalf("reused start=%+v err=%v", fresh, err)
	}
	waitSuspended()
	if !time.Now().Before(fireAt) {
		t.Fatalf("old timer fired before the new invocation suspended: fireAt=%s", fireAt)
	}
	if freshCalls.Load() != 1 {
		t.Fatalf("fresh handler calls before old timer=%d", freshCalls.Load())
	}
	run, err := all[2].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := run.Consumer(ctx, fmt.Sprintf("WF_P_%02d", part))
	if err != nil {
		t.Fatal(err)
	}
	for ctx.Err() == nil {
		info, err := consumer.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if info.NumAckPending == 0 && info.NumPending == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("new invocation run message was not acked")
	}
	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	target := identity.RunSubject(typ, id, provision.Partitions)
	var staleSeq uint64
	for ctx.Err() == nil {
		message, err := run.GetLastMsgForSubject(ctx, target)
		if err != nil && !errors.Is(err, jetstream.ErrMsgNotFound) {
			t.Fatal(err)
		}
		if err == nil && message.Header.Get("Wf-Timer-Inv-Seq") == strconv.FormatUint(old.InvSeq, 10) {
			staleSeq = message.Sequence
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if staleSeq == 0 {
		t.Fatal("old scheduled wakeup was not delivered")
	}
	secondCtx, stopSecond := context.WithCancel(ctx)
	defer stopSecond()
	secondDone := make(chan error, 1)
	go func() { secondDone <- w.RunPartition(secondCtx, part) }()
	for ctx.Err() == nil {
		info, err := consumer.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if info.AckFloor.Stream >= staleSeq {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("old scheduled wakeup was not acked")
	}
	if freshCalls.Load() != 1 {
		t.Fatalf("old timer reran fresh handler %d times", freshCalls.Load())
	}
	records, _, err := j.Read(ctx, typ, id)
	if err != nil || len(records) == 0 || records[len(records)-1].Kind != journal.Suspended {
		t.Fatalf("fresh journal after old timer=%+v err=%v", records, err)
	}
	if _, err := c.Signal(ctx, typ, id, "go", []byte(`true`), "finish-new"); err != nil {
		t.Fatal(err)
	}
	if value, err := c.Await(ctx, typ, id); err != nil || string(value) != "true" {
		t.Fatalf("fresh result=%s err=%v", value, err)
	}
	stopSecond()
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
}
