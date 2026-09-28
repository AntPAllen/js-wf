package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/reconcile"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestJetStreamScheduledWakeup(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	target := identity.RunSubject("test", "timer-lowlevel", provision.Partitions)
	msg := &nats.Msg{Subject: "wf.schedule.test.timer-lowlevel.0", Data: []byte("test.timer-lowlevel"), Header: nats.Header{}}
	msg.Header.Set(jetstream.ScheduleHeader, "@at "+time.Now().Add(1300*time.Millisecond).UTC().Format(time.RFC3339Nano))
	msg.Header.Set(jetstream.ScheduleTargetHeader, target)
	msg.Header.Set("Wf-Test-Generation", "123")
	if _, err := all[0].PublishMsg(ctx, msg, jetstream.WithMsgID("timer:test:timer-lowlevel:0")); err != nil {
		t.Fatal(err)
	}
	run, err := all[0].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	for {
		stored, err := run.GetLastMsgForSubject(ctx, target)
		if err == nil {
			if string(stored.Data) != "test.timer-lowlevel" {
				t.Fatalf("payload=%q", stored.Data)
			}
			if got := stored.Header.Get("Wf-Test-Generation"); got != "123" {
				t.Fatalf("scheduled target generation header=%q", got)
			}
			return
		}
		if !errors.Is(err, jetstream.ErrMsgNotFound) {
			t.Fatal(err)
		}
		if ctx.Err() != nil {
			t.Fatal("scheduled wakeup did not fire")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestWorkflowSleep(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	w, err := worker.New(ctx, all[1], "timer-worker", map[string]worker.Handler{"test": func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		if err := wf.Sleep(c, "short", 1200*time.Millisecond); err != nil {
			return nil, err
		}
		return json.RawMessage(`true`), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	c := client.New(all[0])
	if _, err := c.Start(ctx, "test", "sleep", []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- w.RunPartition(workerCtx, identity.Partition("test", "sleep", provision.Partitions)) }()
	value, err := c.Await(ctx, "test", "sleep")
	if err != nil || string(value) != "true" {
		t.Fatalf("result=%s err=%v", value, err)
	}
	stop()
	<-done
	m := w.Metrics()
	if m.TimersScheduled != 1 || m.TimersFired != 1 || m.CancelledTimerNoOps != 0 {
		t.Fatalf("sleep timer metrics: %+v", m)
	}
	var lateSamples uint64
	for _, n := range m.TimerLateBuckets {
		lateSamples += n
	}
	if lateSamples != 1 || m.TimerLateMaximum < 0 || m.TimerLateTotal < m.TimerLateMaximum {
		t.Fatalf("sleep lateness metrics: %+v", m)
	}
	entries, _, err := journal.New(all[0]).Read(ctx, "test", "sleep")
	if err != nil {
		t.Fatal(err)
	}
	var suspended, completed int
	for _, e := range entries {
		if e.Kind == journal.Suspended {
			suspended++
		}
		if e.Kind == journal.Completed {
			completed++
		}
	}
	if suspended != 1 || completed != 1 {
		t.Fatalf("suspended=%d completed=%d", suspended, completed)
	}
	if _, err := integrity.Check(ctx, all[0]); err != nil {
		t.Fatalf("integrity: %v", err)
	}
}

func TestNonPositiveTimersCompleteWithoutWakeup(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	const typ, id = "test", "immediate-timers"
	w, err := worker.New(ctx, all[1], "immediate-timer-worker", map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		if err := wf.Sleep(c, "zero-sleep", 0); err != nil {
			return nil, err
		}
		if err := wf.Sleep(c, "negative-sleep", -time.Second); err != nil {
			return nil, err
		}
		for _, item := range []struct {
			name     string
			duration time.Duration
		}{{"zero-timer", 0}, {"negative-timer", -time.Second}} {
			timer, err := c.Timer(item.name, item.duration)
			if err != nil {
				return nil, err
			}
			if err := timer.Await(); err != nil {
				return nil, err
			}
		}
		return json.RawMessage(`true`), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- w.RunPartition(workerCtx, identity.Partition(typ, id, provision.Partitions)) }()
	c := client.New(all[0])
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	value, err := c.Await(ctx, typ, id)
	if err != nil || string(value) != "true" {
		t.Fatalf("result=%s err=%v", value, err)
	}
	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if m := w.Metrics(); m.TimersScheduled != 0 || m.TimersFired != 0 {
		t.Fatalf("immediate timer metrics: %+v", m)
	}
	records, _, err := journal.New(all[2]).Read(ctx, typ, id)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range records {
		if record.Kind == journal.Suspended {
			t.Fatal("immediate timer suspended the workflow")
		}
	}
	run, err := all[0].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	info, err := run.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 0 {
		t.Fatalf("run messages=%d, want no retained wakeups after ack", info.State.Msgs)
	}
}

func TestTimerCancellationBeforeScheduledWakeup(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	const typ, id = "test", "timer-cancel"
	w, err := worker.New(ctx, all[1], "timer-cancel-worker", map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		timer, err := c.Timer("pending", 1800*time.Millisecond)
		if err != nil {
			return nil, err
		}
		if _, err := wf.AwaitSignal(c, "cancel"); err != nil {
			return nil, err
		}
		if err := timer.Cancel(); err != nil {
			return nil, err
		}
		return json.RawMessage(`true`), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	c := client.New(all[0])
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- w.RunPartition(workerCtx, identity.Partition(typ, id, provision.Partitions)) }()
	j := journal.New(all[2])
	for ctx.Err() == nil {
		records, _, err := j.Read(ctx, typ, id)
		if err != nil {
			t.Fatal(err)
		}
		if len(records) > 0 && records[len(records)-1].Kind == journal.Suspended {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("workflow did not suspend")
	}
	if _, err := c.Signal(ctx, typ, id, "cancel", []byte(`true`), "cancel-once"); err != nil {
		t.Fatal(err)
	}
	value, err := c.Await(ctx, typ, id)
	if err != nil || string(value) != "true" {
		t.Fatalf("result=%s err=%v", value, err)
	}
	before, _, err := j.Read(ctx, typ, id)
	if err != nil {
		t.Fatal(err)
	}
	var cancelled bool
	for _, entry := range before {
		if entry.Kind == journal.StepCompleted && string(entry.Payload) == `{"cancelled":true}` {
			cancelled = true
		}
	}
	if !cancelled {
		t.Fatalf("missing timer cancellation in %d journal entries", len(before))
	}
	time.Sleep(2 * time.Second)
	for w.Metrics().CancelledTimerNoOps == 0 && ctx.Err() == nil {
		time.Sleep(20 * time.Millisecond)
	}
	after, _, err := j.Read(ctx, typ, id)
	if err != nil || len(after) != len(before) || after[len(after)-1].Kind != journal.Completed {
		t.Fatalf("late timer wakeup changed journal: before=%d after=%d err=%v", len(before), len(after), err)
	}
	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if m := w.Metrics(); m.TimersScheduled != 1 || m.TimersFired != 0 || m.CancelledTimerNoOps != 1 {
		t.Fatalf("cancelled timer metrics: %+v", m)
	}
}

func TestTimerHandlesCoalesceDueAwaits(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const typ, id = "test", "timer-coalesce"
	w, err := worker.New(ctx, all[1], "timer-coalesce-worker", map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		first, err := c.Timer("first", 1200*time.Millisecond)
		if err != nil {
			return nil, err
		}
		second, err := c.Timer("second", 900*time.Millisecond)
		if err != nil {
			return nil, err
		}
		if err := first.Await(); err != nil {
			return nil, err
		}
		if err := second.Await(); err != nil {
			return nil, err
		}
		return json.RawMessage(`true`), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	c := client.New(all[0])
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- w.RunPartition(workerCtx, identity.Partition(typ, id, provision.Partitions)) }()
	loopCtx, stopLoop := context.WithCancel(ctx)
	defer stopLoop()
	loopDone := make(chan error, 1)
	go func() {
		loopDone <- reconcile.RunTimerLoop(loopCtx, all[2], "timer-coalesce-reconciler", 100*time.Millisecond, 10)
	}()
	value, err := c.Await(ctx, typ, id)
	if err != nil || string(value) != "true" {
		t.Fatalf("result=%s err=%v", value, err)
	}
	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	stopLoop()
	if err := <-loopDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if m := w.Metrics(); m.TimersScheduled != 2 || m.TimersFired != 2 {
		t.Fatalf("coalesced timer metrics: %+v", m)
	}
	records, _, err := journal.New(all[0]).Read(ctx, typ, id)
	if err != nil {
		t.Fatal(err)
	}
	var awaits, completions int
	for _, entry := range records {
		if entry.Kind == journal.StepRequested && bytes.Contains(entry.Payload, []byte(`"timer_await"`)) {
			awaits++
		}
		if entry.Kind == journal.StepCompleted {
			completions++
		}
	}
	if awaits != 2 || completions != 4 {
		t.Fatalf("awaits=%d step completions=%d", awaits, completions)
	}
}

func TestTimerHandleReconcilesCrashBeforeSchedule(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	const typ, id = "test", "timer-handle-repair"
	c := client.New(all[0])
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	run, err := all[0].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	if err := run.Purge(ctx); err != nil {
		t.Fatal(err)
	}
	j := journal.New(all[0])
	seq, err := j.Append(ctx, typ, id, journal.Entry{Index: 0, Epoch: 0, Kind: journal.Started}, 0)
	if err != nil {
		t.Fatal(err)
	}
	request, _ := json.Marshal(struct {
		Kind          string    `json:"kind"`
		Name          string    `json:"name"`
		DurationNanos int64     `json:"duration_nanos"`
		FireAt        time.Time `json:"fire_at"`
	}{Kind: "timer_start", Name: "once", DurationNanos: int64(time.Second), FireAt: time.Now().Add(-time.Second)})
	if _, err := j.Append(ctx, typ, id, journal.Entry{Index: 1, Epoch: 0, Kind: journal.StepRequested, Payload: request}, seq); err != nil {
		t.Fatal(err)
	}
	result, err := reconcile.NewTimerScan(all[2]).Scan(ctx, 0, 10, false)
	if err != nil || result.Reenqueued != 1 {
		t.Fatalf("scan=%+v err=%v", result, err)
	}
	w, err := worker.New(ctx, all[1], "timer-handle-repair-worker", map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		timer, err := c.Timer("once", time.Second)
		if err != nil {
			return nil, err
		}
		if err := timer.Await(); err != nil {
			return nil, err
		}
		return json.RawMessage(`true`), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- w.RunPartition(workerCtx, identity.Partition(typ, id, provision.Partitions)) }()
	value, err := c.Await(ctx, typ, id)
	if err != nil || string(value) != "true" {
		t.Fatalf("result=%s err=%v", value, err)
	}
	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestTimerReconcilerRepairsMissingSchedule(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	c := client.New(all[0])
	if _, err := c.Start(ctx, "test", "missed-timer", []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	run, err := all[0].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	if err := run.Purge(ctx); err != nil {
		t.Fatal(err)
	}
	j := journal.New(all[0])
	seq, err := j.Append(ctx, "test", "missed-timer", journal.Entry{Epoch: 0, Index: 0, Kind: journal.Started}, 0)
	if err != nil {
		t.Fatal(err)
	}
	request, _ := json.Marshal(struct {
		Kind          string    `json:"kind"`
		Name          string    `json:"name"`
		DurationNanos int64     `json:"duration_nanos"`
		FireAt        time.Time `json:"fire_at"`
	}{Kind: "timer", Name: "short", DurationNanos: int64(1200 * time.Millisecond), FireAt: time.Now().Add(-time.Second)})
	seq, err = j.Append(ctx, "test", "missed-timer", journal.Entry{Epoch: 0, Index: 1, Kind: journal.StepRequested, Payload: request}, seq)
	if err != nil {
		t.Fatal(err)
	}
	suspended := json.RawMessage(`{"waiting_on":"timer:short"}`)
	_, err = j.Append(ctx, "test", "missed-timer", journal.Entry{Epoch: 0, Index: 2, Kind: journal.Suspended, Payload: suspended}, seq)
	if err != nil {
		t.Fatal(err)
	}
	result, err := reconcile.NewTimerScan(all[2]).Scan(ctx, 0, 10, false)
	if err != nil || result.Reenqueued != 1 {
		t.Fatalf("scan=%+v err=%v", result, err)
	}
	w, err := worker.New(ctx, all[1], "repair-timer-worker", map[string]worker.Handler{"test": func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		if err := wf.Sleep(c, "short", 1200*time.Millisecond); err != nil {
			return nil, err
		}
		return json.RawMessage(`true`), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() {
		done <- w.RunPartition(workerCtx, identity.Partition("test", "missed-timer", provision.Partitions))
	}()
	value, err := c.Await(ctx, "test", "missed-timer")
	if err != nil || string(value) != "true" {
		t.Fatalf("result=%s err=%v", value, err)
	}
	stop()
	<-done
}
