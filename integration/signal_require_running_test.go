package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/wf"
	"js-wf/worker"
)

func TestSignalRequireRunningAndLateSignalNoOp(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var calls atomic.Int64
	w, err := worker.New(ctx, all[1], "signal-state-worker", map[string]worker.Handler{"signalstate": func(*wf.Context, json.RawMessage) (json.RawMessage, error) {
		calls.Add(1)
		return json.RawMessage(`1`), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	c := client.New(all[0])
	const typ, id = "signalstate", "terminal"
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- w.RunPartition(workerCtx, identity.Partition(typ, id, provision.Partitions)) }()
	result, err := c.Await(ctx, typ, id)
	if err != nil || string(result) != "1" || calls.Load() != 1 {
		t.Fatalf("initial result=%s calls=%d err=%v", result, calls.Load(), err)
	}
	before, _, err := journal.New(all[2]).Read(ctx, typ, id)
	if err != nil || len(before) == 0 || before[len(before)-1].Kind != journal.Completed {
		t.Fatalf("terminal journal=%+v err=%v", before, err)
	}
	options := client.SignalOptions{RequireRunning: true}
	if seq, err := c.SignalWithOptions(ctx, typ, id, "late", []byte(`"rejected"`), "rejected", options); seq != 0 || !errors.Is(err, client.ErrNotRunning) {
		t.Fatalf("require-running terminal signal: seq=%d err=%v", seq, err)
	}
	if seq, err := c.SignalWithOptions(ctx, typ, "missing", "late", nil, "missing", options); seq != 0 || !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("require-running missing signal: seq=%d err=%v", seq, err)
	}
	accepted, err := c.Signal(ctx, typ, id, "late", []byte(`"accepted"`), "accepted")
	if err != nil || accepted == 0 {
		t.Fatalf("default late signal: seq=%d err=%v", accepted, err)
	}
	run, err := all[2].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	for {
		info, err := run.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if info.State.Msgs == 0 {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("late wakeup was not acknowledged")
		}
		time.Sleep(20 * time.Millisecond)
	}
	after, _, err := journal.New(all[2]).Read(ctx, typ, id)
	if err != nil || len(after) != len(before) || calls.Load() != 1 {
		t.Fatalf("late signal reran terminal workflow: before=%d after=%d calls=%d err=%v", len(before), len(after), calls.Load(), err)
	}
	signals, err := all[2].Stream(ctx, "WF_SIG")
	if err != nil {
		t.Fatal(err)
	}
	info, err := signals.Info(ctx)
	if err != nil || info.State.Msgs != 1 {
		t.Fatalf("accepted late signal count: info=%+v err=%v", info, err)
	}
	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
