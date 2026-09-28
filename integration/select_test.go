package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
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
)

func TestTimerSignalSelectReplaysAcrossWorkerRestart(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	const typ, signalID = "select", "signal"
	partition := identity.Partition(typ, signalID, provision.Partitions)
	timerID := ""
	for i := 0; i < 1000; i++ {
		candidate := fmt.Sprintf("timer-%d", i)
		if identity.Partition(typ, candidate, provision.Partitions) == partition {
			timerID = candidate
			break
		}
	}
	if timerID == "" {
		t.Fatal("could not find second ID on same partition")
	}
	handler := func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		timer, err := c.Timer("deadline", 900*time.Millisecond)
		if err != nil {
			return nil, err
		}
		choice, _, err := timer.SelectSignal("ready")
		if err != nil {
			return nil, err
		}
		if choice == wf.SignalSelected {
			if err := timer.Cancel(); err != nil {
				return nil, err
			}
		}
		return json.Marshal(choice)
	}
	startWorker := func(id string) (context.CancelFunc, <-chan error) {
		w, err := worker.New(ctx, all[1], id, map[string]worker.Handler{typ: handler})
		if err != nil {
			t.Fatal(err)
		}
		workerCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- w.RunPartition(workerCtx, partition) }()
		return stop, done
	}
	stopWorker := func(stop context.CancelFunc, done <-chan error) {
		stop()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	waitSuspended := func(id string) {
		j := journal.New(all[2])
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
		t.Fatalf("%s did not suspend: %v", id, ctx.Err())
	}
	checkCandidate := func(id string) {
		scan := reconcile.NewSuspendedScan(all[2])
		scan.Grace = 0
		result, err := scan.Scan(ctx, 0, 20, true)
		if err != nil {
			t.Fatal(err)
		}
		for _, candidate := range result.Candidates {
			if candidate.ID == id && candidate.Reason == "select" {
				return
			}
		}
		t.Fatalf("missing select repair candidate for %s: %+v", id, result.Candidates)
	}
	c := client.New(all[0])
	stop, done := startWorker("select-first")
	if _, err := c.Start(ctx, typ, signalID, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	waitSuspended(signalID)
	stopWorker(stop, done)
	if _, err := c.Signal(ctx, typ, signalID, "ready", []byte(`true`), "select-ready"); err != nil {
		t.Fatal(err)
	}
	checkCandidate(signalID)
	stop, done = startWorker("select-second")
	value, err := c.Await(ctx, typ, signalID)
	if err != nil || string(value) != `"signal"` {
		t.Fatalf("signal selection: value=%s err=%v", value, err)
	}
	if _, err := c.Start(ctx, typ, timerID, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	waitSuspended(timerID)
	stopWorker(stop, done)
	time.Sleep(1100 * time.Millisecond)
	checkCandidate(timerID)
	stop, done = startWorker("select-third")
	value, err = c.Await(ctx, typ, timerID)
	if err != nil || string(value) != `"timer"` {
		t.Fatalf("timer selection: value=%s err=%v", value, err)
	}
	stopWorker(stop, done)
	if _, err := integrity.Check(ctx, all[0]); err != nil {
		t.Fatalf("select journal integrity: %v", err)
	}
}
