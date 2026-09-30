package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/reconcile"
	"js-wf/wf"
	"js-wf/worker"
)

func TestPromiseSelectRecoversMissedWakeupAndReplays(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	parent, err := worker.New(ctx, all[1], "parent-worker", map[string]worker.Handler{"parent": func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
		promise, err := wf.CallAsync(c, "child", input)
		if err != nil {
			return nil, err
		}
		timer, err := c.Timer("timeout", 30*time.Second)
		if err != nil {
			return nil, err
		}
		choice, result, err := wf.Select(c, wf.SignalAwaitable("unused"), promise, timer)
		if err != nil {
			return nil, err
		}
		if choice != 1 {
			return nil, fmt.Errorf("unexpected selected case %d", choice)
		}
		if err := timer.Cancel(); err != nil {
			return nil, err
		}
		again, err := wf.AwaitPromise(c, promise)
		if err != nil || string(again) != string(result) {
			return nil, fmt.Errorf("selected promise changed: %s %v", again, err)
		}

		return json.RawMessage(result), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	child, err := worker.New(ctx, all[2], "child-worker", map[string]worker.Handler{"child": func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
		var n int
		if err := json.Unmarshal(input, &n); err != nil {
			return nil, err
		}
		v, err := wf.Run(c, "double", n, func(context.Context) (int, error) { return n * 2, nil })
		if err != nil {
			return nil, err
		}
		return json.Marshal(v)
	}})
	if err != nil {
		t.Fatal(err)
	}
	c := client.New(all[0])
	if _, err := c.Start(ctx, "parent", "root", []byte(`21`)); err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	parentCtx, stopParent := context.WithCancel(workerCtx)
	defer stopParent()
	parentDone := make(chan error, 1)
	go func() {
		parentDone <- parent.RunPartition(parentCtx, identity.Partition("parent", "root", provision.Partitions))
	}()
	j := journal.New(all[0])
	var childID string
	for {
		records, _, err := j.Read(ctx, "parent", "root")
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range records {
			if r.Kind == journal.StepRequested {
				var req struct {
					ChildID string `json:"child_id"`
				}
				if err := json.Unmarshal(r.Payload, &req); err != nil {
					t.Fatal(err)
				}
				if req.ChildID != "" {
					childID = req.ChildID
				}
			}
		}
		if childID != "" && records[len(records)-1].Kind == journal.Suspended {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("parent did not start child and suspend")
		}
		time.Sleep(30 * time.Millisecond)
	}
	stopParent()
	if err := <-parentDone; err != nil {
		t.Fatal(err)
	}
	childCtx, stopChild := context.WithCancel(workerCtx)
	defer stopChild()
	childDone := make(chan error, 1)
	go func() {
		childDone <- child.RunPartition(childCtx, identity.Partition("child", childID, provision.Partitions))
	}()
	if result, err := c.Await(ctx, "child", childID); err != nil || string(result) != "42" {
		t.Fatalf("child completion: %s %v", result, err)
	}
	// Wait for the retained outcome signal, then stop its notifier and remove
	// the corresponding run wakeup so only retained-state repair can resume us.
	signals, err := all[0].Stream(ctx, "WF_SIG")
	if err != nil {
		t.Fatal(err)
	}
	for {
		_, err := signals.GetLastMsgForSubject(ctx, "wf.sig.parent.root.child_0")
		if err == nil {
			break
		}
		if !errors.Is(err, jetstream.ErrMsgNotFound) || ctx.Err() != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond)
	}
	stopChild()
	if err := <-childDone; err != nil {
		t.Fatal(err)
	}
	runs, err := all[0].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	info, err := runs.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for sequence := info.State.FirstSeq; sequence <= info.State.LastSeq && sequence != 0; sequence++ {
		message, err := runs.GetMsg(ctx, sequence)
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if message.Subject == identity.RunSubject("parent", "root", provision.Partitions) && string(message.Data) == "parent.root" {
			if err := runs.DeleteMsg(ctx, sequence); err != nil {
				t.Fatal(err)
			}
		}
	}
	scanner := reconcile.NewSuspendedScan(all[2])
	scanner.Grace = 0
	found, err := scanner.Scan(ctx, 0, 10, true)
	if err != nil {
		t.Fatal(err)
	}
	ready := false
	for _, candidate := range found.Candidates {
		if candidate.Type == "parent" && candidate.ID == "root" && candidate.Reason == "select" {
			ready = true
		}
	}
	if !ready {
		t.Fatalf("missing promise selection repair: %+v", found)
	}
	if _, err := scanner.Scan(ctx, 0, 10, false); err != nil {
		t.Fatal(err)
	}
	parentDone = make(chan error, 1)
	go func() {
		parentDone <- parent.RunPartition(workerCtx, identity.Partition("parent", "root", provision.Partitions))
	}()
	result, err := c.Await(ctx, "parent", "root")
	if err != nil || string(result) != "42" {
		t.Fatalf("result=%s err=%v", result, err)
	}
	stop()
	<-parentDone
	if _, err := integrity.Check(ctx, all[0]); err != nil {
		t.Fatalf("integrity: %v", err)
	}
	childRecords, _, err := j.Read(ctx, "child", childID)
	if err != nil || childRecords[len(childRecords)-1].Kind != journal.Completed {
		t.Fatalf("child journal: %v", err)
	}
}
