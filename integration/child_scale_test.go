package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"sync"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/testcluster"
	"js-wf/wf"
	"js-wf/worker"
)

// TestFiveHundredChildFanout interrupts the parent during child creation,
// then collects all child results after a restart.
func TestFiveHundredChildFanout(t *testing.T) {
	seed, err := testcluster.SeedFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	const childCount = 500
	cut := 100 + rand.New(rand.NewSource(seed)).Intn(300)
	t.Logf("FAULT_SEED=%d parent_cut_after_child=%d", seed, cut)
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	reached := make(chan struct{})
	release := make(chan struct{})
	var pauseOnce sync.Once
	handlers := map[string]worker.Handler{
		"parent": func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
			promises := make([]wf.Promise, childCount)
			for i := range promises {
				input, _ := json.Marshal(i)
				promise, err := wf.CallAsync(c, "child", input)
				if err != nil {
					return nil, err
				}
				promises[i] = promise
				if i == cut {
					pauseOnce.Do(func() {
						close(reached)
						<-release
					})
				}
			}
			sum := 0
			for _, promise := range promises {
				value, err := wf.AwaitPromise(c, promise)
				if err != nil {
					return nil, err
				}
				var n int
				if err := json.Unmarshal(value, &n); err != nil {
					return nil, err
				}
				sum += n
			}
			return json.Marshal(sum)
		},
		"child": func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
			var n int
			if err := json.Unmarshal(input, &n); err != nil {
				return nil, err
			}
			value, err := wf.Run(c, "double", n, func(context.Context) (int, error) { return n * 2, nil })
			if err != nil {
				return nil, err
			}
			return json.Marshal(value)
		},
	}
	if _, err := client.New(all[0]).Start(ctx, "parent", "large-fanout", []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	parentPart := identity.Partition("parent", "large-fanout", provision.Partitions)
	first, err := worker.New(ctx, all[1], "parent-before-cut", handlers)
	if err != nil {
		t.Fatal(err)
	}
	firstCtx, stopFirst := context.WithCancel(ctx)
	firstDone := make(chan error, 1)
	go func() { firstDone <- first.RunPartition(firstCtx, parentPart) }()
	select {
	case <-reached:
	case <-ctx.Done():
		t.Fatal("parent did not reach injected cut")
	}
	stopFirst()
	close(release)
	if err := <-firstDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("first parent worker: %v", err)
	}
	second, err := worker.New(ctx, all[1], "parent-after-cut", handlers)
	if err != nil {
		t.Fatal(err)
	}
	secondCtx, stopSecond := context.WithCancel(ctx)
	secondDone := make(chan error, 1)
	go func() { secondDone <- second.RunPartition(secondCtx, parentPart) }()
	j := journal.New(all[0])
	var childIDs []string
	for ctx.Err() == nil {
		records, _, err := j.Read(ctx, "parent", "large-fanout")
		if err != nil {
			t.Fatal(err)
		}
		childIDs = childIDs[:0]
		for _, record := range records {
			if record.Kind != journal.StepRequested {
				continue
			}
			var request struct {
				Kind    string `json:"kind"`
				ChildID string `json:"child_id"`
			}
			if err := json.Unmarshal(record.Payload, &request); err != nil {
				t.Fatal(err)
			}
			if request.Kind == "call_async" {
				childIDs = append(childIDs, request.ChildID)
			}
		}
		if len(childIDs) == childCount && len(records) > 0 && records[len(records)-1].Kind == journal.Suspended {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if len(childIDs) != childCount {
		t.Fatalf("parent started %d children before timeout: %v", len(childIDs), ctx.Err())
	}
	stopSecond()
	if err := <-secondDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("second parent worker: %v", err)
	}
	seenIDs := map[string]bool{}
	parts := map[uint32]bool{}
	insideParent := 0
	for _, id := range childIDs {
		if id == "" || seenIDs[id] {
			t.Fatalf("duplicate or empty child id %q", id)
		}
		seenIDs[id] = true
		part := identity.Partition("child", id, provision.Partitions)
		if part == parentPart {
			insideParent++
		} else {
			parts[part] = true
		}
	}
	childWorker, err := worker.New(ctx, all[2], "child-fanout-worker", handlers)
	if err != nil {
		t.Fatal(err)
	}
	childCtx, stopChildren := context.WithCancel(ctx)
	defer stopChildren()
	childDone := make([]chan error, 0, len(parts))
	for part := range parts {
		done := make(chan error, 1)
		childDone = append(childDone, done)
		go func(part uint32, done chan error) { done <- childWorker.RunPartition(childCtx, part) }(part, done)
	}
	sig, err := all[0].Stream(ctx, "WF_SIG")
	if err != nil {
		t.Fatal(err)
	}
	for ctx.Err() == nil {
		info, err := sig.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if info.State.Msgs >= uint64(childCount-insideParent) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatalf("children outside parent partition did not finish: %v", ctx.Err())
	}
	final, err := worker.New(ctx, all[1], "parent-after-children", handlers)
	if err != nil {
		t.Fatal(err)
	}
	finalCtx, stopFinal := context.WithCancel(ctx)
	defer stopFinal()
	finalDone := make(chan error, 1)
	go func() { finalDone <- final.RunPartition(finalCtx, parentPart) }()
	value, err := client.New(all[0]).Await(ctx, "parent", "large-fanout")
	if err != nil || string(value) != "249500" {
		t.Fatalf("parent result=%s err=%v", value, err)
	}
	stopFinal()
	if err := <-finalDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("final parent worker: %v", err)
	}
	stopChildren()
	for _, done := range childDone {
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("child worker: %v", err)
		}
	}
	report, err := integrity.Check(ctx, all[0])
	if err != nil {
		t.Fatal(err)
	}
	if report.Invocations != childCount+1 || report.Journals != childCount+1 || report.Terminal != childCount+1 {
		t.Fatalf("fan-out integrity: %+v", report)
	}
	inv, err := all[0].Stream(ctx, "WF_INV")
	if err != nil {
		t.Fatal(err)
	}
	info, err := inv.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != childCount+1 {
		t.Fatalf("invocation count=%d", info.State.Msgs)
	}
	for _, id := range childIDs {
		message, err := inv.GetLastMsgForSubject(ctx, identity.InvocationSubject("child", id))
		if err != nil || message.Header.Get(client.ParentTypeHeader) != "parent" || message.Header.Get(client.ParentIDHeader) != "large-fanout" {
			t.Fatalf("child %s invocation=%+v err=%v", id, message, err)
		}
	}
	t.Logf("completed %d children; %d shared the parent partition", childCount, insideParent)
}
