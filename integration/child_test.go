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
	"js-wf/wf"
	"js-wf/worker"
)

func TestChildCallResumesParent(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	parent, err := worker.New(ctx, all[1], "parent-worker", map[string]worker.Handler{"parent": func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
		result, err := wf.Call(c, "child", input)
		if err != nil {
			return nil, err
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
	parentDone := make(chan error, 1)
	go func() {
		parentDone <- parent.RunPartition(workerCtx, identity.Partition("parent", "root", provision.Partitions))
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
				childID = req.ChildID
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
	childDone := make(chan error, 1)
	go func() {
		childDone <- child.RunPartition(workerCtx, identity.Partition("child", childID, provision.Partitions))
	}()
	result, err := c.Await(ctx, "parent", "root")
	if err != nil || string(result) != "42" {
		t.Fatalf("result=%s err=%v", result, err)
	}
	stop()
	<-parentDone
	<-childDone
	if _, err := integrity.Check(ctx, all[0]); err != nil {
		t.Fatalf("integrity: %v", err)
	}
	childRecords, _, err := j.Read(ctx, "child", childID)
	if err != nil || childRecords[len(childRecords)-1].Kind != journal.Completed {
		t.Fatalf("child journal: %v", err)
	}
}

func TestAsyncChildFanout(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	handlers := map[string]worker.Handler{}
	handlers["parent"] = func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		promises := make([]wf.Promise, 5)
		for i := range promises {
			input, _ := json.Marshal(i)
			p, err := wf.CallAsync(c, "child", input)
			if err != nil {
				return nil, err
			}
			promises[i] = p
		}
		sum := 0
		for _, p := range promises {
			result, err := wf.AwaitPromise(c, p)
			if err != nil {
				return nil, err
			}
			var n int
			if err := json.Unmarshal(result, &n); err != nil {
				return nil, err
			}
			if len(result) > 0 {
				result[0] = '!'
			}
			repeated, err := wf.AwaitPromise(c, p)
			if err != nil {
				return nil, err
			}
			var again int
			if err := json.Unmarshal(repeated, &again); err != nil || again != n {
				return nil, fmt.Errorf("repeated child result changed: %s (%v)", repeated, err)
			}
			sum += n
		}
		return json.Marshal(sum)
	}
	handlers["child"] = func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
		var n int
		if err := json.Unmarshal(input, &n); err != nil {
			return nil, err
		}
		value, err := wf.Run(c, "double", n, func(context.Context) (int, error) { return n * 2, nil })
		if err != nil {
			return nil, err
		}
		return json.Marshal(value)
	}
	w, err := worker.New(ctx, all[1], "fanout-worker", handlers)
	if err != nil {
		t.Fatal(err)
	}
	c := client.New(all[0])
	if _, err := c.Start(ctx, "parent", "fanout", []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	var done []chan error
	startPartition := func(part uint32) {
		ch := make(chan error, 1)
		done = append(done, ch)
		go func() { ch <- w.RunPartition(workerCtx, part) }()
	}
	parentPartition := identity.Partition("parent", "fanout", provision.Partitions)
	startPartition(parentPartition)
	j := journal.New(all[0])
	var childIDs []string
	for {
		records, _, err := j.Read(ctx, "parent", "fanout")
		if err != nil {
			t.Fatal(err)
		}
		childIDs = childIDs[:0]
		for _, r := range records {
			if r.Kind != journal.StepRequested {
				continue
			}
			var req struct {
				Kind    string `json:"kind"`
				ChildID string `json:"child_id"`
			}
			if err := json.Unmarshal(r.Payload, &req); err != nil {
				t.Fatal(err)
			}
			if req.Kind == "call_async" {
				childIDs = append(childIDs, req.ChildID)
			}
		}
		if len(childIDs) == 5 && records[len(records)-1].Kind == journal.Suspended {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("parent did not start all children")
		}
		time.Sleep(30 * time.Millisecond)
	}
	partitions := map[uint32]bool{parentPartition: true}
	for _, childID := range childIDs {
		part := identity.Partition("child", childID, provision.Partitions)
		if !partitions[part] {
			startPartition(part)
			partitions[part] = true
		}
	}
	result, err := c.Await(ctx, "parent", "fanout")
	if err != nil || string(result) != "20" {
		debugCtx, done := context.WithTimeout(context.Background(), 5*time.Second)
		if records, _, readErr := j.Read(debugCtx, "parent", "fanout"); readErr == nil {
			for _, r := range records {
				t.Logf("parent journal index=%d kind=%s payload=%s", r.Index, r.Kind, r.Payload)
			}
		} else {
			t.Logf("parent journal read: %v", readErr)
		}
		for _, childID := range childIDs {
			if records, _, readErr := j.Read(debugCtx, "child", childID); readErr == nil {
				t.Logf("child %s journal entries=%d terminal=%v", childID, len(records), len(records) > 0 && records[len(records)-1].Kind == journal.Completed)
			} else {
				t.Logf("child %s read: %v", childID, readErr)
			}
		}
		done()
		t.Fatalf("result=%s err=%v", result, err)
	}
	stop()
	for _, ch := range done {
		if err := <-ch; err != nil {
			t.Fatalf("worker: %v", err)
		}
	}
	report, err := integrity.Check(ctx, all[0])
	if err != nil {
		t.Fatal(err)
	}
	if report.Invocations != 6 || report.Terminal != 6 {
		t.Fatal(fmt.Sprintf("report=%+v", report))
	}
}
