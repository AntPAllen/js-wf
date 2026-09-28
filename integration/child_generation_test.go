package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/retention"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go/jetstream"
)

func TestLateChildCannotSignalReusedParent(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	const parentType, parentID = "parent", "reused-with-child"
	parentHandler := func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
		if _, err := wf.CallAsync(c, "child", input); err != nil {
			return nil, err
		}
		return json.RawMessage(`true`), nil
	}
	parentWorker, err := worker.New(ctx, all[1], "parent-generation-worker", map[string]worker.Handler{parentType: parentHandler})
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stopParent := context.WithCancel(ctx)
	parentDone := make(chan error, 1)
	go func() {
		parentDone <- parentWorker.RunPartition(workerCtx, identity.Partition(parentType, parentID, provision.Partitions))
	}()
	defer stopParent()
	c := client.New(all[0])
	j := journal.New(all[2])
	childID := func() string {
		records, _, err := j.Read(ctx, parentType, parentID)
		if err != nil {
			t.Fatal(err)
		}
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
				return request.ChildID
			}
		}
		return ""
	}
	first, err := c.Start(ctx, parentType, parentID, []byte(`7`))
	if err != nil {
		t.Fatal(err)
	}
	if value, err := c.Await(ctx, parentType, parentID); err != nil || string(value) != "true" {
		t.Fatalf("first parent: value=%s err=%v", value, err)
	}
	oldChild := childID()
	if oldChild == "" {
		t.Fatal("first parent did not record child ID")
	}
	for ctx.Err() == nil {
		err = retention.Purge(ctx, all[0], parentType, parentID, time.Hour)
		if err == nil {
			break
		}
		if !errors.Is(err, retention.ErrActive) {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("first parent purge: %v", err)
	}
	if _, err := c.SignalToGeneration(ctx, parentType, parentID, "child_0", []byte(`true`), "retired-child", first.InvSeq); !errors.Is(err, client.ErrStaleGeneration) {
		t.Fatalf("old child signaled purged parent: %v", err)
	}
	second, err := c.Start(ctx, parentType, parentID, []byte(`8`))
	if err != nil || second.InvSeq == first.InvSeq {
		t.Fatalf("reused parent start: first=%+v second=%+v err=%v", first, second, err)
	}
	if value, err := c.Await(ctx, parentType, parentID); err != nil || string(value) != "true" {
		t.Fatalf("second parent: value=%s err=%v", value, err)
	}
	newChild := childID()
	if newChild == "" || newChild == oldChild {
		t.Fatalf("child ID crossed generations: old=%q new=%q", oldChild, newChild)
	}
	if _, err := c.SignalToGeneration(ctx, parentType, parentID, "child_0", []byte(`true`), "stale-child", first.InvSeq); !errors.Is(err, client.ErrStaleGeneration) {
		t.Fatalf("old child signaled reused parent: %v", err)
	}
	stopParent()
	if err := <-parentDone; err != nil {
		t.Fatal(err)
	}
	childWorker, err := worker.New(ctx, all[1], "late-child-worker", map[string]worker.Handler{"child": func(_ *wf.Context, input json.RawMessage) (json.RawMessage, error) {
		return input, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	childCtx, stopChild := context.WithCancel(ctx)
	defer stopChild()
	part := identity.Partition("child", oldChild, provision.Partitions)
	childDone := make(chan error, 1)
	go func() { childDone <- childWorker.RunPartition(childCtx, part) }()
	if value, err := c.Await(ctx, "child", oldChild); err != nil || string(value) != "7" {
		t.Fatalf("old child result: value=%s err=%v", value, err)
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
		t.Fatal("old child wakeup was not acknowledged")
	}
	stopChild()
	if err := <-childDone; err != nil {
		t.Fatal(err)
	}
	sig, err := all[2].Stream(ctx, "WF_SIG")
	if err != nil {
		t.Fatal(err)
	}
	info, err := sig.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for seq := uint64(1); seq <= info.State.LastSeq; seq++ {
		message, err := sig.GetMsg(ctx, seq)
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(message.Subject, "wf.sig.parent.reused-with-child.") {
			var result wf.Outcome
			if err := json.Unmarshal(message.Data, &result); err != nil {
				t.Fatal(err)
			}
			if string(result.Result) == "7" || message.Header.Get("Wf-Inv-Seq") == fmt.Sprint(first.InvSeq) {
				t.Fatalf("old child result published to reused parent: %+v", message)
			}
		}
	}
	if _, err := integrity.Check(ctx, all[0]); err != nil {
		t.Fatalf("generation integrity: %v", err)
	}
}
