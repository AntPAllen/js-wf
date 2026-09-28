package integration_test

import (
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
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestGrandchildResultSurvivesMiddleSignalGap(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	const rootType, rootID = "root", "three-deep"
	handlers := map[string]worker.Handler{
		"root": func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
			result, err := wf.Call(c, "middle", input)
			return json.RawMessage(result), err
		},
		"middle": func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
			result, err := wf.Call(c, "leaf", input)
			return json.RawMessage(result), err
		},
		"leaf": func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
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
	fault := &ackLossJS{JetStream: all[1], subject: "wf.sig.root.three-deep.child_0", dropBeforePublish: true, lostErr: nats.ErrTimeout}
	first, err := worker.New(ctx, fault, "chain-before-gap", handlers)
	if err != nil {
		t.Fatal(err)
	}
	firstCtx, stopFirst := context.WithCancel(ctx)
	defer stopFirst()
	firstParts := map[uint32]bool{}
	var firstDone []<-chan error
	startPartitions := func(w *worker.Worker, workerCtx context.Context, parts map[uint32]bool, done *[]<-chan error, typ, id string) {
		part := identity.Partition(typ, id, provision.Partitions)
		if parts[part] {
			return
		}
		parts[part] = true
		ch := make(chan error, 1)
		*done = append(*done, ch)
		go func() { ch <- w.RunPartition(workerCtx, part) }()
	}
	startPartitions(first, firstCtx, firstParts, &firstDone, rootType, rootID)
	api := client.New(all[0])
	if _, err := api.Start(ctx, rootType, rootID, []byte(`7`)); err != nil {
		t.Fatal(err)
	}
	j := journal.New(all[2])
	waitChildID := func(typ, id string) string {
		for ctx.Err() == nil {
			records, _, err := j.Read(ctx, typ, id)
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
				if request.Kind == "call" && request.ChildID != "" {
					return request.ChildID
				}
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("%s.%s did not start its child: %v", typ, id, ctx.Err())
		return ""
	}
	middleID := waitChildID(rootType, rootID)
	startPartitions(first, firstCtx, firstParts, &firstDone, "middle", middleID)
	leafID := waitChildID("middle", middleID)
	startPartitions(first, firstCtx, firstParts, &firstDone, "leaf", leafID)
	for !fault.fired.Load() && ctx.Err() == nil {
		time.Sleep(10 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("middle did not reach injected signal gap")
	}
	stopFirst()
	for _, done := range firstDone {
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	}
	for _, item := range []struct{ typ, id string }{{"middle", middleID}, {"leaf", leafID}} {
		records, _, err := j.Read(ctx, item.typ, item.id)
		if err != nil || len(records) == 0 || records[len(records)-1].Kind != journal.Completed {
			t.Fatalf("%s terminal before restart: records=%+v err=%v", item.typ, records, err)
		}
	}
	sig, err := all[2].Stream(ctx, "WF_SIG")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sig.GetLastMsgForSubject(ctx, fault.subject); !errors.Is(err, jetstream.ErrMsgNotFound) {
		t.Fatalf("root signal reached store before recovery: %v", err)
	}
	second, err := worker.New(ctx, all[1], "chain-after-gap", handlers)
	if err != nil {
		t.Fatal(err)
	}
	secondCtx, stopSecond := context.WithCancel(ctx)
	defer stopSecond()
	secondParts := map[uint32]bool{}
	var secondDone []<-chan error
	for _, item := range []struct{ typ, id string }{{rootType, rootID}, {"middle", middleID}, {"leaf", leafID}} {
		startPartitions(second, secondCtx, secondParts, &secondDone, item.typ, item.id)
	}
	result, err := api.Await(ctx, rootType, rootID)
	if err != nil || string(result) != "14" {
		t.Fatalf("root after middle restart: result=%s err=%v", result, err)
	}
	stopSecond()
	for _, done := range secondDone {
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	}
	info, err := sig.Info(ctx)
	if err != nil || info.State.Msgs != 2 {
		t.Fatalf("one signal per chain edge: info=%+v err=%v", info, err)
	}
	if _, err := sig.GetLastMsgForSubject(ctx, fault.subject); err != nil {
		t.Fatalf("root result signal missing after recovery: %v", err)
	}
	for _, item := range []struct{ typ, id string }{{rootType, rootID}, {"middle", middleID}, {"leaf", leafID}} {
		records, _, err := j.Read(ctx, item.typ, item.id)
		if err != nil {
			t.Fatal(err)
		}
		terminal := 0
		for _, record := range records {
			if record.Kind == journal.Completed || record.Kind == journal.Failed {
				terminal++
			}
		}
		if terminal != 1 || records[len(records)-1].Kind != journal.Completed {
			t.Fatalf("%s.%s terminal count=%d journal=%+v", item.typ, item.id, terminal, records)
		}
	}
	if _, err := integrity.Check(ctx, all[0]); err != nil {
		t.Fatalf("chain integrity: %v", err)
	}
}
