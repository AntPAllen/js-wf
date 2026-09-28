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
	"js-wf/provision"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go/jetstream"
)

func TestStateSurvivesSuspensionAndSnapshot(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	const typ, id = "test", "state-snapshot"
	w, err := worker.New(ctx, all[1], "state-worker", map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		for i := 0; i < 130; i++ {
			if err := c.SetState("count", i); err != nil {
				return nil, err
			}
		}
		if _, err := wf.AwaitSignal(c, "go"); err != nil {
			return nil, err
		}
		var count int
		found, err := c.GetState("count", &count)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, errors.New("state disappeared")
		}
		return json.Marshal(count)
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
	state, err := all[0].KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	stream, err := all[0].Stream(ctx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	for ctx.Err() == nil {
		if _, err := state.Get(ctx, "snap."+identity.Key(typ, id)); err == nil {
			if _, err := stream.GetMsg(ctx, 1); errors.Is(err, jetstream.ErrMsgNotFound) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("worker did not compact suspended state journal")
	}
	if _, err := c.Signal(ctx, typ, id, "go", []byte(`true`), "resume"); err != nil {
		t.Fatal(err)
	}
	result, err := c.Await(ctx, typ, id)
	if err != nil || string(result) != "129" {
		t.Fatalf("result=%s err=%v", result, err)
	}
	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := integrity.Check(ctx, all[0]); err != nil {
		t.Fatal(err)
	}
}
