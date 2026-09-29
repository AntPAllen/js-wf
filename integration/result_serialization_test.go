package integration_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
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

func TestUnserializableStepResultFailsDurably(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	const typ, id = "result-serialization", "channel"
	var effects atomic.Int32
	handler := func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		_, err := wf.Run(c, "bad-result", nil, func(context.Context) (chan int, error) {
			effects.Add(1)
			return make(chan int), nil
		})
		return nil, err
	}
	w, err := worker.New(ctx, all[1], "serialization-worker", map[string]worker.Handler{typ: handler})
	if err != nil {
		t.Fatal(err)
	}
	workCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- w.RunPartition(workCtx, identity.Partition(typ, id, provision.Partitions)) }()
	c := client.New(all[0])
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Await(ctx, typ, id); err == nil || !strings.Contains(err.Error(), wf.ErrStepResultNotSerializable.Error()) {
		t.Fatalf("terminal result error: %v", err)
	}
	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	var records []journal.Record
	j := journal.New(all[2])
	until := time.Now().Add(5 * time.Second)
	for {
		records, _, err = j.Read(ctx, typ, id)
		if (err == nil && len(records) == 4 && records[3].Kind == journal.Failed) || time.Now().After(until) || ctx.Err() != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil || len(records) != 4 || records[1].Kind != journal.StepRequested || records[2].Kind != journal.StepCompleted || records[3].Kind != journal.Failed || effects.Load() != 1 {
		t.Fatalf("retained failure: entries=%d effects=%d err=%v", len(records), effects.Load(), err)
	}
	if _, err := integrity.Check(ctx, all[0]); err != nil {
		t.Fatalf("retained integrity: %v", err)
	}
}
