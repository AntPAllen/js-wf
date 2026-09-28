package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
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
)

func TestCancellationIsJournaledAndIdempotent(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	const typ, id = "cancel", "suspended"
	w, err := worker.New(ctx, all[1], "cancel-worker", map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		_, err := wf.AwaitSignal(c, "go")
		if err != nil {
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
	c := client.New(all[0])
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
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
	if _, err := c.Signal(ctx, typ, id, client.CancelSignalName, nil, "spoof"); !errors.Is(err, client.ErrReservedSignal) {
		t.Fatalf("reserved signal: %v", err)
	}
	first, err := c.Cancel(ctx, typ, id)
	if err != nil {
		t.Fatal(err)
	}
	second, err := c.Cancel(ctx, typ, id)
	if err != nil || second != first {
		t.Fatalf("cancel retry sequences %d, %d: %v", first, second, err)
	}
	if _, err := c.Await(ctx, typ, id); !errors.Is(err, client.ErrCancelled) {
		t.Fatalf("cancel result: %v", err)
	}
	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	records, _, err := j.Read(ctx, typ, id)
	if err != nil {
		t.Fatal(err)
	}
	var consumed int
	for _, record := range records {
		if record.Kind != journal.SignalConsumed {
			continue
		}
		var signal struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(record.Payload, &signal); err != nil || signal.Name != client.CancelSignalName {
			t.Fatalf("consumed signal=%+v err=%v", signal, err)
		}
		consumed++
	}
	if consumed != 1 || records[len(records)-1].Kind != journal.Failed {
		t.Fatalf("cancel journal consumed=%d terminal=%s", consumed, records[len(records)-1].Kind)
	}
	if _, err := integrity.Check(ctx, all[0]); err != nil {
		t.Fatalf("integrity: %v", err)
	}
}

func TestCancellationInterruptsRunningEffect(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	const typ, id = "cancel", "running-effect"
	entered := make(chan struct{})
	effectStopped := make(chan struct{})
	w, err := worker.New(ctx, all[1], "running-cancel-worker", map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		_, err := wf.Run(c, "block", 0, func(effectCtx context.Context) (int, error) {
			close(entered)
			<-effectCtx.Done()
			close(effectStopped)
			return 0, effectCtx.Err()
		})
		return nil, err
	}})
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- w.RunPartition(workerCtx, identity.Partition(typ, id, provision.Partitions)) }()
	c := client.New(all[0])
	handle, err := c.Start(ctx, typ, id, []byte(`null`))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("effect did not start")
	}
	stale := &nats.Msg{Subject: "wf.sig." + typ + "." + id + "." + client.CancelSignalName, Header: nats.Header{}}
	stale.Header.Set("Wf-Inv-Seq", strconv.FormatUint(handle.InvSeq+1, 10))
	if _, err := all[0].PublishMsg(ctx, stale); err != nil {
		t.Fatal(err)
	}
	select {
	case <-effectStopped:
		t.Fatal("stale-generation cancel interrupted the effect")
	case <-time.After(300 * time.Millisecond):
	}
	if _, err := c.Cancel(ctx, typ, id); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Await(ctx, typ, id); !errors.Is(err, client.ErrCancelled) {
		t.Fatalf("cancelled result: %v", err)
	}
	select {
	case <-effectStopped:
	case <-ctx.Done():
		t.Fatal("running effect did not receive cancellation")
	}
	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	records, _, err := journal.New(all[2]).Read(ctx, typ, id)
	if err != nil {
		t.Fatal(err)
	}
	consumed := 0
	for _, record := range records {
		if record.Kind == journal.StepCompleted {
			t.Fatal("canceled effect journaled a completion")
		}
		if record.Kind == journal.SignalConsumed {
			var signal struct {
				Name string `json:"name"`
			}
			if err := json.Unmarshal(record.Payload, &signal); err != nil {
				t.Fatal(err)
			}
			if signal.Name == client.CancelSignalName {
				consumed++
			}
		}
	}
	if consumed != 1 || records[len(records)-1].Kind != journal.Failed {
		t.Fatalf("consumed=%d journal=%+v", consumed, records)
	}
	if _, err := integrity.Check(ctx, all[0]); err != nil {
		t.Fatal(err)
	}
}
