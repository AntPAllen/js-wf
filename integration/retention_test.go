package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/history"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/retention"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/anishathalye/porcupine"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestPurgeTombstoneAndIDReuse(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	const typ, id = "test", "reuse"
	handler := func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
		if string(input) == "1" {
			return input, nil
		}
		return wf.AwaitSignal(c, "go")
	}
	w, err := worker.New(ctx, all[1], "reuse-worker", map[string]worker.Handler{typ: handler})
	if err != nil {
		t.Fatal(err)
	}
	recorder := &history.Recorder{}
	c := client.NewObserved(all[0], recorder)
	first, err := c.Start(ctx, typ, id, []byte(`1`))
	if err != nil {
		t.Fatal(err)
	}
	part := identity.Partition(typ, id, provision.Partitions)
	workerCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.RunPartition(workerCtx, part) }()
	value, err := c.Await(ctx, typ, id)
	if err != nil || string(value) != "1" {
		workerExited := false
		var workerErr error
		select {
		case workerErr = <-done:
			workerExited = true
		default:
		}
		inspectCtx, stopInspect := context.WithTimeout(context.Background(), 3*time.Second)
		defer stopInspect()
		records, _, journalErr := journal.New(all[2]).Read(inspectCtx, typ, id)
		run, runErr := all[0].Stream(inspectCtx, "WF_RUN")
		var runState any
		if runErr == nil {
			info, infoErr := run.Info(inspectCtx)
			if infoErr == nil {
				runState = info.State
			} else {
				runErr = infoErr
			}
		}
		t.Fatalf("first result=%s err=%v workerExited=%v workerErr=%v records=%+v journalErr=%v run=%+v runErr=%v", value, err, workerExited, workerErr, records, journalErr, runState, runErr)
	}
	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	var readers sync.WaitGroup
	readErrors := make(chan error, 100)
	for i := 0; i < 100; i++ {
		readers.Add(1)
		go func(i int) {
			defer readers.Done()
			value, err := client.NewObserved(all[i%len(all)], recorder).Await(ctx, typ, id)
			if err != nil || string(value) != "1" {
				readErrors <- fmt.Errorf("reader %d result=%s: %v", i, value, err)
			}
		}(i)
	}
	readers.Wait()
	close(readErrors)
	for err := range readErrors {
		t.Error(err)
	}
	if t.Failed() {
		return
	}
	if _, err := journal.New(all[0]).SnapshotPrefix(ctx, typ, id, 1); err != nil {
		t.Fatal(err)
	}
	for ctx.Err() == nil {
		err = retention.Purge(ctx, all[0], typ, id, time.Minute)
		if !errors.Is(err, retention.ErrActive) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := retention.Purge(ctx, all[0], typ, id, time.Minute); err != nil {
		t.Fatalf("purge retry: %v", err)
	}
	if _, err := c.Await(ctx, typ, id); !errors.Is(err, client.ErrPurged) {
		t.Fatalf("purged result: %v", err)
	}
	state, err := all[0].KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	entry, err := state.Get(ctx, identity.Key(typ, id))
	if err != nil {
		t.Fatal(err)
	}
	marker, tomb, err := retention.Decode(entry.Value())
	if err != nil || !tomb || marker.InvSeq != first.InvSeq {
		t.Fatalf("tombstone=%+v found=%v err=%v", marker, tomb, err)
	}
	if _, err := state.Get(ctx, "snap."+identity.Key(typ, id)); !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatalf("snapshot pointer retained: %v", err)
	}
	stale := &nats.Msg{Subject: "wf.sig.test.reuse.go", Data: []byte(`99`), Header: nats.Header{}}
	stale.Header.Set("Wf-Inv-Seq", strconv.FormatUint(first.InvSeq, 10))
	if _, err := all[0].PublishMsg(ctx, stale); err != nil {
		t.Fatal(err)
	}
	second, err := c.Start(ctx, typ, id, []byte(`2`))
	if err != nil || second.InvSeq <= first.InvSeq {
		t.Fatalf("reused start: %+v err=%v", second, err)
	}
	swept, err := retention.SweepTombstones(ctx, all[2], time.Now().Add(2*time.Minute))
	if err != nil || swept.Deleted != 1 {
		t.Fatalf("sweep reused id=%+v err=%v", swept, err)
	}
	newCtx, stopNew := context.WithCancel(ctx)
	defer stopNew()
	newDone := make(chan error, 1)
	go func() { newDone <- w.RunPartition(newCtx, part) }()
	for ctx.Err() == nil {
		records, _, err := journal.New(all[0]).Read(ctx, typ, id)
		if err != nil {
			t.Fatal(err)
		}
		if len(records) > 0 && records[len(records)-1].Kind == journal.Suspended {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("reused invocation did not suspend after stale signal")
	}
	secondSignal, err := c.Signal(ctx, typ, id, "go", []byte(`2`), "fresh")
	if err != nil {
		t.Fatal(err)
	}
	value, err = c.Await(ctx, typ, id)
	if err != nil || string(value) != "2" {
		t.Fatalf("reused result=%s err=%v", value, err)
	}
	stopNew()
	if err := <-newDone; err != nil {
		t.Fatal(err)
	}
	records, _, err := journal.New(all[0]).Read(ctx, typ, id)
	if err != nil || len(records) == 0 || records[0].Kind != journal.Started || records[0].Index != 0 {
		t.Fatalf("new journal: entries=%d err=%v", len(records), err)
	}
	var consumed []uint64
	for _, record := range records {
		if record.Kind == journal.SignalConsumed {
			var event struct {
				Sequence uint64 `json:"sig_seq"`
			}
			if err := json.Unmarshal(record.Payload, &event); err != nil {
				t.Fatal(err)
			}
			consumed = append(consumed, event.Sequence)
		}
	}
	if len(consumed) != 1 || consumed[0] != secondSignal {
		t.Fatalf("new journal consumed signals %v, want only %d", consumed, secondSignal)
	}
	if _, err := integrity.Check(ctx, all[0]); err != nil {
		t.Fatal(err)
	}
	if result, err := history.CheckResults(recorder.Snapshot(), 10*time.Second); err != nil || result != porcupine.Ok {
		t.Fatalf("result history=%s err=%v", result, err)
	}
}

func TestRetentionRunsAsWorkflow(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c := client.New(all[0])
	target, err := worker.New(ctx, all[1], "target-worker", map[string]worker.Handler{"test": func(_ *wf.Context, input json.RawMessage) (json.RawMessage, error) { return input, nil }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Start(ctx, "test", "to-retire", []byte(`42`)); err != nil {
		t.Fatal(err)
	}
	targetCtx, stopTarget := context.WithCancel(ctx)
	targetDone := make(chan error, 1)
	go func() {
		targetDone <- target.RunPartition(targetCtx, identity.Partition("test", "to-retire", provision.Partitions))
	}()
	if value, err := c.Await(ctx, "test", "to-retire"); err != nil || string(value) != "42" {
		t.Fatalf("target result=%s err=%v", value, err)
	}
	stopTarget()
	if err := <-targetDone; err != nil {
		t.Fatal(err)
	}
	purger, err := worker.New(ctx, all[2], "retention-worker", map[string]worker.Handler{"retention": retention.Handler(all[2], time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	request, _ := json.Marshal(retention.Request{Type: "test", ID: "to-retire"})
	if _, err := c.Start(ctx, "retention", "job-1", request); err != nil {
		t.Fatal(err)
	}
	purgeCtx, stopPurger := context.WithCancel(ctx)
	defer stopPurger()
	purgeDone := make(chan error, 1)
	go func() {
		purgeDone <- purger.RunPartition(purgeCtx, identity.Partition("retention", "job-1", provision.Partitions))
	}()
	if value, err := c.Await(ctx, "retention", "job-1"); err != nil || string(value) != "true" {
		t.Fatalf("purge workflow result=%s err=%v", value, err)
	}
	stopPurger()
	if err := <-purgeDone; err != nil {
		t.Fatal(err)
	}
	if _, err := c.Await(ctx, "test", "to-retire"); !errors.Is(err, client.ErrPurged) {
		t.Fatalf("retired target: %v", err)
	}
	if _, err := integrity.Check(ctx, all[0]); err != nil {
		t.Fatal(err)
	}
}
