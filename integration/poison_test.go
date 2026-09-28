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

func TestHandlerPanicAttemptsSurviveWorkerRestart(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	const typ, id = "poison", "handler-panic"
	var calls atomic.Int32
	handler := func(*wf.Context, json.RawMessage) (json.RawMessage, error) {
		calls.Add(1)
		panic("poison")
	}
	first, err := worker.New(ctx, all[1], "poison-first", map[string]worker.Handler{typ: handler}, worker.WithMaxPanicAttempts(3))
	if err != nil {
		t.Fatal(err)
	}
	firstCtx, stopFirst := context.WithCancel(ctx)
	firstDone := make(chan error, 1)
	part := identity.Partition(typ, id, provision.Partitions)
	go func() { firstDone <- first.RunPartition(firstCtx, part) }()
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
		if len(records) >= 2 && records[len(records)-1].Kind == journal.Attempt {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("first panic attempt was not recorded")
	}
	stopFirst()
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	second, err := worker.New(ctx, all[2], "poison-second", map[string]worker.Handler{typ: handler}, worker.WithMaxPanicAttempts(3))
	if err != nil {
		t.Fatal(err)
	}
	secondCtx, stopSecond := context.WithCancel(ctx)
	defer stopSecond()
	secondDone := make(chan error, 1)
	go func() { secondDone <- second.RunPartition(secondCtx, part) }()
	_, err = c.Await(ctx, typ, id)
	if err == nil || !strings.Contains(err.Error(), "workflow panic: poison") {
		t.Fatalf("await poison invocation: %v", err)
	}
	stopSecond()
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	records, _, err := j.Read(ctx, typ, id)
	if err != nil {
		t.Fatal(err)
	}
	var attempts int
	for _, record := range records {
		if record.Kind != journal.Attempt {
			continue
		}
		attempt, err := journal.DecodeAttempt(record.Payload)
		if err != nil || attempt.Count != attempts+1 {
			t.Fatalf("attempt %d: %+v err=%v", attempts+1, attempt, err)
		}
		attempts++
	}
	if attempts != 3 || calls.Load() != 3 || records[len(records)-1].Kind != journal.Failed || first.Metrics().LeaseAcquisitions == 0 || second.Metrics().LeaseAcquisitions == 0 {
		t.Fatalf("attempts=%d calls=%d terminal=%s first=%+v second=%+v", attempts, calls.Load(), records[len(records)-1].Kind, first.Metrics(), second.Metrics())
	}
	if _, err := integrity.Check(ctx, all[0]); err != nil {
		t.Fatalf("integrity: %v", err)
	}
}

func TestFinalPanicAttemptCompletesAfterCrash(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	const typ, id = "poison", "final-attempt-crash"
	c := client.New(all[0])
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	j := journal.New(all[1])
	seq, err := j.Append(ctx, typ, id, journal.Entry{Epoch: 1, Index: 0, Kind: journal.Started}, 0)
	if err != nil {
		t.Fatal(err)
	}
	for count := 1; count <= 3; count++ {
		payload, _ := json.Marshal(journal.AttemptPayload{Count: count, Error: "workflow panic: poison"})
		seq, err = j.Append(ctx, typ, id, journal.Entry{Epoch: 1, Index: uint64(count), Kind: journal.Attempt, Payload: payload}, seq)
		if err != nil {
			t.Fatal(err)
		}
	}
	var calls atomic.Int32
	w, err := worker.New(ctx, all[2], "poison-recovery", map[string]worker.Handler{typ: func(*wf.Context, json.RawMessage) (json.RawMessage, error) {
		calls.Add(1)
		panic("must not run")
	}}, worker.WithMaxPanicAttempts(3))
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- w.RunPartition(workerCtx, identity.Partition(typ, id, provision.Partitions)) }()
	_, err = c.Await(ctx, typ, id)
	if err == nil || !strings.Contains(err.Error(), "workflow panic: poison") {
		t.Fatalf("await final attempt: %v", err)
	}
	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	records, _, err := j.Read(ctx, typ, id)
	if err != nil || len(records) != 5 || records[4].Kind != journal.Failed || calls.Load() != 0 {
		t.Fatalf("final attempt recovery: records=%+v calls=%d err=%v", records, calls.Load(), err)
	}
	if _, err := integrity.Check(ctx, all[0]); err != nil {
		t.Fatalf("integrity: %v", err)
	}
}
