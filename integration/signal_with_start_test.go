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
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/wf"
	"js-wf/worker"
)

func TestSignalWithStartBuffersBeforeWorkerAndFencesInput(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	const typ, id = "signalstart", "before-worker"
	c := client.New(all[0])
	if seq, err := c.Signal(ctx, typ, id, "ready", []byte(`"early"`), "early"); seq != 0 || !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("ordinary signal before start: seq=%d err=%v", seq, err)
	}
	handle, first, err := c.SignalWithStart(ctx, typ, id, "ready", []byte(`"first"`), "first", []byte(`"seed"`))
	if err != nil || handle.InvSeq == 0 || first == 0 {
		t.Fatalf("signal with start: handle=%+v seq=%d err=%v", handle, first, err)
	}
	retryHandle, duplicate, err := client.New(all[1]).SignalWithStart(ctx, typ, id, "ready", []byte(`"first"`), "first", []byte(`"seed"`))
	if err != nil || retryHandle.InvSeq != handle.InvSeq || duplicate != first {
		t.Fatalf("matching retry: handle=%+v seq=%d err=%v", retryHandle, duplicate, err)
	}
	if _, seq, err := c.SignalWithStart(ctx, typ, id, "ready", []byte(`"other"`), "other", []byte(`"changed"`)); seq != 0 || !errors.Is(err, client.ErrInputMismatch) {
		t.Fatalf("changed start input: seq=%d err=%v", seq, err)
	}
	if _, seq, err := c.SignalWithStart(ctx, typ, id, "ready", []byte(`"changed"`), "first", []byte(`"seed"`)); seq != 0 || !errors.Is(err, client.ErrSignalMismatch) {
		t.Fatalf("changed signal payload: seq=%d err=%v", seq, err)
	}
	w, err := worker.New(ctx, all[2], "signal-start-worker", map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		value, err := wf.AwaitSignal(c, "ready")
		return json.RawMessage(value), err
	}})
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- w.RunPartition(workerCtx, identity.Partition(typ, id, provision.Partitions)) }()
	result, err := c.Await(ctx, typ, id)
	if err != nil || string(result) != `"first"` {
		t.Fatalf("buffered result=%s err=%v", result, err)
	}
	records, _, err := journal.New(all[1]).Read(ctx, typ, id)
	if err != nil {
		t.Fatal(err)
	}
	consumed := 0
	for _, record := range records {
		if record.Kind == journal.SignalConsumed {
			consumed++
		}
	}
	if consumed != 1 || records[len(records)-1].Kind != journal.Completed {
		t.Fatalf("buffered signal journal: consumed=%d records=%+v", consumed, records)
	}
	inv, err := all[1].Stream(ctx, "WF_INV")
	if err != nil {
		t.Fatal(err)
	}
	stored, err := inv.GetLastMsgForSubject(ctx, identity.InvocationSubject(typ, id))
	if err != nil || stored.Sequence != handle.InvSeq || string(stored.Data) != `"seed"` {
		t.Fatalf("invocation after retries: stored=%+v err=%v", stored, err)
	}
	signals, err := all[1].Stream(ctx, "WF_SIG")
	if err != nil {
		t.Fatal(err)
	}
	sigInfo, err := signals.Info(ctx)
	if err != nil || sigInfo.State.Msgs != 1 {
		t.Fatalf("signal count after retries: info=%+v err=%v", sigInfo, err)
	}
	sig, err := signals.GetLastMsgForSubject(ctx, "wf.sig."+typ+"."+id+".ready")
	if err != nil || sig.Sequence != first || sig.Header.Get("Wf-Inv-Seq") != strconv.FormatUint(handle.InvSeq, 10) {
		t.Fatalf("generation-fenced signal: msg=%+v err=%v", sig, err)
	}
	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestSignalWithStartRacesMatchingStart(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const typ = "signalstart"
	for index := 0; index < 20; index++ {
		id := fmt.Sprintf("race-%d", index)
		gate := make(chan struct{})
		var group sync.WaitGroup
		var started, signaled client.Handle
		var startErr, signalErr error
		var signalSeq uint64
		group.Add(2)
		go func() {
			defer group.Done()
			<-gate
			started, startErr = client.New(all[0]).Start(ctx, typ, id, []byte(`"same"`))
		}()
		go func() {
			defer group.Done()
			<-gate
			signaled, signalSeq, signalErr = client.New(all[1]).SignalWithStart(ctx, typ, id, "ready", []byte(`1`), "race", []byte(`"same"`))
		}()
		close(gate)
		group.Wait()
		if startErr != nil && !errors.Is(startErr, client.ErrAlreadyStarted) || signalErr != nil || started.InvSeq == 0 || signaled.InvSeq != started.InvSeq || signalSeq == 0 {
			t.Fatalf("race %d: start=%+v err=%v signal=%+v seq=%d err=%v", index, started, startErr, signaled, signalSeq, signalErr)
		}
	}
	for _, name := range []string{"WF_INV", "WF_SIG", "WF_RUN"} {
		stream, err := all[2].Stream(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		info, err := stream.Info(ctx)
		want := uint64(20)
		if name == "WF_RUN" {
			want = 40
		}
		if err != nil || info.State.Msgs != want {
			t.Fatalf("%s after concurrent start and signal: info=%+v want=%d err=%v", name, info, want, err)
		}
	}
}
