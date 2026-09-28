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
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/reconcile"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestSuspendedScanRepairsMatchingSignalOnly(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	const typ, id = "test", "suspended-signal"
	c := client.New(all[0])
	handle, err := c.Start(ctx, typ, id, []byte(`null`))
	if err != nil {
		t.Fatal(err)
	}
	run, err := all[0].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	if err := run.Purge(ctx); err != nil {
		t.Fatal(err)
	}
	j := journal.New(all[0])
	seq, err := j.Append(ctx, typ, id, journal.Entry{Index: 0, Epoch: 0, Kind: journal.Started}, 0)
	if err != nil {
		t.Fatal(err)
	}
	seq, err = j.Append(ctx, typ, id, journal.Entry{Index: 1, Epoch: 0, Kind: journal.StepRequested, Payload: json.RawMessage(`{"kind":"signal","name":"go"}`)}, seq)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.Append(ctx, typ, id, journal.Entry{Index: 2, Epoch: 0, Kind: journal.Suspended, Payload: json.RawMessage(`{"waiting_on":"signal:go"}`)}, seq); err != nil {
		t.Fatal(err)
	}
	scan := reconcile.NewSuspendedScan(all[2])
	if result, err := scan.Scan(ctx, 0, 10, true); err != nil || result.Reenqueued != 0 {
		t.Fatalf("empty scan=%+v err=%v", result, err)
	}
	stale := &nats.Msg{Subject: "wf.sig.test.suspended-signal.go", Data: []byte(`0`), Header: nats.Header{}}
	stale.Header.Set("Wf-Inv-Seq", "0")
	if _, err := all[0].PublishMsg(ctx, stale); err != nil {
		t.Fatal(err)
	}
	if result, err := scan.Scan(ctx, 0, 10, true); err != nil || result.Reenqueued != 0 {
		t.Fatalf("stale generation scan=%+v err=%v", result, err)
	}
	current := &nats.Msg{Subject: stale.Subject, Data: []byte(`42`), Header: nats.Header{}}
	current.Header.Set("Wf-Inv-Seq", strconv.FormatUint(handle.InvSeq, 10))
	if _, err := all[0].PublishMsg(ctx, current); err != nil {
		t.Fatal(err)
	}
	result, err := scan.Scan(ctx, 0, 10, true)
	if err != nil || result.Reenqueued != 1 || len(result.Candidates) != 1 || result.Candidates[0].Reason != "signal" {
		t.Fatalf("dry run=%+v err=%v", result, err)
	}
	info, err := run.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 0 {
		t.Fatalf("dry run published a wakeup: state=%+v err=%v", info.State, err)
	}
	w, err := worker.New(ctx, all[1], "suspended-signal-worker", map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		value, err := wf.AwaitSignal(c, "go")
		return value, err
	}})
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- w.RunPartition(workerCtx, identity.Partition(typ, id, provision.Partitions)) }()
	controlCtx, stopControl := context.WithTimeout(ctx, 350*time.Millisecond)
	_, controlErr := c.Await(controlCtx, typ, id)
	stopControl()
	if !errors.Is(controlErr, context.DeadlineExceeded) {
		t.Fatalf("without reconciler the missing wakeup should stall: %v", controlErr)
	}
	loopCtx, stopLoop := context.WithCancel(ctx)
	loopDone := make(chan error, 1)
	go func() {
		loopDone <- reconcile.RunSuspendedLoop(loopCtx, all[2], "suspended-leader", 100*time.Millisecond, 10)
	}()
	value, err := c.Await(ctx, typ, id)
	if err != nil || string(value) != "42" {
		probeCtx, stopProbe := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopProbe()
		records, _, journalErr := j.Read(probeCtx, typ, id)
		info, runErr := run.Info(probeCtx)
		var loopErr, workerErr error
		select {
		case loopErr = <-loopDone:
		default:
		}
		select {
		case workerErr = <-done:
		default:
		}
		var last journal.Kind
		if len(records) > 0 {
			last = records[len(records)-1].Kind
		}
		t.Fatalf("result=%s err=%v journal_entries=%d journal_tail=%s journal_err=%v run_info=%+v run_err=%v loop_err=%v worker_err=%v", value, err, len(records), last, journalErr, info, runErr, loopErr, workerErr)
	}
	stopLoop()
	if err := <-loopDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if result, err := scan.Scan(ctx, 0, 10, true); err != nil || result.Reenqueued != 0 {
		t.Fatalf("terminal scan=%+v err=%v", result, err)
	}
}

func TestSuspendedScanRepairsOverdueTimer(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	const typ, id = "test", "suspended-timer"
	c := client.New(all[0])
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	run, err := all[0].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	if err := run.Purge(ctx); err != nil {
		t.Fatal(err)
	}
	j := journal.New(all[0])
	seq, err := j.Append(ctx, typ, id, journal.Entry{Index: 0, Epoch: 0, Kind: journal.Started}, 0)
	if err != nil {
		t.Fatal(err)
	}
	request, _ := json.Marshal(struct {
		Kind          string    `json:"kind"`
		Name          string    `json:"name"`
		DurationNanos int64     `json:"duration_nanos"`
		FireAt        time.Time `json:"fire_at"`
	}{Kind: "timer", Name: "short", DurationNanos: int64(time.Second), FireAt: time.Now().Add(-time.Minute)})
	seq, err = j.Append(ctx, typ, id, journal.Entry{Index: 1, Epoch: 0, Kind: journal.StepRequested, Payload: request}, seq)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.Append(ctx, typ, id, journal.Entry{Index: 2, Epoch: 0, Kind: journal.Suspended, Payload: json.RawMessage(`{"waiting_on":"timer:short"}`)}, seq); err != nil {
		t.Fatal(err)
	}
	scan := reconcile.NewSuspendedScan(all[2])
	result, err := scan.Scan(ctx, 0, 10, false)
	if err != nil || result.Reenqueued != 1 || result.Candidates[0].Reason != "timer" {
		t.Fatalf("timer scan=%+v err=%v", result, err)
	}
	if result, err := scan.Scan(ctx, 0, 10, false); err != nil || result.Reenqueued != 1 {
		t.Fatalf("repeated timer scan=%+v err=%v", result, err)
	}
	info, err := run.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("duplicate reconciliation wakeup: state=%+v err=%v", info.State, err)
	}
	w, err := worker.New(ctx, all[1], "suspended-timer-worker", map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		if err := wf.Sleep(c, "short", time.Second); err != nil {
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
	value, err := c.Await(ctx, typ, id)
	if err != nil || string(value) != "true" {
		t.Fatalf("result=%s err=%v", value, err)
	}
	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestSuspendedScanBudgetsPurgedSequenceHoles(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const typ, id = "test", "after-hole"
	if _, err := all[0].Publish(ctx, identity.InvocationSubject(typ, "hole"), []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	inv, err := all[0].Stream(ctx, "WF_INV")
	if err != nil {
		t.Fatal(err)
	}
	if err := inv.Purge(ctx, jetstream.WithPurgeSubject(identity.InvocationSubject(typ, "hole"))); err != nil {
		t.Fatal(err)
	}
	ack, err := all[0].Publish(ctx, identity.InvocationSubject(typ, id), []byte(`null`))
	if err != nil {
		t.Fatal(err)
	}
	j := journal.New(all[0])
	seq, err := j.Append(ctx, typ, id, journal.Entry{Index: 0, Epoch: 0, Kind: journal.Started}, 0)
	if err != nil {
		t.Fatal(err)
	}
	seq, err = j.Append(ctx, typ, id, journal.Entry{Index: 1, Epoch: 0, Kind: journal.StepRequested, Payload: json.RawMessage(`{"kind":"signal","name":"go"}`)}, seq)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.Append(ctx, typ, id, journal.Entry{Index: 2, Epoch: 0, Kind: journal.Suspended, Payload: json.RawMessage(`{"waiting_on":"signal:go"}`)}, seq); err != nil {
		t.Fatal(err)
	}
	if _, err := all[0].Publish(ctx, "wf.sig.test.after-hole.noise", []byte(`0`)); err != nil {
		t.Fatal(err)
	}
	signals, err := all[0].Stream(ctx, "WF_SIG")
	if err != nil {
		t.Fatal(err)
	}
	if err := signals.Purge(ctx, jetstream.WithPurgeSubject("wf.sig.test.after-hole.noise")); err != nil {
		t.Fatal(err)
	}
	message := &nats.Msg{Subject: "wf.sig.test.after-hole.go", Data: []byte(`true`), Header: nats.Header{}}
	message.Header.Set("Wf-Inv-Seq", strconv.FormatUint(ack.Sequence, 10))
	signalAck, err := all[0].PublishMsg(ctx, message)
	if err != nil {
		t.Fatal(err)
	}
	scan := reconcile.NewSuspendedScan(all[2])
	first, err := scan.Scan(ctx, 1, 1, true)
	if err != nil || first.NextSequence != ack.Sequence || first.Inspected != 0 || first.Reenqueued != 0 {
		t.Fatalf("hole page=%+v err=%v", first, err)
	}
	for name, scanFn := range map[string]func(context.Context, uint64, int, bool) (reconcile.ScanResult, error){
		"start": reconcile.NewStartScan(all[2]).Scan,
		"timer": reconcile.NewTimerScan(all[2]).Scan,
	} {
		page, err := scanFn(ctx, 1, 1, true)
		if err != nil || page.NextSequence != ack.Sequence || page.Inspected != 0 {
			t.Fatalf("%s hole page=%+v err=%v", name, page, err)
		}
	}
	signalPage, err := reconcile.NewSignalScan(all[2]).Scan(ctx, 1, 1, true)
	if err != nil || signalPage.NextSequence != signalAck.Sequence || signalPage.Inspected != 0 {
		t.Fatalf("signal hole page=%+v err=%v", signalPage, err)
	}
	second, err := scan.Scan(ctx, first.NextSequence, 1, true)
	if err != nil || second.Inspected != 1 || second.Reenqueued != 1 {
		t.Fatalf("retained page=%+v err=%v", second, err)
	}
}
