package integration_test

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/reconcile"
	"js-wf/wf"
	"js-wf/worker"
)

// Inject only a native hint's clock-bound lookup failure. Journal, lease,
// consumer ACK, scanner wakeup and result operations use actual three-node NATS.
func TestNativeTimerHintFailureRepairsFromDurableSuspension(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const typ, id = "hintfailure", "one"
	const domain = "utc-quorum-v1"
	var boundsCalls atomic.Int32
	acked := make(chan worker.DispatchEvent, 4)
	hints := make(chan worker.OperationEvent, 4)
	handler := func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		if err := wf.Sleep(c, "wait", 2*time.Second); err != nil {
			return nil, err
		}
		return json.RawMessage(`42`), nil
	}
	bounds := func(context.Context) (time.Time, time.Time, error) {
		now := time.Now().UTC()
		if boundsCalls.Add(1) == 2 {
			return time.Time{}, time.Time{}, context.DeadlineExceeded
		}
		return now, now, nil
	}
	first, err := worker.New(ctx, all[0], "hint-first", map[string]worker.Handler{typ: handler}, worker.WithTimerClock(domain, bounds),
		worker.WithDispatchObserver(func(e worker.DispatchEvent) {
			if e.Stage == "ack" || e.Stage == "execution_retry" {
				acked <- e
			}
		}),
		worker.WithOperationObserver(func(e worker.OperationEvent) {
			if e.Operation == "timer_native_hint" {
				hints <- e
			}
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	partition := identity.Partition(typ, id, provision.Partitions)
	firstCtx, stopFirst := context.WithCancel(ctx)
	defer stopFirst()
	firstDone := make(chan error, 1)
	go func() { firstDone <- first.RunPartition(firstCtx, partition) }()
	c := client.New(all[1])
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-acked:
		if event.Stage != "ack" || event.Error != "" {
			t.Fatalf("hint failure did not durably suspend before ACK: %+v", event)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	stopFirst()
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case event := <-hints:
		if event.Error != "context deadline exceeded" || event.TimerPublished == nil || *event.TimerPublished {
			t.Fatalf("missing failed native hint: %+v", event)
		}
	default:
		t.Fatal("missing native hint observation")
	}
	records, _, err := journal.New(all[1]).Read(ctx, typ, id)
	if err != nil || len(records) != 3 || records[2].Kind != journal.Suspended {
		t.Fatalf("durable suspension=%+v err=%v", records, err)
	}
	var request struct {
		FireAt      time.Time `json:"fire_at"`
		ClockDomain string    `json:"clock_domain"`
	}
	if err := json.Unmarshal(records[1].Payload, &request); err != nil || request.ClockDomain != domain || request.FireAt.IsZero() {
		t.Fatalf("request=%+v err=%v", request, err)
	}
	scan := reconcile.NewSuspendedScan(all[2])
	scan.Grace = 0
	scan.DomainNow = func(_ context.Context, d string) (time.Time, error) {
		if d != domain {
			t.Fatalf("domain=%s", d)
		}
		return time.Now().UTC(), nil
	}
	for {
		before := time.Now().UTC()
		result, err := scan.Scan(ctx, 1, 1, false)
		if err != nil {
			t.Fatal(err)
		}
		if result.Reenqueued == 1 {
			if time.Now().UTC().Before(request.FireAt) {
				t.Fatal("repair wakeup before recorded deadline")
			}
			break
		}
		if result.Reenqueued != 0 {
			t.Fatalf("repair count=%+v", result)
		}
		if before.After(request.FireAt.Add(5 * time.Second)) {
			t.Fatal("due repair exceeded scan budget")
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}
	second, err := worker.New(ctx, all[2], "hint-second", map[string]worker.Handler{typ: handler}, worker.WithTimerClock(domain, func(context.Context) (time.Time, time.Time, error) { now := time.Now().UTC(); return now, now, nil }))
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	secondCtx, stopSecond := context.WithCancel(ctx)
	defer stopSecond()
	secondDone := make(chan error, 1)
	go func() { secondDone <- second.RunPartition(secondCtx, partition) }()
	result, err := c.Await(ctx, typ, id)
	if err != nil || string(result) != "42" {
		t.Fatalf("result=%s err=%v", result, err)
	}
	if time.Now().UTC().Before(request.FireAt) {
		t.Fatal("timer completed early")
	}
	for {
		run, err := all[1].Stream(ctx, "WF_RUN")
		if err != nil {
			t.Fatal(err)
		}
		info, err := run.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if info.State.Msgs == 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}
	stopSecond()
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	records, _, err = journal.New(all[0]).Read(ctx, typ, id)
	if err != nil || len(records) != 5 || records[3].Kind != journal.StepCompleted || records[4].Kind != journal.Completed {
		t.Fatalf("final journal=%+v err=%v", records, err)
	}
	report, err := integrity.Check(ctx, all[1])
	if err != nil || report != (integrity.Report{Invocations: 1, Journals: 1, Entries: 5, Terminal: 1}) {
		t.Fatalf("integrity=%+v err=%v", report, err)
	}
	t.Log("NATIVE_HINT_FAILURE durable_suspension=true deadline_preserved=true repair_wakeup=1 journal_entries=5 logical_run_drain=true")
}
