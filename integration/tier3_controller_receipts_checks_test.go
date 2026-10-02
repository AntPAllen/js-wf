//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/history"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/wf"
	"js-wf/worker"
)

func TestMatrixControllerJournalReceiptBoundsActualRetainedAppend(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	observer, err := startMatrixControllerReceiptObserver(ctx, all[0])
	if err != nil {
		t.Fatal(err)
	}
	defer observer.consume.Stop()
	entry := journal.Entry{Epoch: 1, Index: 0, Kind: journal.Started, WorkerID: "controller-receipt-owner"}
	before := time.Now()
	sequence, err := journal.New(all[0]).Append(ctx, "controller-receipt", "one", entry, 0)
	if err != nil {
		t.Fatal(err)
	}
	var receipts []matrixControllerJournalReceipt
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		receipts, err = observer.snapshot()
		if err != nil {
			t.Fatal(err)
		}
		if len(receipts) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	records := []journal.Record{{Entry: entry, Sequence: sequence}}
	times, err := matrixControllerReceiptTimes("controller-receipt", "one", records, receipts)
	if err != nil || times[sequence].IsZero() || times[sequence].Before(before) {
		t.Fatalf("receipt times=%v err=%v", times, err)
	}
	observation := worker.OperationEvent{Type: "controller-receipt", ID: "one", Worker: entry.WorkerID, Operation: "journal_append", JournalIndex: entry.Index, JournalKind: entry.Kind, At: time.Now(), Error: journal.ErrUnknown.Error()}
	observation.Duration = observation.At.Sub(before)
	bounds, err := matrixControllerAppendBounds(observation.Type, observation.ID, records, []worker.OperationEvent{observation}, times)
	if err != nil || !bounds[0].After.Equal(times[sequence]) {
		t.Fatalf("unknown append receipt bounds=%+v err=%v", bounds, err)
	}
}

func TestMatrixControllerReceiptRejectsChangedEntryAndDuplicate(t *testing.T) {
	entry := journal.Entry{Epoch: 1, Index: 0, Kind: journal.Started, WorkerID: "owner"}
	record := journal.Record{Entry: entry, Sequence: 7}
	receipt := matrixControllerJournalReceipt{Sequence: 7, Subject: identity.JournalSubject("typ", "id"), Entry: entry, ObservedAt: time.Unix(100, 0)}
	for _, mode := range []string{"changed_epoch", "changed_payload", "duplicate", "missing_time"} {
		t.Run(mode, func(t *testing.T) {
			changed := receipt
			receipts := []matrixControllerJournalReceipt{changed}
			switch mode {
			case "changed_epoch":
				receipts[0].Entry.Epoch = 2
			case "changed_payload":
				receipts[0].Entry.Payload = []byte(`1`)
			case "duplicate":
				receipts = append(receipts, receipt)
			case "missing_time":
				receipts[0].ObservedAt = time.Time{}
			}
			if _, err := matrixControllerReceiptTimes("typ", "id", []journal.Record{record}, receipts); err == nil {
				t.Fatal("invalid receipt accepted")
			}
		})
	}
}

func TestMatrixControllerLatencyAuditActualTimerWorkflow(t *testing.T) {
	runMatrixControllerLatencyAuditActualTimerWorkflow(t, false)
}
func TestMatrixControllerLatencyAuditCanonicalTimerWorkflow(t *testing.T) {
	runMatrixControllerLatencyAuditActualTimerWorkflow(t, true)
}
func runMatrixControllerLatencyAuditActualTimerWorkflow(t *testing.T, common bool) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	observer, err := startMatrixControllerReceiptObserver(ctx, all[0])
	if err != nil {
		t.Fatal(err)
	}
	defer observer.consume.Stop()
	var mu sync.Mutex
	var operations []worker.OperationEvent
	timerCalls := make(map[string]matrixControllerTimerCall)
	recorder := &history.Recorder{}
	c := client.NewObserved(all[0], recorder)
	handler := func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		for i := 0; i < 8; i++ {
			name := fmt.Sprintf("timer-%d", i)
			mu.Lock()
			timer := timerCalls[name]
			if timer.FirstCall.IsZero() {
				timer = matrixControllerTimerCall{ID: "one", Name: name, FirstCall: time.Now().UTC(), Duration: 250 * time.Millisecond}
				timerCalls[name] = timer
			}
			mu.Unlock()
			if err := wf.Sleep(c, name, 250*time.Millisecond); err != nil {
				return nil, err
			}
			mu.Lock()
			timer = timerCalls[name]
			if timer.FirstReturn.IsZero() {
				timer.FirstReturn = time.Now().UTC()
				timerCalls[name] = timer
			}
			mu.Unlock()
		}
		return json.RawMessage(`42`), nil
	}
	var clockOptions []worker.Option
	if common {
		clockOptions = append(clockOptions, worker.WithTimerClock("utc-quorum-v1", func(context.Context) (time.Time, time.Time, error) {
			now := time.Now().UTC()
			return now.Add(-20 * time.Millisecond), now.Add(20 * time.Millisecond), nil
		}))
	}
	clockOptions = append(clockOptions, worker.WithOperationObserver(func(event worker.OperationEvent) {
		if event.Operation == "journal_append" || event.Operation == "timer_clock" || event.Operation == "timer_domain_clock" || event.Operation == "timer_native_hint" {
			mu.Lock()
			operations = append(operations, event)
			mu.Unlock()
		}
	}))
	w, err := worker.New(ctx, all[0], "controller-audit-owner", map[string]worker.Handler{"matrixtimer": handler}, clockOptions...)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	done := make(chan error, 1)
	go func() {
		done <- w.RunPartition(ctx, identity.Partition("matrixtimer", "one", provision.Partitions))
	}()
	defer func() { cancel(); <-done }()
	if _, err := c.Start(ctx, "matrixtimer", "one", []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	result, err := c.Await(ctx, "matrixtimer", "one")
	if err != nil || string(result) != "42" {
		t.Fatalf("result=%s err=%v", result, err)
	}
	receipts, err := observer.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	ops := append([]worker.OperationEvent(nil), operations...)
	var timers []matrixControllerTimerCall
	for _, timer := range timerCalls {
		timers = append(timers, timer)
	}
	mu.Unlock()
	proof, err := auditMatrixControllerLatencies(ctx, all[0], ops, receipts, recorder.Snapshot(), timers, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(proof.Bounds) != 26 || len(proof.Samples) != 10 || proof.Clock != "unshifted-controller-conservative-bounds" {
		t.Fatalf("proof=%+v", proof)
	}
	if proof.Samples[1].Event != "timer_due" || proof.Samples[1].Delay < 0 || proof.Samples[9].ObservedLower == nil {
		t.Fatalf("samples=%+v", proof.Samples)
	}
	if common {
		hints := 0
		for _, op := range ops {
			if op.Operation != "timer_native_hint" || op.Error != "" || op.TimerPublished == nil || !*op.TimerPublished {
				continue
			}
			hints++
			matched := false
			for _, bound := range proof.Bounds {
				if bound.Entry.Index != op.JournalIndex || bound.Entry.Kind != journal.StepRequested {
					continue
				}
				var req struct {
					FireAt time.Time `json:"fire_at"`
					Domain string    `json:"clock_domain"`
				}
				if err := json.Unmarshal(bound.Entry.Payload, &req); err != nil {
					t.Fatal(err)
				}
				if op.TimerDeadline != nil && req.FireAt.Equal(*op.TimerDeadline) && req.Domain == op.ClockDomain {
					matched = true
				}
			}
			if !matched {
				t.Fatalf("native hint does not match durable request index: %+v", op)
			}
		}
		if hints != 8 {
			t.Fatalf("fresh native hint count=%d want8", hints)
		}
	}
	if root := os.Getenv("WF_CONTROLLER_AUDIT_OUT"); root != "" {
		if err := os.MkdirAll(root, 0755); err != nil {
			t.Fatal(err)
		}
		for name, value := range map[string]any{"controller-latency-audit.json": proof, "controller-operations.json": ops, "controller-receipts.json": receipts, "controller-timers.json": timers, "latencies.json": proof.Samples, "controller-client-calls.json": recorder.Snapshot()} {
			data, err := json.MarshalIndent(value, "", "  ")
			if err == nil {
				err = os.WriteFile(filepath.Join(root, name), data, 0644)
			}
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	_, err = auditMatrixControllerLatencies(ctx, all[0], ops, receipts, recorder.Snapshot(), nil, time.Now().Add(time.Minute))
	if err == nil {
		t.Fatal("missing actual timer-call evidence accepted")
	}
}
