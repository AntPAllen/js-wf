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

func TestContinuationPanicBudgetSurvivesCheckpointsAndRestart(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	const typ, id = "checkpoint-panic", "budget"
	var initialCalls, middleCalls, finishCalls atomic.Int64
	handlers := map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		if initialCalls.Add(1) == 1 {
			panic("initial panic")
		}
		if err := c.SetState("value", 10); err != nil {
			return nil, err
		}
		return nil, wf.Continue(c, "middle_v1", 10)
	}}
	stages := map[string]worker.ContinuationHandler{
		"middle_v1": func(c *wf.Context, _, locals json.RawMessage) (json.RawMessage, error) {
			call := middleCalls.Add(1)
			var value int
			if found, err := c.GetState("value", &value); err != nil || !found || value != 10 || string(locals) != "10" {
				if err != nil {
					return nil, err
				}
				panic("bad middle state")
			}
			if call == 1 {
				panic("middle panic")
			}
			if err := c.SetState("value", 20); err != nil {
				return nil, err
			}
			return nil, wf.Continue(c, "finish_v1", 20)
		},
		"finish_v1": func(c *wf.Context, _, locals json.RawMessage) (json.RawMessage, error) {
			finishCalls.Add(1)
			var value int
			if found, err := c.GetState("value", &value); err != nil || !found || value != 20 || string(locals) != "20" {
				if err != nil {
					return nil, err
				}
				panic("bad finish state")
			}
			if _, err := wf.AwaitSignal(c, "gate"); err != nil {
				return nil, err
			}
			panic("final poison")
		},
	}
	first, err := worker.New(ctx, all[1], "checkpoint-panic-first", handlers, worker.WithContinuations(typ, stages), worker.WithMaxPanicAttempts(3))
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	c := client.New(all[0])
	handle, err := c.Start(ctx, typ, id, []byte(`null`))
	if err != nil {
		t.Fatal(err)
	}
	partition := identity.Partition(typ, id, provision.Partitions)
	firstCtx, stopFirst := context.WithCancel(ctx)
	defer stopFirst()
	firstDone := make(chan error, 1)
	go func() { firstDone <- first.RunPartition(firstCtx, partition) }()
	store := journal.New(all[0])
	for {
		records, _, err := store.Read(ctx, typ, id)
		if err != nil {
			t.Fatal(err)
		}
		if len(records) > 0 {
			last := records[len(records)-1]
			if last.Kind == journal.Failed {
				t.Fatalf("failed before final stage: %s", last.Payload)
			}
			if last.Kind == journal.Suspended && string(last.Payload) == `{"waiting_on":"signal:gate"}` {
				break
			}
		}
		if ctx.Err() != nil {
			t.Fatal(ctx.Err())
		}
		time.Sleep(20 * time.Millisecond)
	}
	stopFirst()
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if initialCalls.Load() != 2 || middleCalls.Load() != 2 || finishCalls.Load() < 1 {
		t.Fatalf("stage calls=%d/%d/%d", initialCalls.Load(), middleCalls.Load(), finishCalls.Load())
	}
	finishBefore := finishCalls.Load()
	view, err := store.ReadCheckpoint(ctx, typ, id, handle.InvSeq)
	if err != nil || view == nil {
		t.Fatalf("checkpoint=%+v err=%v", view, err)
	}
	runtime := view.Snapshot.Runtime
	_, info, err := wf.NewCheckpointContext(ctx, nil, nil, view.Frame, wf.CheckpointLocation{Type: typ, ID: id, InvSeq: handle.InvSeq, Index: runtime.Index, Epoch: runtime.Epoch, Hash: runtime.SHA256})
	if err != nil || info.PanicAttempts != 2 || info.Stage != "finish_v1" {
		t.Fatalf("restored facts=%+v err=%v", info, err)
	}
	guard := &checkpointNoArchivePort{SnapshotWritePort: journal.NewSnapshotPort(all[2])}
	second, err := worker.New(ctx, all[2], "checkpoint-panic-second", handlers, worker.WithContinuations(typ, stages), worker.WithMaxPanicAttempts(3), worker.WithJournalStore(journal.NewWithJetStreamSnapshotPort(all[2], guard)))
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	secondCtx, stopSecond := context.WithCancel(ctx)
	defer stopSecond()
	secondDone := make(chan error, 1)
	go func() { secondDone <- second.RunPartition(secondCtx, partition) }()
	if _, err := c.Signal(ctx, typ, id, "gate", []byte(`true`), "final-gate"); err != nil {
		t.Fatal(err)
	}
	_, resultErr := c.Await(ctx, typ, id)
	stopSecond()
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	if resultErr == nil || resultErr.Error() != "workflow panic: final poison" {
		t.Fatalf("wrong terminal failure: %v", resultErr)
	}
	if initialCalls.Load() != 2 || middleCalls.Load() != 2 || finishCalls.Load() != finishBefore+1 || guard.archives != 0 || guard.frames == 0 {
		t.Fatalf("budget reset or prefix replay: calls=%d/%d/%d archive=%d frame=%d", initialCalls.Load(), middleCalls.Load(), finishCalls.Load(), guard.archives, guard.frames)
	}
	records, _, err := store.Read(ctx, typ, id)
	if err != nil {
		t.Fatal(err)
	}
	attempts := 0
	for _, record := range records {
		if record.Kind == journal.Attempt {
			attempt, err := journal.DecodeAttempt(record.Payload)
			attempts++
			if err != nil || attempt.Count != attempts {
				t.Fatalf("panic count=%+v expected=%d err=%v", attempt, attempts, err)
			}
		}
	}
	if attempts != 3 || records[len(records)-1].Kind != journal.Failed {
		t.Fatalf("attempts=%d terminal=%s", attempts, records[len(records)-1].Kind)
	}
	for _, peer := range all {
		_, err := client.New(peer).Await(ctx, typ, id)
		if err == nil || err.Error() != resultErr.Error() {
			t.Fatalf("peer failure changed: %v", err)
		}
	}
	report, err := integrity.Check(ctx, all[0])
	if err != nil || report.Invocations != 1 || report.Terminal != 1 {
		t.Fatalf("integrity=%+v err=%v", report, err)
	}
	if !strings.Contains(resultErr.Error(), "final poison") {
		t.Fatal(resultErr)
	}
	t.Logf("continuation panic budget: attempts=%d checkpoint_baseline=%d initial=%d middle=%d finish_before=%d finish_after=%d archive_reads=%d frame_reads=%d", attempts, info.PanicAttempts, initialCalls.Load(), middleCalls.Load(), finishBefore, finishCalls.Load(), guard.archives, guard.frames)
}
