package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

func TestContinuationCanceledTimerDoesNotReenterStageAndVersionsReplay(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	const typ, id = "checkpoint-timer", "canceled"
	var initialCalls, stageCalls atomic.Int64
	var stageMax atomic.Int32
	stageMax.Store(2)
	handlers := map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		initialCalls.Add(1)
		version, err := wf.Version(c, "prefix-choice", 1, 2)
		if err != nil {
			return nil, err
		}
		timer, err := c.Timer("canceled-before-frame", 12*time.Second)
		if err != nil {
			return nil, err
		}
		if err := timer.Cancel(); err != nil {
			return nil, err
		}
		if err := timer.Await(); !errors.Is(err, wf.ErrTimerCancelled) {
			return nil, fmt.Errorf("canceled timer await: %v", err)
		}
		return nil, wf.Continue(c, "finish_v1", version)
	}}
	stages := map[string]worker.ContinuationHandler{"finish_v1": func(c *wf.Context, _, locals json.RawMessage) (json.RawMessage, error) {
		stageCalls.Add(1)
		version, err := wf.Version(c, "suffix-choice", 1, int(stageMax.Load()))
		if err != nil {
			return nil, err
		}
		if version != 2 || string(locals) != "2" {
			return nil, fmt.Errorf("version drift: locals=%s version=%d", locals, version)
		}
		if _, err := wf.AwaitSignal(c, "gate"); err != nil {
			return nil, err
		}
		return json.RawMessage(`2`), nil
	}}
	first, err := worker.New(ctx, all[1], "timer-first", handlers, worker.WithContinuations(typ, stages))
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	c := client.New(all[0])
	handle, err := c.Start(ctx, typ, id, []byte(`null`))
	if err != nil {
		t.Fatal(err)
	}
	part := identity.Partition(typ, id, provision.Partitions)
	firstCtx, stopFirst := context.WithCancel(ctx)
	defer stopFirst()
	firstDone := make(chan error, 1)
	go func() { firstDone <- first.RunPartition(firstCtx, part) }()
	store := journal.New(all[0])
	var before []journal.Record
	for {
		records, _, err := store.Read(ctx, typ, id)
		if err != nil {
			t.Fatal(err)
		}
		if len(records) > 0 {
			last := records[len(records)-1]
			if last.Kind == journal.Failed {
				t.Fatalf("failed before gate: %s", last.Payload)
			}
			if last.Kind == journal.Suspended && string(last.Payload) == `{"waiting_on":"signal:gate"}` {
				before = records
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
	initialBefore, stageBefore := initialCalls.Load(), stageCalls.Load()
	var fireAt time.Time
	for _, record := range before {
		if record.Kind == journal.StepRequested {
			var req struct {
				Kind   string    `json:"kind"`
				FireAt time.Time `json:"fire_at"`
			}
			if err := json.Unmarshal(record.Payload, &req); err != nil {
				t.Fatal(err)
			}
			if req.Kind == "timer_start" {
				fireAt = req.FireAt
			}
		}
	}
	if fireAt.IsZero() || !fireAt.After(time.Now()) {
		t.Fatalf("did not stop before native deadline: %v", fireAt)
	}
	view, err := store.ReadCheckpoint(ctx, typ, id, handle.InvSeq)
	if err != nil || view == nil {
		t.Fatalf("frame=%+v err=%v", view, err)
	}
	runtime := view.Snapshot.Runtime
	_, info, err := wf.NewCheckpointContext(ctx, nil, nil, view.Frame, wf.CheckpointLocation{Type: typ, ID: id, InvSeq: handle.InvSeq, Index: runtime.Index, Epoch: runtime.Epoch, Hash: runtime.SHA256})
	if err != nil || len(info.CancelledTimers) != 1 || info.CancelledTimers[0] != 2 || info.Stage != "finish_v1" {
		t.Fatalf("canceled frame facts=%+v err=%v", info, err)
	}
	for _, record := range view.Records {
		if record.Kind == journal.StepRequested {
			var req struct {
				Kind string `json:"kind"`
			}
			_ = json.Unmarshal(record.Payload, &req)
			if req.Kind == "timer_cancel" {
				t.Fatal("cancellation must be archived before retained suffix")
			}
		}
	}
	stageMax.Store(3)
	guard := &checkpointNoArchivePort{SnapshotWritePort: journal.NewSnapshotPort(all[2])}
	second, err := worker.New(ctx, all[2], "timer-second", handlers, worker.WithContinuations(typ, stages), worker.WithJournalStore(journal.NewWithJetStreamSnapshotPort(all[2], guard)))
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	secondCtx, stopSecond := context.WithCancel(ctx)
	defer stopSecond()
	secondDone := make(chan error, 1)
	go func() { secondDone <- second.RunPartition(secondCtx, part) }()
	for second.Metrics().CancelledTimerNoOps == 0 {
		if ctx.Err() != nil {
			t.Fatal("native canceled wakeup did not arrive")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if initialCalls.Load() != initialBefore || stageCalls.Load() != stageBefore {
		t.Fatalf("canceled wakeup entered handler: initial=%d/%d stage=%d/%d", initialCalls.Load(), initialBefore, stageCalls.Load(), stageBefore)
	}
	after, _, err := store.Read(ctx, typ, id)
	if err != nil {
		t.Fatal(err)
	}
	beforeBytes, _ := json.Marshal(before)
	afterBytes, _ := json.Marshal(after)
	if !bytes.Equal(beforeBytes, afterBytes) {
		t.Fatal("canceled native wakeup changed logical journal")
	}
	if guard.archives != 0 || guard.frames == 0 {
		t.Fatalf("archive=%d frame=%d", guard.archives, guard.frames)
	}
	if _, err := c.Signal(ctx, typ, id, "gate", []byte(`true`), "gate-key"); err != nil {
		t.Fatal(err)
	}
	result, resultErr := c.Await(ctx, typ, id)
	stopSecond()
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	if resultErr != nil || string(result) != "2" {
		t.Fatalf("version replay result=%s err=%v", result, resultErr)
	}
	metrics := second.Metrics()
	if metrics.CancelledTimerNoOps != 1 || metrics.TimersScheduled != 0 || metrics.TimersFired != 0 || initialCalls.Load() != initialBefore || stageCalls.Load() != stageBefore+1 {
		t.Fatalf("timer/entry metrics=%+v calls=%d/%d", metrics, initialCalls.Load(), stageCalls.Load())
	}
	records, _, err := store.Read(ctx, typ, id)
	if err != nil {
		t.Fatal(err)
	}
	for _, peer := range all {
		value, err := client.New(peer).Await(ctx, typ, id)
		if err != nil || string(value) != "2" {
			t.Fatalf("peer result=%s err=%v", value, err)
		}
	}
	report, err := integrity.Check(ctx, all[0])
	if err != nil || report.Invocations != 1 || report.Terminal != 1 {
		t.Fatalf("audit=%+v err=%v", report, err)
	}
	objects := map[string][]byte{runtime.Object: view.Frame}
	history, err := json.Marshal(records)
	if err != nil {
		t.Fatal(err)
	}
	var obs wf.ReplayObservation
	replayed, err := wf.ReplayWithContinuations(history, func(c *wf.Context) (json.RawMessage, error) { return handlers[typ](c, json.RawMessage(`null`)) }, map[string]wf.ReplayContinuation[json.RawMessage]{"finish_v1": func(c *wf.Context, locals json.RawMessage) (json.RawMessage, error) {
		return stages["finish_v1"](c, json.RawMessage(`null`), locals)
	}}, wf.ReplayOptions{Type: typ, ID: id, InvSeq: handle.InvSeq, Objects: objects, Observation: &obs})
	if err != nil || string(replayed) != "2" || obs.Continuations != 1 || obs.PlayedSteps != obs.RecordedSteps {
		t.Fatalf("offline=%s observation=%+v err=%v", replayed, obs, err)
	}
	t.Logf("continuation canceled timer: step=%d frame_offset=%d native_noops=%d archive_reads=%d frame_reads=%d stage_before=%d stage_after_wakeup=%d stage_after_gate=%d version_max=%d result=%s offline_steps=%d", info.CancelledTimers[0], runtime.StepPosition, metrics.CancelledTimerNoOps, guard.archives, guard.frames, stageBefore, stageBefore, stageBefore+1, stageMax.Load(), result, obs.PlayedSteps)
}
