package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/retention"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go/jetstream"
)

type continuationPromiseBlobs struct {
	worker.ResultBlobPort
	childObject string
	childReads  int
}

func (p *continuationPromiseBlobs) GetBytes(ctx context.Context, name string) ([]byte, error) {
	if name == p.childObject {
		p.childReads++
	}
	return p.ResultBlobPort.GetBytes(ctx, name)
}

func TestContinuationResolvedPromiseSurvivesRestartAndRetirement(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	const typ, id, childType = "checkpoint-promise", "resolved", "checkpoint-child"
	payload, _ := json.Marshal(strings.Repeat("x", wf.MaxInlineTerminal))
	expectedResult := strconv.Itoa(len(payload))
	var initialCalls, childCalls atomic.Int64
	var promised atomic.Value
	handlers := map[string]worker.Handler{
		typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
			initialCalls.Add(1)
			promise, err := wf.CallAsync(c, childType, []byte(`null`))
			if err != nil {
				return nil, err
			}
			promised.Store(promise)
			result, err := wf.AwaitPromise(c, promise)
			if err != nil {
				return nil, err
			}
			if !bytes.Equal(result, payload) {
				return nil, fmt.Errorf("wrong initial child result")
			}
			return nil, wf.Continue(c, "finish_v1", promise)
		},
		childType: func(_ *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
			childCalls.Add(1)
			return payload, nil
		},
	}
	stages := map[string]worker.ContinuationHandler{"finish_v1": func(c *wf.Context, _, locals json.RawMessage) (json.RawMessage, error) {
		if _, err := wf.AwaitSignal(c, "gate"); err != nil {
			return nil, err
		}
		var promise wf.Promise
		if err := json.Unmarshal(locals, &promise); err != nil {
			return nil, err
		}
		result, err := wf.AwaitPromise(c, promise)
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(result, payload) {
			return nil, fmt.Errorf("wrong restored child result")
		}
		result[0] = '!'
		again, err := wf.AwaitPromise(c, promise)
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(again, payload) {
			return nil, fmt.Errorf("promise result alias or second consumption")
		}
		return json.RawMessage(strconv.Itoa(len(again))), nil
	}}
	first, err := worker.New(ctx, all[1], "promise-first", handlers, worker.WithContinuations(typ, stages))
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	c := client.New(all[0])
	handle, err := c.Start(ctx, typ, id, []byte(`null`))
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stopFirst := context.WithCancel(ctx)
	defer stopFirst()
	var done []chan error
	runPartition := func(part uint32) {
		ch := make(chan error, 1)
		done = append(done, ch)
		go func() { ch <- first.RunPartition(runCtx, part) }()
	}
	parentPart := identity.Partition(typ, id, provision.Partitions)
	runPartition(parentPart)
	for promised.Load() == nil {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		default:
		}
		time.Sleep(20 * time.Millisecond)
	}
	promise := promised.Load().(wf.Promise)
	childPart := identity.Partition(childType, promise.ChildID, provision.Partitions)
	if childPart != parentPart {
		runPartition(childPart)
	}
	store := journal.New(all[0])
	for {
		records, _, err := store.Read(ctx, typ, id)
		if err != nil {
			t.Fatal(err)
		}
		if len(records) > 0 {
			last := records[len(records)-1]
			if last.Kind == journal.Failed {
				t.Fatalf("parent failed: %s", last.Payload)
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
	for _, ch := range done {
		if err := <-ch; err != nil {
			t.Fatal(err)
		}
	}
	initialBefore := initialCalls.Load()
	if childCalls.Load() != 1 {
		t.Fatalf("child calls=%d", childCalls.Load())
	}
	view, err := store.ReadCheckpoint(ctx, typ, id, handle.InvSeq)
	if err != nil || view == nil {
		t.Fatalf("checkpoint=%+v err=%v", view, err)
	}
	var frame struct {
		PromiseOutcomes map[string]json.RawMessage `json:"promise_outcomes"`
	}
	if err := json.Unmarshal(view.Frame, &frame); err != nil {
		t.Fatal(err)
	}
	var childOutcome wf.Outcome
	if err := json.Unmarshal(frame.PromiseOutcomes[promise.SignalName], &childOutcome); err != nil || childOutcome.ResultRef == "" || childOutcome.ResultHash == "" || len(childOutcome.Result) != 0 {
		t.Fatalf("frame outcome=%+v err=%v", childOutcome, err)
	}
	if len(view.Frame) >= len(payload) {
		t.Fatal("derived child cache was serialized")
	}
	// All worker loops are stopped before retirement and every sweep.
	if err := retention.Purge(ctx, all[0], childType, promise.ChildID, time.Minute); !errors.Is(err, retention.ErrNotTerminal) {
		t.Fatalf("child retired before parent terminal: %v", err)
	}
	swept, err := retention.SweepBlobsQuiescent(ctx, all[0], 0, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	objects, err := all[0].ObjectStore(ctx, "WF_BLOB")
	if err != nil {
		t.Fatal(err)
	}
	kept, err := objects.GetBytes(ctx, childOutcome.ResultRef)
	if err != nil || !bytes.Equal(kept, payload) {
		t.Fatalf("child reference collected or changed: %v", err)
	}
	guard := &checkpointNoArchivePort{SnapshotWritePort: journal.NewSnapshotPort(all[2])}
	blobs := &continuationPromiseBlobs{ResultBlobPort: worker.NewResultBlobPort(all[2]), childObject: childOutcome.ResultRef}
	// Count the real child-result transport independently of journal/frame reads.
	second, err := worker.New(ctx, all[2], "promise-second", handlers, worker.WithContinuations(typ, stages), worker.WithJournalStore(journal.NewWithJetStreamSnapshotPort(all[2], guard)), worker.WithResultBlobPort(blobs))
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	secondCtx, stopSecond := context.WithCancel(ctx)
	defer stopSecond()
	secondDone := make(chan error, 1)
	go func() { secondDone <- second.RunPartition(secondCtx, parentPart) }()
	if _, err := c.Signal(ctx, typ, id, "gate", []byte(`true`), "gate-key"); err != nil {
		t.Fatal(err)
	}
	result, resultErr := c.Await(ctx, typ, id)
	stopSecond()
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	if resultErr != nil || string(result) != expectedResult {
		t.Fatalf("result=%s err=%v", result, resultErr)
	}
	if initialCalls.Load() != initialBefore || childCalls.Load() != 1 || guard.archives != 0 || guard.frames == 0 || blobs.childReads != 1 {
		t.Fatalf("calls=%d/%d archive=%d frame=%d child_reads=%d", initialCalls.Load(), childCalls.Load(), guard.archives, guard.frames, blobs.childReads)
	}
	records, _, err := store.Read(ctx, typ, id)
	if err != nil {
		t.Fatal(err)
	}
	calls, consumed, awaits := 0, 0, 0
	for _, record := range records {
		if record.Kind == journal.SignalConsumed {
			var data struct {
				Name string `json:"name"`
			}
			if err := json.Unmarshal(record.Payload, &data); err != nil {
				t.Fatal(err)
			}
			if data.Name == promise.SignalName {
				consumed++
			}
		}
		if record.Kind == journal.StepRequested {
			var req struct{ Kind, Name string }
			if err := json.Unmarshal(record.Payload, &req); err != nil {
				t.Fatal(err)
			}
			if req.Kind == "call_async" {
				calls++
			}
			if req.Name == promise.SignalName {
				awaits++
			}
		}
	}
	// call_async also names the same signal; only one additional await request.
	if calls != 1 || consumed != 1 || awaits != 2 {
		t.Fatalf("call=%d consumed=%d named_requests=%d", calls, consumed, awaits)
	}
	if err := retention.Purge(ctx, all[0], childType, promise.ChildID, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Await(ctx, childType, promise.ChildID); !errors.Is(err, client.ErrPurged) {
		t.Fatalf("child not retired: %v", err)
	}
	if _, err := retention.SweepBlobsQuiescent(ctx, all[0], 0, time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	kept, err = objects.GetBytes(ctx, childOutcome.ResultRef)
	if err != nil || !bytes.Equal(kept, payload) {
		t.Fatalf("parent promise reference collected after child retirement: %v", err)
	}
	for _, peer := range all {
		value, err := client.New(peer).Await(ctx, typ, id)
		if err != nil || string(value) != expectedResult {
			t.Fatalf("peer result=%s err=%v", value, err)
		}
	}
	report, err := integrity.Check(ctx, all[0])
	if err != nil || report.Invocations != 1 || report.Terminal != 1 {
		t.Fatalf("integrity=%+v err=%v", report, err)
	}
	replayObjects := map[string][]byte{childOutcome.ResultRef: kept, view.Snapshot.Runtime.Object: view.Frame}
	history, err := json.Marshal(records)
	if err != nil {
		t.Fatal(err)
	}
	var observation wf.ReplayObservation
	replayed, err := wf.ReplayWithContinuations(history, func(c *wf.Context) (json.RawMessage, error) { return handlers[typ](c, json.RawMessage(`null`)) }, map[string]wf.ReplayContinuation[json.RawMessage]{"finish_v1": func(c *wf.Context, locals json.RawMessage) (json.RawMessage, error) {
		return stages["finish_v1"](c, json.RawMessage(`null`), locals)
	}}, wf.ReplayOptions{Type: typ, ID: id, InvSeq: handle.InvSeq, Objects: replayObjects, Observation: &observation})
	if err != nil || string(replayed) != expectedResult || childCalls.Load() != 1 || observation.Continuations != 1 || observation.PlayedSteps != observation.RecordedSteps {
		t.Fatalf("offline=%s observation=%+v err=%v", replayed, observation, err)
	}
	if err := retention.Purge(ctx, all[0], typ, id, time.Minute); err != nil {
		t.Fatal(err)
	}
	reclaimed, err := retention.SweepBlobsQuiescent(ctx, all[0], 0, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{childOutcome.ResultRef, view.Snapshot.Runtime.Object, view.Snapshot.Object} {
		if _, err := objects.GetInfo(ctx, name); !errors.Is(err, jetstream.ErrObjectNotFound) {
			t.Fatalf("retired object %s survived: %v", name, err)
		}
	}
	t.Logf("continuation promise: child_bytes=%d frame_bytes=%d initial_calls=%d child_calls=%d archive_reads=%d frame_reads=%d child_reads=%d consumed=%d live_sweep_deleted=%d retired_sweep_deleted=%d offline_steps=%d", len(payload), len(view.Frame), initialBefore, childCalls.Load(), guard.archives, guard.frames, blobs.childReads, consumed, swept.Deleted, reclaimed.Deleted, observation.PlayedSteps)
}
