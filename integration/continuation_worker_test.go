package integration_test

import (
	"context"
	"encoding/json"
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

func TestContinuationWorkerRestartsWithoutPrefixReads(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	const typ, id = "checkpoint", "worker-restart"
	c := client.New(all[0])
	handle, err := c.Start(ctx, typ, id, []byte(`23`))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := c.Signal(ctx, typ, id, "buffered", []byte(fmt.Sprintf("%d", i+1)), fmt.Sprintf("buffered-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	var initialCalls, middleCalls, effects atomic.Int64
	var prefixKey, suffixKey string
	handlers := map[string]worker.Handler{typ: func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
		initialCalls.Add(1)
		for i := 0; i < 1000; i++ {
			if err := c.SetState("total", i); err != nil {
				return nil, err
			}
		}
		if _, err := wf.RunOnce(c, "prefix", 23, func(_ context.Context, key string) (int, error) { effects.Add(1); prefixKey = key; return 23, nil }); err != nil {
			return nil, err
		}
		return nil, wf.Continue(c, "middle_v1", 45)
	}}
	stages := map[string]worker.ContinuationHandler{
		"middle_v1": func(c *wf.Context, input, locals json.RawMessage) (json.RawMessage, error) {
			middleCalls.Add(1)
			if string(input) != "23" || string(locals) != "45" {
				return nil, fmt.Errorf("wrong continuation inputs")
			}
			value, err := wf.AwaitSignal(c, "buffered")
			if err != nil {
				return nil, err
			}
			if string(value) != "1" {
				return nil, fmt.Errorf("first buffered signal %q", value)
			}
			if err := c.SetState("total", 50); err != nil {
				return nil, err
			}
			if _, err := wf.RunOnce(c, "suffix", 23, func(_ context.Context, key string) (int, error) { effects.Add(1); suffixKey = key; return 46, nil }); err != nil {
				return nil, err
			}
			return nil, wf.Continue(c, "finish_v1", 67)
		},
		"finish_v1": func(c *wf.Context, input, locals json.RawMessage) (json.RawMessage, error) {
			var total int
			if string(input) != "23" || string(locals) != "67" {
				return nil, fmt.Errorf("wrong final inputs")
			}
			if found, err := c.GetState("total", &total); err != nil || !found || total != 50 {
				return nil, fmt.Errorf("restored total %d: %v", total, err)
			}
			value, err := wf.AwaitSignal(c, "buffered")
			if err != nil {
				return nil, err
			}
			if string(value) != "2" {
				return nil, fmt.Errorf("second buffered signal %q", value)
			}
			if _, err := wf.AwaitSignal(c, "gate"); err != nil {
				return nil, err
			}
			return json.RawMessage(`50`), nil
		},
	}
	first, err := worker.New(ctx, all[1], "checkpoint-first", handlers, worker.WithContinuations(typ, stages))
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	firstCtx, stopFirst := context.WithCancel(ctx)
	defer stopFirst()
	firstDone := make(chan error, 1)
	partition := identity.Partition(typ, id, provision.Partitions)
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
				t.Fatalf("workflow failed: %s", last.Payload)
			}
			if last.Kind == journal.Suspended && string(last.Payload) == `{"waiting_on":"signal:gate"}` {
				break
			}
		}
		if ctx.Err() != nil {
			t.Fatal(ctx.Err())
		}
		time.Sleep(25 * time.Millisecond)
	}
	stopFirst()
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if initialCalls.Load() != 1 || middleCalls.Load() != 1 || effects.Load() != 2 || prefixKey == "" || suffixKey == prefixKey {
		t.Fatalf("calls initial=%d middle=%d effects=%d keys=%q/%q", initialCalls.Load(), middleCalls.Load(), effects.Load(), prefixKey, suffixKey)
	}
	// An incomplete rollout must reject the retained stage before entering
	// either the initial or middle handler.
	for _, registered := range []bool{false, true} {
		unknownObserved := make(chan struct{}, 1)
		options := []worker.Option{}
		expected := wf.ErrContinuationUnsupported
		if registered {
			expected = wf.ErrUnknownContinuation
			options = append(options, worker.WithContinuations(typ, map[string]worker.ContinuationHandler{"middle_v1": stages["middle_v1"]}))
		}
		options = append(options, worker.WithDispatchObserver(func(event worker.DispatchEvent) {
			if event.Stage == "execution_retry" && event.Error == expected.Error() {
				select {
				case unknownObserved <- struct{}{}:
				default:
				}
			}
		}))
		unknown, err := worker.New(ctx, all[2], fmt.Sprintf("checkpoint-unknown-%t", registered), handlers, options...)
		if err != nil {
			t.Fatal(err)
		}
		unknownCtx, stopUnknown := context.WithCancel(ctx)
		unknownDone := make(chan error, 1)
		go func() { unknownDone <- unknown.RunPartition(unknownCtx, partition) }()
		if err := c.Enqueue(ctx, typ, id, fmt.Sprintf("unknown-stage-probe-%t", registered)); err != nil {
			t.Fatal(err)
		}
		select {
		case <-unknownObserved:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		stopUnknown()
		if err := <-unknownDone; err != nil {
			t.Fatal(err)
		}
		if err := unknown.Close(); err != nil {
			t.Fatal(err)
		}
		if initialCalls.Load() != 1 || middleCalls.Load() != 1 || effects.Load() != 2 {
			t.Fatal("unknown stage executed user code")
		}
	}
	guard := &checkpointNoArchivePort{SnapshotWritePort: journal.NewSnapshotPort(all[2])}
	second, err := worker.New(ctx, all[2], "checkpoint-second", handlers, worker.WithJournalStore(journal.NewWithJetStreamSnapshotPort(all[2], guard)), worker.WithContinuations(typ, stages))
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	secondCtx, stopSecond := context.WithCancel(ctx)
	defer stopSecond()
	secondDone := make(chan error, 1)
	go func() { secondDone <- second.RunPartition(secondCtx, partition) }()
	if _, err := c.Signal(ctx, typ, id, "gate", []byte(`true`), "finish-gate"); err != nil {
		t.Fatal(err)
	}
	result, err := c.Await(ctx, typ, id)
	if err != nil || string(result) != "50" {
		t.Fatalf("result=%s err=%v", result, err)
	}
	stopSecond()
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	if guard.archives != 0 || guard.frames == 0 || initialCalls.Load() != 1 || middleCalls.Load() != 1 || effects.Load() != 2 {
		t.Fatalf("prefix accessed: archive=%d frame=%d initial=%d middle=%d effects=%d", guard.archives, guard.frames, initialCalls.Load(), middleCalls.Load(), effects.Load())
	}
	for _, peer := range all {
		retained, err := client.New(peer).Await(ctx, typ, id)
		if err != nil || string(retained) != "50" {
			t.Fatalf("peer result=%s err=%v", retained, err)
		}
		records, _, err := journal.New(peer).Read(ctx, typ, id)
		if err != nil || len(records) < 2010 || records[len(records)-1].Kind != journal.Completed {
			t.Fatalf("audit records=%d err=%v", len(records), err)
		}
	}
	report, err := integrity.Check(ctx, all[0])
	if err != nil || report.Invocations != 1 || report.Terminal != 1 || report.Entries < 2010 {
		t.Fatalf("retained audit=%+v err=%v", report, err)
	}
	view, err := store.ReadCheckpoint(ctx, typ, id, handle.InvSeq)
	if err != nil || view == nil || view.Snapshot.Runtime.Stage != "finish_v1" {
		t.Fatalf("checkpoint=%+v err=%v", view, err)
	}
	initialBefore, middleBefore := initialCalls.Load(), middleCalls.Load()
	auditRecords, _, err := store.Read(ctx, typ, id)
	if err != nil {
		t.Fatal(err)
	}
	objects, err := all[0].ObjectStore(ctx, "WF_BLOB")
	if err != nil {
		t.Fatal(err)
	}
	replayObjects := make(map[string][]byte)
	for _, record := range auditRecords {
		var references struct {
			Result string `json:"result_ref"`
			Signal string `json:"ref"`
		}
		if len(record.Payload) == 0 {
			continue
		}
		if err := json.Unmarshal(record.Payload, &references); err != nil {
			t.Fatal(err)
		}
		for _, ref := range []string{references.Result, references.Signal} {
			if ref == "" || replayObjects[ref] != nil {
				continue
			}
			data, err := objects.GetBytes(ctx, ref)
			if err != nil {
				t.Fatal(err)
			}
			replayObjects[ref] = data
		}
	}
	journalBytes, err := json.Marshal(auditRecords)
	if err != nil {
		t.Fatal(err)
	}
	replayStages := make(map[string]wf.ReplayContinuation[json.RawMessage])
	for name, stage := range stages {
		replayStages[name] = func(c *wf.Context, locals json.RawMessage) (json.RawMessage, error) {
			return stage(c, json.RawMessage(`23`), locals)
		}
	}
	var observation wf.ReplayObservation
	replayed, err := wf.ReplayWithContinuations(journalBytes, func(c *wf.Context) (json.RawMessage, error) { return handlers[typ](c, json.RawMessage(`23`)) }, replayStages, wf.ReplayOptions{Type: typ, ID: id, InvSeq: handle.InvSeq, Objects: replayObjects, Observation: &observation})
	if err != nil || string(replayed) != "50" || effects.Load() != 2 || initialCalls.Load() != initialBefore+1 || middleCalls.Load() != middleBefore+1 || observation.Continuations != 2 || observation.PlayedSteps != observation.RecordedSteps {
		t.Fatalf("offline result=%s effects=%d observation=%+v err=%v", replayed, effects.Load(), observation, err)
	}
	t.Logf("offline continuation audit: frames=%d boundaries=%d played=%d recorded=%d effects=%d", len(replayObjects), observation.Continuations, observation.PlayedSteps, observation.RecordedSteps, effects.Load())
	t.Logf("prefix-free worker restart: initial=%d middle=%d effects=%d suffix=%d frame_reads=%d archive_reads=%d", initialBefore, middleBefore, effects.Load(), len(view.Records), guard.frames, guard.archives)
}
