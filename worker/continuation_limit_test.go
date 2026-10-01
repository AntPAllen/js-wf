package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/testcluster"
	"js-wf/wf"

	"github.com/nats-io/nats.go/jetstream"
)

type continuationLimitFramePort struct {
	journal.SnapshotWritePort
	frames, archives int
}

func (p *continuationLimitFramePort) GetObject(ctx context.Context, name string) ([]byte, error) {
	if strings.HasPrefix(name, "snapshot-") {
		p.archives++
		return nil, fmt.Errorf("archive read forbidden")
	}
	p.frames++
	return p.SnapshotWritePort.GetObject(ctx, name)
}

func TestContinuationPreservesGlobalJournalLimitAndTerminalSlot(t *testing.T) {
	cluster, err := testcluster.Start(t.TempDir(), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	var all []jetstream.JetStream
	for _, nc := range cluster.Clients {
		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, js)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	for {
		attempt, stop := context.WithTimeout(ctx, 2*time.Second)
		err := provision.Ensure(attempt, all[0], 3)
		stop()
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("provision: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	const typ, id = "checkpoint-limit", "global"
	var initialCalls, middleCalls, effects atomic.Int64
	handlers := map[string]Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		initialCalls.Add(1)
		if err := c.SetState("value", 10); err != nil {
			return nil, err
		}
		return nil, wf.Continue(c, "middle_v1", 10)
	}}
	stages := map[string]ContinuationHandler{
		"middle_v1": func(c *wf.Context, _, locals json.RawMessage) (json.RawMessage, error) {
			middleCalls.Add(1)
			var value int
			found, err := c.GetState("value", &value)
			if err != nil {
				return nil, err
			}
			if !found || value != 10 || string(locals) != "10" {
				return nil, fmt.Errorf("bad middle state")
			}
			return nil, wf.Continue(c, "finish_v1", 10)
		},
		"finish_v1": func(c *wf.Context, _, _ json.RawMessage) (json.RawMessage, error) {
			if _, err := wf.AwaitSignal(c, "gate"); err != nil {
				return nil, err
			}
			_, err := wf.Run(c, "must_not_run", 10, func(context.Context) (int, error) { effects.Add(1); return 20, nil })
			return json.RawMessage(`20`), err
		},
	}
	first, err := New(ctx, all[1], "limit-first", handlers, WithContinuations(typ, stages))
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	// The existing worker limit fixtures lower this private budget. The default
	// and hard journal cap remain 100,000; no production configuration is added.
	first.maxEntries = 16
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
				if len(records) != 13 || last.Index != 12 {
					t.Fatalf("pre-resume entries=%d index=%d", len(records), last.Index)
				}
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
	view, err := store.ReadCheckpoint(ctx, typ, id, handle.InvSeq)
	if err != nil || view == nil {
		t.Fatalf("view=%+v err=%v", view, err)
	}
	if view.Anchor.Index != 9 || view.Snapshot.Runtime.StepPosition != 8 {
		t.Fatalf("saved indices=%+v", view.Snapshot.Runtime)
	}
	initialBefore, middleBefore := initialCalls.Load(), middleCalls.Load()
	if initialBefore < 1 || middleBefore < 1 {
		t.Fatal("missing prefix stage entry")
	}
	guard := &continuationLimitFramePort{SnapshotWritePort: journal.NewSnapshotPort(all[2])}
	second, err := New(ctx, all[2], "limit-second", handlers, WithContinuations(typ, stages), WithJournalStore(journal.NewWithJetStreamSnapshotPort(all[2], guard)))
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	second.maxEntries = 16
	secondCtx, stopSecond := context.WithCancel(ctx)
	defer stopSecond()
	secondDone := make(chan error, 1)
	go func() { secondDone <- second.RunPartition(secondCtx, part) }()
	if _, err := c.Signal(ctx, typ, id, "gate", []byte(`true`), "gate-key"); err != nil {
		t.Fatal(err)
	}
	_, resultErr := c.Await(ctx, typ, id)
	stopSecond()
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	if resultErr == nil || resultErr.Error() != journal.ErrTooLong.Error() {
		t.Fatalf("wrong failure: %v", resultErr)
	}
	if effects.Load() != 0 || initialCalls.Load() != initialBefore || middleCalls.Load() != middleBefore || guard.archives != 0 || guard.frames == 0 {
		t.Fatalf("effects=%d calls=%d/%d reads=%d/%d", effects.Load(), initialCalls.Load(), middleCalls.Load(), guard.archives, guard.frames)
	}
	records, _, err := store.Read(ctx, typ, id)
	if err != nil || len(records) != 16 || records[15].Index != 15 || records[15].Kind != journal.Failed {
		t.Fatalf("records=%d err=%v", len(records), err)
	}
	if records[13].Kind != journal.SignalConsumed || records[14].Kind != journal.StepCompleted {
		t.Fatal("enabling signal completion did not fit before reserved failure")
	}
	var outcome wf.Outcome
	if err := json.Unmarshal(records[15].Payload, &outcome); err != nil || outcome.InvSeq != handle.InvSeq || outcome.Error != journal.ErrTooLong.Error() || outcome.LimitEntry != nil {
		t.Fatalf("outcome=%+v err=%v", outcome, err)
	}
	var request struct{ Kind, Name, InputHash string }
	if err := json.Unmarshal(outcome.LimitRequest, &request); err != nil || request.Kind != "run" || request.Name != "must_not_run" {
		t.Fatalf("rejected=%+v err=%v", request, err)
	}
	state, err := all[0].KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := state.Get(ctx, identity.Key(typ, id))
	if err != nil || !bytes.Equal(terminal.Value(), records[15].Payload) {
		t.Fatalf("state differs: %v", err)
	}
	for _, peer := range all {
		_, err := client.New(peer).Await(ctx, typ, id)
		if err == nil || err.Error() != resultErr.Error() {
			t.Fatalf("peer failure changed: %v", err)
		}
	}
	report, err := integrity.Check(ctx, all[0])
	if err != nil || report.Invocations != 1 || report.Terminal != 1 {
		t.Fatalf("audit=%+v err=%v", report, err)
	}
	objects, err := all[0].ObjectStore(ctx, "WF_BLOB")
	if err != nil {
		t.Fatal(err)
	}
	replayObjects := map[string][]byte{}
	for _, r := range records {
		if r.Kind != journal.StepCompleted {
			continue
		}
		var done struct {
			Ref string `json:"result_ref"`
		}
		if err := json.Unmarshal(r.Payload, &done); err != nil {
			t.Fatal(err)
		}
		if done.Ref != "" {
			raw, err := objects.GetBytes(ctx, done.Ref)
			if err != nil {
				t.Fatal(err)
			}
			replayObjects[done.Ref] = raw
		}
	}
	// Match the CLI's explicit limit-audit protocol: replace Failed with its
	// retained rejected request and prove replay stops at the pending effect.
	replayRecords := append([]journal.Record(nil), records...)
	replayRecords[15].Kind = journal.StepRequested
	replayRecords[15].Payload = outcome.LimitRequest
	replay := func(history []journal.Record) (wf.ReplayObservation, error) {
		raw, err := json.Marshal(history)
		if err != nil {
			return wf.ReplayObservation{}, err
		}
		var obs wf.ReplayObservation
		_, err = wf.ReplayWithContinuations(raw, func(c *wf.Context) (json.RawMessage, error) { return handlers[typ](c, json.RawMessage(`null`)) }, map[string]wf.ReplayContinuation[json.RawMessage]{
			"middle_v1": func(c *wf.Context, locals json.RawMessage) (json.RawMessage, error) {
				return stages["middle_v1"](c, json.RawMessage(`null`), locals)
			},
			"finish_v1": func(c *wf.Context, locals json.RawMessage) (json.RawMessage, error) {
				return stages["finish_v1"](c, json.RawMessage(`null`), locals)
			},
		}, wf.ReplayOptions{Type: typ, ID: id, InvSeq: handle.InvSeq, Objects: replayObjects, Observation: &obs})
		return obs, err
	}
	obs, err := replay(replayRecords)
	if !errors.Is(err, wf.ErrReplayPendingStep) || obs.Continuations != 2 || obs.PlayedSteps != obs.RecordedSteps || effects.Load() != 0 {
		t.Fatalf("limit replay=%+v err=%v effects=%d", obs, err, effects.Load())
	}
	changed := append([]journal.Record(nil), replayRecords...)
	changed[15].Payload = json.RawMessage(`{"kind":"run","name":"wrong","input_hash":"wrong"}`)
	if _, err := replay(changed); !errors.Is(err, wf.ErrNonDeterministic) || effects.Load() != 0 {
		t.Fatalf("changed limit request accepted: %v effects=%d", err, effects.Load())
	}
	t.Logf("continuation global limit: budget=%d logical_entries=%d anchor=%d sdk_offset=%d effects=%d archive_reads=%d frame_reads=%d offline_steps=%d continuations=%d initial_before=%d middle_before=%d", second.maxEntries, len(records), view.Anchor.Index, view.Snapshot.Runtime.StepPosition, effects.Load(), guard.archives, guard.frames, obs.PlayedSteps, obs.Continuations, initialBefore, middleBefore)
}
