package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
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

	"github.com/nats-io/nats.go"
)

func TestContinuationCancellationInterruptsActiveSuffix(t *testing.T) {
	t.Run("notification", func(t *testing.T) { testContinuationActiveCancellation(t, false) })
	t.Run("missed_notification_durable_poll", func(t *testing.T) { testContinuationActiveCancellation(t, true) })
}

func testContinuationActiveCancellation(t *testing.T, missedNotification bool) {
	t.Helper()
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	const typ, id = "checkpoint-cancel", "active-suffix"
	var initialCalls, stageCalls, effectCalls atomic.Int64
	entered := make(chan context.Context, 1)
	handlers := map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		initialCalls.Add(1)
		if err := c.SetState("value", 23); err != nil {
			return nil, err
		}
		return nil, wf.Continue(c, "finish_v1", 45)
	}}
	stages := map[string]worker.ContinuationHandler{"finish_v1": func(c *wf.Context, _, locals json.RawMessage) (json.RawMessage, error) {
		stageCalls.Add(1)
		var value int
		found, err := c.GetState("value", &value)
		if err != nil || !found || value != 23 || string(locals) != "45" {
			return nil, fmt.Errorf("state/locals changed: value=%d found=%v locals=%s err=%v", value, found, locals, err)
		}
		if _, err := wf.AwaitSignal(c, "gate"); err != nil {
			return nil, err
		}
		_, err = wf.Run(c, "blocked-effect", 0, func(effectCtx context.Context) (int, error) {
			effectCalls.Add(1)
			entered <- effectCtx
			<-effectCtx.Done()
			return 0, effectCtx.Err()
		})
		return nil, err
	}}
	first, err := worker.New(ctx, all[1], "cancel-prefix", handlers, worker.WithContinuations(typ, stages))
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
	firstDone := make(chan error, 1)
	go func() { firstDone <- first.RunPartition(firstCtx, part) }()
	firstJoined := false
	defer func() {
		stopFirst()
		if !firstJoined {
			if err := <-firstDone; err != nil {
				t.Errorf("first shutdown: %v", err)
			}
		}
	}()
	store := journal.New(all[0])
	var prefix []journal.Record
	for ctx.Err() == nil {
		prefix, _, err = store.Read(ctx, typ, id)
		if err != nil {
			t.Fatal(err)
		}
		if len(prefix) > 0 {
			last := prefix[len(prefix)-1]
			if last.Kind == journal.Failed {
				t.Fatalf("failed before replacement: %s", last.Payload)
			}
			if last.Kind == journal.Suspended && string(last.Payload) == `{"waiting_on":"signal:gate"}` {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("continuation did not suspend")
	}
	stopFirst()
	err = <-firstDone
	firstJoined = true
	if err != nil {
		t.Fatal(err)
	}
	initialBefore, stageBefore := initialCalls.Load(), stageCalls.Load()
	guard := &checkpointNoArchivePort{SnapshotWritePort: journal.NewSnapshotPort(all[2])}
	second, err := worker.New(ctx, all[2], "cancel-active-continuation", handlers, worker.WithContinuations(typ, stages), worker.WithJournalStore(journal.NewWithJetStreamSnapshotPort(all[2], guard)))
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if missedNotification {
		// Deliberately remove the core subscription before delivery. The retained
		// cancel signal and production 15-second poll must interrupt the suffix.
		if err := second.Close(); err != nil {
			t.Fatal(err)
		}
		if err := all[2].Conn().FlushWithContext(ctx); err != nil {
			t.Fatal(err)
		}
	}
	secondCtx, stopSecond := context.WithCancel(ctx)
	secondDone := make(chan error, 1)
	go func() { secondDone <- second.RunPartition(secondCtx, part) }()
	secondJoined := false
	defer func() {
		stopSecond()
		if !secondJoined {
			if err := <-secondDone; err != nil {
				t.Errorf("second shutdown: %v", err)
			}
		}
	}()
	if _, err := c.Signal(ctx, typ, id, "gate", []byte(`true`), "gate-key"); err != nil {
		t.Fatal(err)
	}
	var effectCtx context.Context
	select {
	case effectCtx = <-entered:
	case <-ctx.Done():
		t.Fatal("replacement did not enter suffix effect")
	}
	stale := &nats.Msg{Subject: "wf.sig." + typ + "." + id + "." + client.CancelSignalName, Header: nats.Header{}}
	stale.Header.Set("Wf-Inv-Seq", strconv.FormatUint(handle.InvSeq+1, 10))
	if _, err := all[0].PublishMsg(ctx, stale); err != nil {
		t.Fatal(err)
	}
	select {
	case <-effectCtx.Done():
		t.Fatal("stale-generation cancel interrupted active continuation")
	case <-time.After(300 * time.Millisecond):
	}
	started := time.Now()
	sequence, err := c.Cancel(ctx, typ, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Await(ctx, typ, id); !errors.Is(err, client.ErrCancelled) {
		t.Fatalf("cancel outcome: %v", err)
	}
	elapsed := time.Since(started)
	stopSecond()
	err = <-secondDone
	secondJoined = true
	if err != nil {
		t.Fatal(err)
	}
	if effectCtx.Err() == nil || initialCalls.Load() != initialBefore || stageCalls.Load() != stageBefore+1 || effectCalls.Load() != 1 || guard.archives != 0 || guard.frames == 0 {
		t.Fatalf("cancel/prefix check: effect_error=%v initial=%d/%d stage=%d/%d effects=%d reads=%d/%d", effectCtx.Err(), initialCalls.Load(), initialBefore, stageCalls.Load(), stageBefore, effectCalls.Load(), guard.archives, guard.frames)
	}
	records, _, err := store.Read(ctx, typ, id)
	if err != nil || len(records) != len(prefix)+5 {
		t.Fatalf("terminal records=%d prefix=%d err=%v", len(records), len(prefix), err)
	}
	before, _ := json.Marshal(prefix)
	after, _ := json.Marshal(records[:len(prefix)])
	if !bytes.Equal(before, after) {
		t.Fatal("confirmed prefix changed")
	}
	suffix := records[len(prefix):]
	if suffix[0].Kind != journal.SignalConsumed || suffix[1].Kind != journal.StepCompleted || suffix[2].Kind != journal.StepRequested || suffix[3].Kind != journal.SignalConsumed || suffix[4].Kind != journal.Failed {
		t.Fatalf("cancellation suffix=%+v", suffix)
	}
	var consumed struct {
		Name     string `json:"name"`
		Sequence uint64 `json:"sig_seq"`
	}
	var terminal wf.Outcome
	if json.Unmarshal(suffix[3].Payload, &consumed) != nil || consumed.Name != client.CancelSignalName || consumed.Sequence != sequence || json.Unmarshal(suffix[4].Payload, &terminal) != nil || terminal.InvSeq != handle.InvSeq || terminal.Error != client.ErrCancelled.Error() {
		t.Fatalf("cancel sequence/outcome: consumed=%+v terminal=%+v", consumed, terminal)
	}
	for _, peer := range all {
		if _, err := client.New(peer).Await(ctx, typ, id); !errors.Is(err, client.ErrCancelled) {
			t.Fatalf("peer immutable outcome: %v", err)
		}
	}
	report, err := integrity.Check(ctx, all[0])
	if err != nil || report.Invocations != 1 || report.Terminal != 1 || report.Entries != len(records) {
		t.Fatalf("raw audit=%+v err=%v", report, err)
	}
	t.Logf("active continuation cancellation: missed_notification=%v prefix=%d suffix=%d effect_calls=%d archive_reads=%d frame_reads=%d cancellation_to_terminal=%s", missedNotification, len(prefix), len(suffix), effectCalls.Load(), guard.archives, guard.frames, elapsed)
}
