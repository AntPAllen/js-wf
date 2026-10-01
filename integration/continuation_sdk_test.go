package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/wf"
)

func TestContinueUsesRecordedEpochAfterHiddenCompletionAck(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	const typ, id = "test", "continue-sdk"
	handle, err := client.New(all[0]).Start(ctx, typ, id, []byte(`23`))
	if err != nil {
		t.Fatal(err)
	}
	leases, err := lease.New(ctx, all[0])
	if err != nil {
		t.Fatal(err)
	}
	owner, err := leases.Acquire(ctx, typ, id, "first-owner")
	if err != nil {
		t.Fatal(err)
	}
	firstEpoch := owner.Epoch()
	ownerID := "first-owner"
	defer func() { _ = owner.Release(context.Background()) }()
	objects, err := all[0].ObjectStore(ctx, "WF_BLOB")
	if err != nil {
		t.Fatal(err)
	}
	store := journal.New(all[0])
	var records []journal.Record
	var tail uint64
	hideAck := false
	hidden := false
	appendEntry := func(kind journal.Kind, payload json.RawMessage) error {
		if err := owner.Renew(ctx); err != nil {
			return err
		}
		entry := journal.Entry{Index: uint64(len(records)), Epoch: owner.Epoch(), Kind: kind, Payload: payload, WorkerID: ownerID}
		seq, err := store.Append(ctx, typ, id, entry, tail)
		if err != nil {
			return err
		}
		records = append(records, journal.Record{Entry: entry, Sequence: seq})
		tail = seq
		if hideAck && !hidden && kind == journal.StepCompleted {
			hidden = true
			return context.DeadlineExceeded
		}
		return nil
	}
	if err := appendEntry(journal.Started, nil); err != nil {
		t.Fatal(err)
	}
	puts := 0
	configure := func(entries []wf.Entry) *wf.Context {
		c := wf.NewContext(ctx, entries, func(_ context.Context, kind wf.Kind, payload json.RawMessage) error {
			return appendEntry(journal.Kind(kind), payload)
		})
		c.SetChildSupport(typ, id, handle.InvSeq, nil)
		c.SetContinuationSupport(func(stage string) bool { return stage == "next_v1" }, func(completed uint64, recorded bool) (wf.ContinuationAnchor, error) {
			if completed != 0 {
				if completed >= uint64(len(records)) {
					return wf.ContinuationAnchor{}, journal.ErrGap
				}
				return wf.ContinuationAnchor{Index: completed, Epoch: records[completed].Epoch}, nil
			}
			index := uint64(len(records) + 1)
			if recorded {
				index--
			}
			return wf.ContinuationAnchor{Index: index, Epoch: owner.Epoch()}, nil
		})
		c.SetResultStore(func(ctx context.Context, raw []byte) (string, error) {
			puts++
			hash := sha256.Sum256(raw)
			name := "step-result-" + hex.EncodeToString(hash[:])
			_, err := objects.PutBytes(ctx, name, raw)
			return name, err
		}, func(ctx context.Context, name string) ([]byte, error) { return objects.GetBytes(ctx, name) })
		return c
	}
	effects := 0
	prefixKey := ""
	prefix := func(c *wf.Context) error {
		if err := c.SetState("total", 23); err != nil {
			return err
		}
		_, err := wf.RunOnce(c, "external", 23, func(_ context.Context, key string) (int, error) { effects++; prefixKey = key; return 46, nil })
		return err
	}
	initial := configure(nil)
	if err := prefix(initial); err != nil {
		t.Fatal(err)
	}
	hideAck = true
	if err := wf.Continue(initial, "next_v1", 23); !errors.Is(err, context.DeadlineExceeded) || !hidden {
		t.Fatalf("completion ack not hidden: %v", err)
	}
	if _, ok := initial.Continuation(); ok {
		t.Fatal("unknown completion advertised continuation")
	}
	if !errors.Is(initial.CheckComplete(), context.DeadlineExceeded) {
		t.Fatal("unknown checkpoint became success")
	}
	if err := owner.Release(ctx); err != nil {
		t.Fatal(err)
	}
	owner, err = leases.Acquire(ctx, typ, id, "replacement-owner")
	ownerID = "replacement-owner"
	if err != nil {
		t.Fatal(err)
	}
	if owner.Epoch() <= firstEpoch {
		t.Fatal("replacement wasn't fenced above first owner")
	}
	records, tail, err = journal.New(all[1]).Read(ctx, typ, id)
	if err != nil {
		t.Fatal(err)
	}
	steps := make([]wf.Entry, 0, len(records)-1)
	for _, record := range records[1:] {
		steps = append(steps, wf.Entry{Index: record.Index, Kind: wf.Kind(record.Kind), Payload: record.Payload})
	}
	replay := configure(steps)
	if err := prefix(replay); err != nil {
		t.Fatal(err)
	}
	if err := wf.Continue(replay, "next_v1", 23); !errors.Is(err, wf.ErrContinuation) {
		t.Fatal(err)
	}
	point, ok := replay.Continuation()
	if !ok || point.Epoch != firstEpoch || puts != 1 || effects != 1 || len(records) != 7 {
		t.Fatalf("point=%+v puts=%d effects=%d records=%d", point, puts, effects, len(records))
	}
	runtime := journal.RuntimeCheckpoint{InvSeq: handle.InvSeq, Stage: point.Stage, Sequence: tail, Index: point.Index, Epoch: point.Epoch, StepPosition: point.StepPosition, Object: point.Object, SHA256: point.SHA256}
	if err := owner.Renew(ctx); err != nil {
		t.Fatal(err)
	}
	snap, err := store.WriteCheckpointSnapshot(ctx, typ, id, runtime)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PurgeSnapshot(ctx, typ, id, snap); err != nil {
		t.Fatal(err)
	}
	guard := &checkpointNoArchivePort{SnapshotWritePort: journal.NewSnapshotPort(all[2])}
	view, err := journal.NewWithJetStreamSnapshotPort(all[2], guard).ReadCheckpoint(ctx, typ, id, handle.InvSeq)
	if err != nil || view == nil {
		t.Fatalf("resume=%+v err=%v", view, err)
	}
	resumed, info, err := wf.NewCheckpointContext(ctx, nil, func(_ context.Context, kind wf.Kind, payload json.RawMessage) error {
		return appendEntry(journal.Kind(kind), payload)
	}, view.Frame, wf.CheckpointLocation{Type: typ, ID: id, InvSeq: handle.InvSeq, Index: point.Index, Epoch: point.Epoch, Hash: point.SHA256})
	if err != nil || info.Stage != "next_v1" {
		t.Fatal(info, err)
	}
	var total int
	if found, err := resumed.GetState("total", &total); err != nil || !found || total != 23 {
		t.Fatal(found, total, err)
	}
	newKey := ""
	if _, err := wf.RunOnce(resumed, "external", 23, func(_ context.Context, key string) (int, error) { newKey = key; return 69, nil }); err != nil {
		t.Fatal(err)
	}
	if newKey == "" || newKey == prefixKey || guard.archives != 0 {
		t.Fatalf("key identity/archive reads: %q %q %d", prefixKey, newKey, guard.archives)
	}
}
