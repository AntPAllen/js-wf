package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go/jetstream"
)

func TestSnapshotPurgeAndRecovery(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	const typ, id = "test", "snap"
	if _, err := client.New(all[0]).Start(ctx, typ, id, []byte(`1`)); err != nil {
		t.Fatal(err)
	}
	j := journal.New(all[0])
	var seq uint64
	var index uint64
	appendEntry := func(kind journal.Kind, payload json.RawMessage) {
		t.Helper()
		var err error
		seq, err = j.Append(ctx, typ, id, journal.Entry{Epoch: 1, Index: index, Kind: kind, Payload: payload}, seq)
		if err != nil {
			t.Fatal(err)
		}
		index++
	}
	appendEntry(journal.Started, nil)
	for i := 0; i < 60; i++ {
		appendEntry(journal.StepRequested, nil)
		appendEntry(journal.StepCompleted, nil)
	}
	before, _, err := j.Read(ctx, typ, id)
	if err != nil {
		t.Fatal(err)
	}
	snap, err := j.WriteSnapshot(ctx, typ, id, 8)
	if err != nil {
		t.Fatal(err)
	}
	// The manifest can become visible before the purge, and a writer can append
	// while that purge is pending. The explicit sequence cutoff must retain it.
	appendEntry(journal.StepRequested, nil)
	appendEntry(journal.StepCompleted, nil)
	between, tail, err := j.Read(ctx, typ, id)
	if err != nil || len(between) != len(before)+2 || tail != seq {
		t.Fatalf("between: entries=%d tail=%d err=%v", len(between), tail, err)
	}
	if err := journal.New(all[1]).PurgeSnapshot(ctx, typ, id, snap); err != nil {
		t.Fatal(err)
	}
	after, tail, err := j.Read(ctx, typ, id)
	if err != nil || !reflect.DeepEqual(after, between) || tail != seq {
		t.Fatalf("after purge: entries=%d tail=%d err=%v", len(after), tail, err)
	}
	stream, err := all[0].Stream(ctx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.GetMsg(ctx, before[0].Sequence); !errors.Is(err, jetstream.ErrMsgNotFound) {
		t.Fatalf("prefix still retained: %v", err)
	}
	if _, err := stream.GetLastMsgForSubject(ctx, identity.JournalSubject(typ, id)); err != nil {
		t.Fatal(err)
	}
	second, err := j.SnapshotPrefix(ctx, typ, id, 4)
	if err != nil || second.LastSeq <= snap.LastSeq {
		t.Fatalf("second snapshot: %+v err=%v", second, err)
	}
	if err := j.PurgeSnapshot(ctx, typ, id, snap); err != nil {
		t.Fatal(err)
	}
	final, _, err := j.Read(ctx, typ, id)
	if err != nil || !reflect.DeepEqual(final, between) {
		t.Fatalf("second purge changed logical journal: err=%v", err)
	}
	payload, _ := json.Marshal(wf.Outcome{Result: json.RawMessage(`2`)})
	appendEntry(journal.Completed, payload)
	state, err := all[0].KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.Put(ctx, identity.Key(typ, id), payload); err != nil {
		t.Fatal(err)
	}
	report, err := integrity.Check(ctx, all[0])
	if err != nil || report.Entries != int(index) || report.Terminal != 1 {
		t.Fatalf("integrity after purge: %+v err=%v", report, err)
	}
	objects, err := all[0].ObjectStore(ctx, "WF_BLOB")
	if err != nil {
		t.Fatal(err)
	}
	if err := objects.Delete(ctx, second.Object); err != nil {
		t.Fatal(err)
	}
	if _, _, err := j.Read(ctx, typ, id); !errors.Is(err, journal.ErrGap) {
		t.Fatalf("missing snapshot object: %v", err)
	}
	if _, err := integrity.Check(ctx, all[0]); !errors.Is(err, journal.ErrGap) {
		t.Fatalf("integrity missed missing snapshot object: %v", err)
	}
}

func TestWorkerCompactsLongJournal(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	const typ, id = "test", "auto-snap"
	w, err := worker.New(ctx, all[1], "snapshot-worker", map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		for i := 0; i < 130; i++ {
			if _, err := wf.Run(c, fmt.Sprintf("step-%d", i), i, func(context.Context) (int, error) { return i, nil }); err != nil {
				return nil, err
			}
		}
		return json.RawMessage(`1`), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.New(all[0]).Start(ctx, typ, id, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- w.RunPartition(workerCtx, identity.Partition(typ, id, provision.Partitions)) }()
	if _, err := client.New(all[0]).Await(ctx, typ, id); err != nil {
		t.Fatal(err)
	}
	state, err := all[0].KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	// Await observes the terminal key before the worker's compaction completes.
	var manifest jetstream.KeyValueEntry
	for ctx.Err() == nil {
		manifest, err = state.Get(ctx, "snap."+identity.Key(typ, id))
		if err == nil {
			break
		}
		if !errors.Is(err, jetstream.ErrKeyNotFound) {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	var snap journal.Snapshot
	if err := json.Unmarshal(manifest.Value(), &snap); err != nil || snap.LastIndex == 0 {
		t.Fatalf("snapshot: %+v err=%v", snap, err)
	}
	stream, err := all[0].Stream(ctx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	for ctx.Err() == nil {
		_, err = stream.GetMsg(ctx, 1)
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !errors.Is(err, jetstream.ErrMsgNotFound) {
		t.Fatalf("worker did not purge prefix: %v", err)
	}
	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	records, _, err := journal.New(all[0]).Read(ctx, typ, id)
	if err != nil || len(records) != 262 || records[len(records)-1].Kind != journal.Completed {
		t.Fatalf("compacted journal: entries=%d err=%v", len(records), err)
	}
}

func TestSnapshotPurgesOnlyConsumedSignals(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	const typ, id = "test", "signal-snap"
	if _, err := client.New(all[0]).Start(ctx, typ, id, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	first, err := all[0].Publish(ctx, "wf.sig.test.signal-snap.go", []byte(`1`))
	if err != nil {
		t.Fatal(err)
	}
	second, err := all[0].Publish(ctx, "wf.sig.test.signal-snap.go", []byte(`2`))
	if err != nil {
		t.Fatal(err)
	}
	j := journal.New(all[0])
	var tail uint64
	for i, e := range []journal.Entry{
		{Epoch: 1, Index: 0, Kind: journal.Started},
		{Epoch: 1, Index: 1, Kind: journal.SignalConsumed, Payload: json.RawMessage(fmt.Sprintf(`{"sig_seq":%d,"name":"go","payload":"MQ=="}`, first.Sequence))},
		{Epoch: 1, Index: 2, Kind: journal.StepRequested},
		{Epoch: 1, Index: 3, Kind: journal.StepCompleted},
	} {
		tail, err = j.Append(ctx, typ, id, e, tail)
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if _, err := j.SnapshotPrefix(ctx, typ, id, 1); err != nil {
		t.Fatal(err)
	}
	stream, err := all[0].Stream(ctx, "WF_SIG")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.GetMsg(ctx, first.Sequence); !errors.Is(err, jetstream.ErrMsgNotFound) {
		t.Fatalf("consumed signal retained: %v", err)
	}
	if _, err := stream.GetMsg(ctx, second.Sequence); err != nil {
		t.Fatalf("unconsumed signal removed: %v", err)
	}
	records, _, err := j.Read(ctx, typ, id)
	if err != nil || len(records) != 4 {
		t.Fatalf("logical journal: entries=%d err=%v", len(records), err)
	}
}

func TestSignalRetryAfterSnapshotPurge(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	const typ, id = "test", "signal-retry"
	c := client.New(all[0])
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	data := []byte(`"yes"`)
	seq, err := c.Signal(ctx, typ, id, "go", data, "same")
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	payload, _ := json.Marshal(struct {
		Sequence uint64 `json:"sig_seq"`
		Name     string `json:"name"`
		Hash     string `json:"hash"`
	}{seq, "go", hex.EncodeToString(digest[:])})
	j := journal.New(all[0])
	var tail uint64
	for i, e := range []journal.Entry{
		{Epoch: 1, Index: 0, Kind: journal.Started},
		{Epoch: 1, Index: 1, Kind: journal.SignalConsumed, Payload: payload},
		{Epoch: 1, Index: 2, Kind: journal.StepRequested},
		{Epoch: 1, Index: 3, Kind: journal.StepCompleted},
	} {
		tail, err = j.Append(ctx, typ, id, e, tail)
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if _, err := j.SnapshotPrefix(ctx, typ, id, 1); err != nil {
		t.Fatal(err)
	}
	retrySeq, err := c.Signal(ctx, typ, id, "go", data, "same")
	if err != nil || retrySeq != seq {
		t.Fatalf("same signal retry: seq=%d want=%d err=%v", retrySeq, seq, err)
	}
	if _, err := c.Signal(ctx, typ, id, "go", []byte(`"changed"`), "same"); !errors.Is(err, client.ErrSignalMismatch) {
		t.Fatalf("different signal retry: %v", err)
	}
}
