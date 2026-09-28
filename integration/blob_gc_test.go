package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"js-wf/identity"
	"js-wf/journal"
	"js-wf/retention"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestQuiescentBlobSweepPreservesRetainedReferences(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	objects, err := all[0].ObjectStore(ctx, "WF_BLOB")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"input-shared", "signal-snapshot", "step-result-snapshot", "terminal-result-state", "input-orphan", "user-unmanaged"} {
		deadline := time.Now().Add(5 * time.Second)
		for {
			_, err := objects.PutBytes(ctx, name, []byte(name))
			if err == nil {
				break
			}
			if !errors.Is(err, nats.ErrNoResponders) || time.Now().After(deadline) {
				t.Fatal(err)
			}
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(50 * time.Millisecond):
			}
		}
	}
	putInvocation := func(id string) uint64 {
		t.Helper()
		message := &nats.Msg{Subject: identity.InvocationSubject("test", id), Header: nats.Header{}}
		message.Header.Set("Wf-Input-Ref", "input-shared")
		ack, err := all[0].PublishMsg(ctx, message)
		if err != nil {
			t.Fatal(err)
		}
		return ack.Sequence
	}
	putInvocation("one")
	secondSeq := putInvocation("two")
	signal := &nats.Msg{Subject: "wf.sig.test.one.go", Header: nats.Header{}}
	signal.Header.Set("Wf-Signal-Ref", "signal-snapshot")
	signalAck, err := all[0].PublishMsg(ctx, signal)
	if err != nil {
		t.Fatal(err)
	}
	j := journal.New(all[0])
	var seq uint64
	var nextIndex int
	appendEntry := func(kind journal.Kind, payload json.RawMessage) {
		t.Helper()
		var err error
		seq, err = j.Append(ctx, "test", "one", journal.Entry{Index: uint64(nextIndex), Epoch: 1, Kind: kind, Payload: payload}, seq)
		if err != nil {
			t.Fatal(err)
		}
		nextIndex++
	}
	appendEntry(journal.Started, nil)
	appendEntry(journal.StepRequested, json.RawMessage(`{"kind":"run","name":"work"}`))
	appendEntry(journal.StepCompleted, json.RawMessage(`{"result_ref":"step-result-snapshot"}`))
	consumed, _ := json.Marshal(struct {
		Sequence uint64 `json:"sig_seq"`
		Name     string `json:"name"`
		Ref      string `json:"ref"`
	}{signalAck.Sequence, "go", "signal-snapshot"})
	appendEntry(journal.SignalConsumed, consumed)
	appendEntry(journal.Suspended, json.RawMessage(`{"waiting_on":"signal:other"}`))
	snapshot, err := j.SnapshotPrefix(ctx, "test", "one", 1)
	if err != nil {
		t.Fatal(err)
	}
	state, err := all[0].KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	terminal, _ := json.Marshal(struct {
		InvSeq    uint64 `json:"inv_seq"`
		ResultRef string `json:"result_ref"`
	}{secondSeq, "terminal-result-state"})
	if _, err := state.Put(ctx, "test.two", terminal); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Add(time.Minute)
	tooYoung, err := retention.SweepBlobsQuiescent(ctx, all[0], time.Hour, now)
	if err != nil || tooYoung.Deleted != 0 {
		t.Fatalf("age guard=%+v err=%v", tooYoung, err)
	}
	first, err := retention.SweepBlobsQuiescent(ctx, all[0], 0, now)
	if err != nil || first.Deleted != 1 {
		t.Fatalf("first sweep=%+v err=%v", first, err)
	}
	if _, err := objects.GetInfo(ctx, "input-orphan"); !errors.Is(err, jetstream.ErrObjectNotFound) {
		t.Fatalf("orphan retained: %v", err)
	}
	for _, name := range []string{"input-shared", "signal-snapshot", "step-result-snapshot", "terminal-result-state", snapshot.Object, "user-unmanaged"} {
		if _, err := objects.GetInfo(ctx, name); err != nil {
			t.Fatalf("referenced object %s deleted: %v", name, err)
		}
	}
	inv, err := all[0].Stream(ctx, "WF_INV")
	if err != nil {
		t.Fatal(err)
	}
	if err := inv.Purge(ctx, jetstream.WithPurgeSubject(identity.InvocationSubject("test", "one"))); err != nil {
		t.Fatal(err)
	}
	stillShared, err := retention.SweepBlobsQuiescent(ctx, all[0], 0, now)
	if err != nil || stillShared.Deleted != 0 {
		t.Fatalf("shared input reference lost: %+v err=%v", stillShared, err)
	}
	if err := inv.Purge(ctx, jetstream.WithPurgeSubject(identity.InvocationSubject("test", "two"))); err != nil {
		t.Fatal(err)
	}
	if err := state.Delete(ctx, "test.two"); err != nil {
		t.Fatal(err)
	}
	if err := state.Delete(ctx, "snap.test.one"); err != nil {
		t.Fatal(err)
	}
	jrn, err := all[0].Stream(ctx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	if err := jrn.Purge(ctx, jetstream.WithPurgeSubject(identity.JournalSubject("test", "one"))); err != nil {
		t.Fatal(err)
	}
	last, err := retention.SweepBlobsQuiescent(ctx, all[0], 0, now)
	if err != nil || last.Deleted != 5 {
		t.Fatalf("final sweep=%+v err=%v", last, err)
	}
	if _, err := objects.GetInfo(ctx, "user-unmanaged"); err != nil {
		t.Fatalf("unmanaged object deleted: %v", err)
	}
}

func TestQuiescentBlobSweepFailsClosedOnMissingSnapshot(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	objects, err := all[0].ObjectStore(ctx, "WF_BLOB")
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 0; ; attempt++ {
		_, err = objects.PutBytes(ctx, "input-orphan", []byte(`orphan`))
		if err == nil {
			break
		}
		if !errors.Is(err, nats.ErrNoResponders) || attempt == 9 {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
	state, err := all[0].KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.Put(ctx, "snap.test.missing", []byte(`{"version":1,"object":"snapshot-missing","sha256":"deadbeef"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := retention.SweepBlobsQuiescent(ctx, all[0], 0, time.Now().Add(time.Minute)); err == nil {
		t.Fatal("missing snapshot did not abort blob sweep")
	}
	if _, err := objects.GetInfo(ctx, "input-orphan"); err != nil {
		t.Fatalf("sweep deleted an object before finishing mark phase: %v", err)
	}
}
