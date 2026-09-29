package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/sim"
)

// Keep the modeled journal sequence and CAS baseline tied to a real stream.
// Lost acknowledgments and unchanged-tail wrong-sequence replies have separate
// fault fixtures because a healthy server does not produce them on demand.
func TestSimJournalAppendContractAgainstRealCluster(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	real := journal.New(all[0])
	model := sim.NewJournalTransport(sim.NewScheduler(1))
	simulated := journal.NewWithPorts(model, model)
	const typ = "sim-contract"
	appendBoth := func(id string, entry journal.Entry, expected uint64) uint64 {
		t.Helper()
		gotReal, realErr := real.Append(ctx, typ, id, entry, expected)
		gotModel, modelErr := simulated.Append(ctx, typ, id, entry, expected)
		if gotReal != gotModel || realErr != nil || modelErr != nil {
			t.Fatalf("append %s index=%d: real=(%d,%v) model=(%d,%v)", id, entry.Index, gotReal, realErr, gotModel, modelErr)
		}
		return gotReal
	}
	first := appendBoth("one", journal.Entry{Kind: journal.Started, Index: 0, Epoch: 1}, 0)
	appendBoth("two", journal.Entry{Kind: journal.Started, Index: 0, Epoch: 1}, 0)
	appendBoth("one", journal.Entry{Kind: journal.StepRequested, Index: 1, Epoch: 2}, first)
	stale := journal.Entry{Kind: journal.StepRequested, Index: 1, Epoch: 1}
	if _, realErr := real.Append(ctx, typ, "one", stale, first); !errors.Is(realErr, journal.ErrStale) {
		t.Fatalf("real stale append: %v", realErr)
	}
	if _, modelErr := simulated.Append(ctx, typ, "one", stale, first); !errors.Is(modelErr, journal.ErrStale) {
		t.Fatalf("modeled stale append: %v", modelErr)
	}
	stream, err := all[1].Stream(ctx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"one", "two"} {
		subject := identity.JournalSubject(typ, id)
		modeled := model.Messages(subject)
		records, realTail, err := real.Read(ctx, typ, id)
		if err != nil || len(records) != len(modeled) {
			t.Fatalf("read %s: records=%d modeled=%d err=%v", id, len(records), len(modeled), err)
		}
		modelRecords, modelTail, modelErr := simulated.Read(ctx, typ, id)
		if modelErr != nil || realTail != modelTail || !reflect.DeepEqual(records, modelRecords) {
			t.Fatalf("read contract %s: real=%+v tail=%d model=%+v tail=%d err=%v", id, records, realTail, modelRecords, modelTail, modelErr)
		}
		for _, message := range modeled {
			realMessage, err := stream.GetMsg(ctx, message.Sequence)
			if err != nil || realMessage.Subject != subject || !bytes.Equal(realMessage.Data, message.Data) {
				t.Fatalf("retained message %d for %s differs: err=%v", message.Sequence, id, err)
			}
		}
	}
}

func TestMissingJournalCASHeaderMutationIsDetected(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	const typ, id = "mutation", "missing-cas"
	if _, err := client.New(all[0]).Start(ctx, typ, id, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	subject := identity.JournalSubject(typ, id)
	for _, entry := range []journal.Entry{
		{Kind: journal.Started, Index: 0},
		{Kind: journal.StepRequested, Index: 1, Epoch: 1, WorkerID: "worker-a"},
		{Kind: journal.StepRequested, Index: 1, Epoch: 2, WorkerID: "worker-b"},
	} {
		data, err := json.Marshal(entry)
		if err != nil {
			t.Fatal(err)
		}
		// This deliberately bypasses journal.Append's CAS header.
		if _, err := all[0].Publish(ctx, subject, data); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := journal.New(all[1]).Read(ctx, typ, id); !errors.Is(err, journal.ErrGap) {
		t.Fatalf("missing-CAS mutation escaped journal reader: %v", err)
	}
	if _, err := integrity.Check(ctx, all[2]); err == nil {
		t.Fatal("missing-CAS mutation escaped retained-state checker")
	}
}

func TestSimSnapshotReadContractAgainstRealCluster(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	const typ, id = "snapshot-contract", "one"
	real := journal.New(all[0])
	schedule := sim.NewScheduler(27)
	live := sim.NewJournalTransport(schedule)
	snapshots := sim.NewSnapshotReadTransport(schedule)
	snapshots.BindJournal(live)
	modeled := journal.NewWithSnapshotPort(live, live, snapshots)
	var records []journal.Record
	appendBoth := func(kind journal.Kind) {
		t.Helper()
		expected := uint64(0)
		if len(records) > 0 {
			expected = records[len(records)-1].Sequence
		}
		entry := journal.Entry{Kind: kind, Index: uint64(len(records)), Epoch: 1, WorkerID: "snapshot-worker"}
		realSeq, realErr := real.Append(ctx, typ, id, entry, expected)
		modelSeq, modelErr := modeled.Append(ctx, typ, id, entry, expected)
		if realErr != nil || modelErr != nil || realSeq != modelSeq {
			t.Fatalf("append %d: real=(%d,%v) model=(%d,%v)", entry.Index, realSeq, realErr, modelSeq, modelErr)
		}
		records = append(records, journal.Record{Entry: entry, Sequence: realSeq})
	}
	appendBoth(journal.Started)
	for i := 0; i < 10; i++ {
		appendBoth(journal.StepRequested)
		appendBoth(journal.StepCompleted)
	}
	state, err := all[1].KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	objects, err := all[1].ObjectStore(ctx, "WF_BLOB")
	if err != nil {
		t.Fatal(err)
	}
	for _, keep := range []int{4, 2} {
		realSnap, err := real.SnapshotPrefix(ctx, typ, id, keep)
		if err != nil {
			t.Fatal(err)
		}
		modelSnap, err := modeled.SnapshotPrefix(ctx, typ, id, keep)
		if err != nil || realSnap != modelSnap {
			t.Fatalf("snapshot keep=%d real=%+v model=%+v err=%v", keep, realSnap, modelSnap, err)
		}
		realRecords, realTail, realErr := real.Read(ctx, typ, id)
		modelRecords, modelTail, modelErr := modeled.Read(ctx, typ, id)
		if realErr != nil || modelErr != nil || realTail != modelTail || !reflect.DeepEqual(realRecords, modelRecords) || !reflect.DeepEqual(realRecords, records) {
			t.Fatalf("snapshot read keep=%d real=(%d,%d,%v) model=(%d,%d,%v)", keep, len(realRecords), realTail, realErr, len(modelRecords), modelTail, modelErr)
		}
		realManifest, err := state.Get(ctx, "snap."+identity.Key(typ, id))
		if err != nil {
			t.Fatal(err)
		}
		modelManifest, err := snapshots.GetManifest(ctx, "snap."+identity.Key(typ, id))
		if err != nil || !bytes.Equal(realManifest.Value(), modelManifest) {
			t.Fatalf("snapshot manifest keep=%d differs: %v", keep, err)
		}
		realObject, err := objects.GetBytes(ctx, realSnap.Object)
		if err != nil {
			t.Fatal(err)
		}
		modelObject, err := snapshots.GetObject(ctx, modelSnap.Object)
		if err != nil || !bytes.Equal(realObject, modelObject) {
			t.Fatalf("snapshot object keep=%d differs: %v", keep, err)
		}
	}
}
