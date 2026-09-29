package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"js-wf/identity"
	"js-wf/journal"
	"js-wf/retention"
	"js-wf/sim"
	"js-wf/wf"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestSimPurgeAndBlobSweepContractAgainstRealCluster(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const typ, id = "purgecontract", "one"
	key := identity.Key(typ, id)
	invSubject := identity.InvocationSubject(typ, id)
	model := sim.NewPurgeTransport(sim.NewScheduler(42))
	objects, err := all[0].ObjectStore(ctx, "WF_BLOB")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"input-contract", "signal-contract", "terminal-result-contract", "input-orphan", "user-unmanaged"} {
		if _, err := objects.PutBytes(ctx, name, []byte(name)); err != nil {
			t.Fatal(err)
		}
		model.Blobs.PutObject(name, []byte(name), time.Unix(0, 0).Add(-time.Hour))
	}
	inputHeader := nats.Header{}
	inputHeader.Set("Wf-Input-Ref", "input-contract")
	realInv, err := all[0].PublishMsg(ctx, &nats.Msg{Subject: invSubject, Header: inputHeader, Data: []byte(`null`)})
	if err != nil {
		t.Fatal(err)
	}
	modelInv, err := model.Blobs.PublishSubject("WF_INV", invSubject, inputHeader, []byte(`null`))
	if err != nil {
		t.Fatal(err)
	}
	signalHeader := nats.Header{}
	signalHeader.Set("Wf-Signal-Ref", "signal-contract")
	signalSubject := "wf.sig." + typ + "." + id + ".go"
	if _, err := all[0].PublishMsg(ctx, &nats.Msg{Subject: signalSubject, Header: signalHeader, Data: []byte(`signal`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := model.Blobs.PublishSubject("WF_SIG", signalSubject, signalHeader, []byte(`signal`)); err != nil {
		t.Fatal(err)
	}
	realOutcome, _ := json.Marshal(wf.Outcome{InvSeq: realInv.Sequence, ResultRef: "terminal-result-contract"})
	modelOutcome, _ := json.Marshal(wf.Outcome{InvSeq: modelInv, ResultRef: "terminal-result-contract"})
	store := journal.New(all[0])
	seq, err := store.Append(ctx, typ, id, journal.Entry{Index: 0, Epoch: 1, Kind: journal.Started}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, typ, id, journal.Entry{Index: 1, Epoch: 1, Kind: journal.Completed, Payload: realOutcome}, seq); err != nil {
		t.Fatal(err)
	}
	realSnapshot, err := store.SnapshotPrefix(ctx, typ, id, 1)
	if err != nil {
		t.Fatal(err)
	}
	realSnapshotBytes, err := objects.GetBytes(ctx, realSnapshot.Object)
	if err != nil {
		t.Fatal(err)
	}
	var modelStartedSeq uint64
	for _, entry := range []journal.Entry{{Index: 0, Epoch: 1, Kind: journal.Started}, {Index: 1, Epoch: 1, Kind: journal.Completed, Payload: modelOutcome}} {
		data, err := json.Marshal(entry)
		if err != nil {
			t.Fatal(err)
		}
		modelSeq, err := model.Blobs.PublishSubject("WF_JRN", identity.JournalSubject(typ, id), nil, data)
		if err != nil {
			t.Fatal(err)
		}
		if entry.Kind == journal.Started {
			modelStartedSeq = modelSeq
		}
	}
	keyHash := sha256.Sum256([]byte(key))
	modelSnapshotName := "snapshot-" + hex.EncodeToString(keyHash[:8]) + "-model"
	modelSnapshotBytes, err := json.Marshal([]journal.Record{{Entry: journal.Entry{Index: 0, Epoch: 1, Kind: journal.Started}, Sequence: modelStartedSeq}})
	if err != nil {
		t.Fatal(err)
	}
	model.Blobs.PutObject(modelSnapshotName, modelSnapshotBytes, time.Unix(0, 0).Add(-time.Hour))
	modelSnapshotHash := sha256.Sum256(modelSnapshotBytes)
	modelManifest, err := json.Marshal(journal.Snapshot{Version: 1, LastSeq: modelStartedSeq, LastIndex: 0, Epoch: 1, Object: modelSnapshotName, SHA256: hex.EncodeToString(modelSnapshotHash[:])})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := model.Blobs.State().Create(ctx, "snap."+key, modelManifest); err != nil {
		t.Fatal(err)
	}
	if err := model.Blobs.Purge("WF_JRN", modelStartedSeq); err != nil {
		t.Fatal(err)
	}
	state, err := all[0].KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.Put(ctx, key, realOutcome); err != nil {
		t.Fatal(err)
	}
	if _, err := model.Blobs.State().Create(ctx, key, modelOutcome); err != nil {
		t.Fatal(err)
	}
	compareSweep := func(stage string, wantDeleted int) {
		t.Helper()
		real, realErr := retention.SweepBlobsQuiescent(ctx, all[0], 0, time.Now().Add(time.Minute))
		modeled, modelErr := retention.SweepBlobsQuiescentWithPort(ctx, model.Blobs, 0, time.Unix(0, 0).Add(time.Hour))
		if realErr != nil || modelErr != nil || real != modeled || real.Deleted != wantDeleted {
			t.Fatalf("%s real=%+v err=%v model=%+v err=%v", stage, real, realErr, modeled, modelErr)
		}
	}
	compareSweep("before purge", 1)
	if _, err := objects.PutBytes(ctx, realSnapshot.Object, []byte(`corrupt`)); err != nil {
		t.Fatal(err)
	}
	model.Blobs.PutObject(modelSnapshotName, []byte(`corrupt`), time.Unix(0, 0).Add(-time.Hour))
	realCorrupt := retention.Purge(ctx, all[0], typ, id, time.Hour)
	modelCorrupt := retention.PurgeWithPort(ctx, model, typ, id, time.Hour)
	if !errors.Is(realCorrupt, journal.ErrGap) || !errors.Is(modelCorrupt, journal.ErrGap) {
		t.Fatalf("corrupt snapshot real=%v model=%v", realCorrupt, modelCorrupt)
	}
	if _, err := state.Get(ctx, "purging."+key); !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatalf("real purge marker written despite corrupt snapshot: %v", err)
	}
	if _, err := model.Blobs.State().Get(ctx, "purging."+key); !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatalf("model purge marker written despite corrupt snapshot: %v", err)
	}
	if _, err := objects.PutBytes(ctx, realSnapshot.Object, realSnapshotBytes); err != nil {
		t.Fatal(err)
	}
	model.Blobs.PutObject(modelSnapshotName, modelSnapshotBytes, time.Unix(0, 0).Add(-time.Hour))
	if err := retention.Purge(ctx, all[0], typ, id, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := retention.PurgeWithPort(ctx, model, typ, id, time.Hour); err != nil {
		t.Fatal(err)
	}
	realState, err := state.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	modelState, err := model.Blobs.State().Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	realMarker, realTomb, realErr := retention.Decode(realState.Value())
	modelMarker, modelTomb, modelErr := retention.Decode(modelState.Value)
	if realErr != nil || modelErr != nil || !realTomb || !modelTomb || realMarker.InvSeq != realInv.Sequence || modelMarker.InvSeq != modelInv || model.PurgeEventCount() != 1 {
		t.Fatalf("real tombstone=%+v err=%v model=%+v err=%v", realMarker, realErr, modelMarker, modelErr)
	}
	if err := retention.Purge(ctx, all[0], typ, id, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := retention.PurgeWithPort(ctx, model, typ, id, time.Hour); err != nil || model.PurgeEventCount() != 1 {
		t.Fatalf("modeled idempotent purge: events=%d err=%v", model.PurgeEventCount(), err)
	}
	inv, err := all[0].Stream(ctx, "WF_INV")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := inv.GetLastMsgForSubject(ctx, invSubject); !errors.Is(err, jetstream.ErrMsgNotFound) {
		t.Fatalf("real invocation retained: %v", err)
	}
	if _, err := model.Invocation(ctx, invSubject); !errors.Is(err, jetstream.ErrMsgNotFound) {
		t.Fatalf("model invocation retained: %v", err)
	}
	compareSweep("after purge", 4)
	for _, name := range []string{"input-contract", "signal-contract", "terminal-result-contract", "input-orphan"} {
		if _, err := objects.GetInfo(ctx, name); !errors.Is(err, jetstream.ErrObjectNotFound) || model.Blobs.HasObject(name) {
			t.Fatalf("%s survived: %v", name, err)
		}
	}
	if _, err := objects.GetInfo(ctx, "user-unmanaged"); err != nil || !model.Blobs.HasObject("user-unmanaged") {
		t.Fatalf("unmanaged object removed: %v", err)
	}
	if model.Blobs.HasObject(modelSnapshotName) {
		t.Fatal("modeled compacted snapshot object survived purge and blob sweep")
	}
}
