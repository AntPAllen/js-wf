package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"js-wf/identity"
	"js-wf/journal"
	"js-wf/retention"
	"js-wf/sim"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestSimBlobSweepContractAgainstRealCluster(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	objects, err := all[0].ObjectStore(ctx, "WF_BLOB")
	if err != nil {
		t.Fatal(err)
	}
	model := sim.NewBlobSweepTransport(sim.NewScheduler(42))
	now := time.Now().Add(time.Minute)
	for _, name := range []string{"input-kept", "signal-kept", "step-result-kept", "snapshot-kept", "terminal-result-kept", "input-orphan", "user-unmanaged"} {
		if _, err := objects.PutBytes(ctx, name, []byte(name)); err != nil {
			t.Fatal(err)
		}
		model.PutObject(name, []byte(name), now.Add(-time.Hour))
	}
	inputHeader := nats.Header{}
	inputHeader.Set("Wf-Input-Ref", "input-kept")
	invMessage := &nats.Msg{Subject: identity.InvocationSubject("test", "blob-contract"), Header: inputHeader, Data: []byte(`null`)}
	if _, err := all[0].PublishMsg(ctx, invMessage); err != nil {
		t.Fatal(err)
	}
	modelInv, err := model.Publish("WF_INV", inputHeader, invMessage.Data)
	if err != nil {
		t.Fatal(err)
	}
	holeSubject := identity.InvocationSubject("test", "blob-hole")
	if _, err := all[0].Publish(ctx, holeSubject, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	modelHole, err := model.Publish("WF_INV", nil, []byte(`null`))
	if err != nil {
		t.Fatal(err)
	}
	inv, err := all[0].Stream(ctx, "WF_INV")
	if err != nil {
		t.Fatal(err)
	}
	if err := inv.Purge(ctx, jetstream.WithPurgeSubject(holeSubject)); err != nil {
		t.Fatal(err)
	}
	if err := model.Purge("WF_INV", modelHole); err != nil {
		t.Fatal(err)
	}
	signalHeader := nats.Header{}
	signalHeader.Set("Wf-Signal-Ref", "signal-kept")
	signalMessage := &nats.Msg{Subject: "wf.sig.test.blob-contract.go", Header: signalHeader, Data: []byte(`signal`)}
	if _, err := all[0].PublishMsg(ctx, signalMessage); err != nil {
		t.Fatal(err)
	}
	modelSignal, err := model.Publish("WF_SIG", signalHeader, signalMessage.Data)
	if err != nil {
		t.Fatal(err)
	}
	stepBytes, err := json.Marshal(journal.Entry{Kind: journal.StepCompleted, Payload: json.RawMessage(`{"result_ref":"step-result-kept"}`)})
	if err != nil {
		t.Fatal(err)
	}
	journalSubject := identity.JournalSubject("test", "blob-contract")
	if _, err := all[0].Publish(ctx, journalSubject, stepBytes); err != nil {
		t.Fatal(err)
	}
	modelStep, err := model.Publish("WF_JRN", nil, stepBytes)
	if err != nil {
		t.Fatal(err)
	}
	snapshotBytes, err := json.Marshal([]journal.Record{{Entry: journal.Entry{Kind: journal.SignalConsumed, Payload: json.RawMessage(`{"ref":"signal-kept"}`)}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := objects.PutBytes(ctx, "snapshot-kept", snapshotBytes); err != nil {
		t.Fatal(err)
	}
	model.PutObject("snapshot-kept", snapshotBytes, now.Add(-time.Hour))
	hash := sha256.Sum256(snapshotBytes)
	manifest, err := json.Marshal(journal.Snapshot{Object: "snapshot-kept", SHA256: hex.EncodeToString(hash[:])})
	if err != nil {
		t.Fatal(err)
	}
	state, err := all[0].KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.Put(ctx, "snap.test.blob-contract", manifest); err != nil {
		t.Fatal(err)
	}
	modelSnapshotRev, err := model.State().Create(ctx, "snap.test.blob-contract", manifest)
	if err != nil {
		t.Fatal(err)
	}
	terminalValue := []byte(`{"result_ref":"terminal-result-kept"}`)
	if _, err := state.Put(ctx, "test.blob-contract", terminalValue); err != nil {
		t.Fatal(err)
	}
	modelTerminalRev, err := model.State().Create(ctx, "test.blob-contract", terminalValue)
	if err != nil {
		t.Fatal(err)
	}
	compare := func(stage string, wantDeleted int) {
		t.Helper()
		modeled, modelErr := retention.SweepBlobsQuiescentWithPort(ctx, model, 0, now)
		real, realErr := retention.SweepBlobsQuiescent(ctx, all[0], 0, now)
		if modelErr != nil || realErr != nil || real != modeled || real.Deleted != wantDeleted {
			probeCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			listed, listErr := objects.List(probeCtx)
			infoState := make(map[string]string)
			for _, name := range []string{"input-kept", "signal-kept", "step-result-kept", "snapshot-kept", "terminal-result-kept", "input-orphan", "user-unmanaged"} {
				info, infoErr := objects.GetInfo(probeCtx, name)
				if infoErr != nil {
					infoState[name] = infoErr.Error()
				} else {
					infoState[name] = fmt.Sprintf("deleted=%t nuid=%s", info.Deleted, info.NUID)
				}
			}
			objectStream, streamErr := all[0].Stream(probeCtx, "OBJ_WF_BLOB")
			var objectState any
			if streamErr == nil {
				streamInfo, infoErr := objectStream.Info(probeCtx)
				if infoErr == nil {
					objectState = streamInfo.State
				} else {
					objectState = infoErr
				}
			} else {
				objectState = streamErr
			}
			t.Fatalf("%s real=%+v err=%v model=%+v err=%v; relist=%d err=%v object_infos=%v object_stream=%v", stage, real, realErr, modeled, modelErr, len(listed), listErr, infoState, objectState)
		}
	}
	compare("retained", 1)
	if err := inv.Purge(ctx, jetstream.WithPurgeSubject(invMessage.Subject)); err != nil {
		t.Fatal(err)
	}
	if err := model.Purge("WF_INV", modelInv); err != nil {
		t.Fatal(err)
	}
	signals, err := all[0].Stream(ctx, "WF_SIG")
	if err != nil {
		t.Fatal(err)
	}
	if err := signals.Purge(ctx, jetstream.WithPurgeSubject(signalMessage.Subject)); err != nil {
		t.Fatal(err)
	}
	if err := model.Purge("WF_SIG", modelSignal); err != nil {
		t.Fatal(err)
	}
	jrn, err := all[0].Stream(ctx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	if err := jrn.Purge(ctx, jetstream.WithPurgeSubject(journalSubject)); err != nil {
		t.Fatal(err)
	}
	if err := model.Purge("WF_JRN", modelStep); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"snap.test.blob-contract", "test.blob-contract"} {
		if err := state.Delete(ctx, key); err != nil {
			t.Fatal(err)
		}
	}
	if err := model.State().Delete(ctx, "snap.test.blob-contract", modelSnapshotRev); err != nil {
		t.Fatal(err)
	}
	if err := model.State().Delete(ctx, "test.blob-contract", modelTerminalRev); err != nil {
		t.Fatal(err)
	}
	compare("purged", 5)
	for _, name := range []string{"input-kept", "signal-kept", "step-result-kept", "snapshot-kept", "terminal-result-kept", "input-orphan"} {
		if _, err := objects.GetInfo(ctx, name); !errors.Is(err, jetstream.ErrObjectNotFound) || model.HasObject(name) {
			t.Fatalf("%s survived blob reclamation: %v", name, err)
		}
	}
	if _, err := objects.GetInfo(ctx, "user-unmanaged"); err != nil || !model.HasObject("user-unmanaged") {
		t.Fatalf("unmanaged object removed: %v", err)
	}
}
