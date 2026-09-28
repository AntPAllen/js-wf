package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/sim"
	"js-wf/worker"

	"github.com/nats-io/nats.go"
)

func TestSimSignalDrainContractAgainstRealCluster(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	model := sim.NewSignalTransport(sim.NewScheduler(62))
	realClient := client.New(all[0])
	modelClient := client.NewWithSignalPorts(model, model)
	const typ, id = "signal-contract", "drain-port"
	invocation := &nats.Msg{Subject: identity.InvocationSubject(typ, id), Data: []byte("input")}
	realInv, err := all[0].PublishMsg(ctx, invocation)
	if err != nil {
		t.Fatal(err)
	}
	modelGeneration, err := model.PublishInvocation(ctx, invocation)
	if err != nil {
		t.Fatal(err)
	}
	realFirst, err := realClient.Signal(ctx, typ, id, "go", []byte("first"), "first")
	if err != nil {
		t.Fatal(err)
	}
	modelFirst, err := modelClient.Signal(ctx, typ, id, "go", []byte("first"), "first")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := all[0].Publish(ctx, "wf.sig.signal-contract.other.go", []byte("unrelated")); err != nil {
		t.Fatal(err)
	}
	model.CommitSignal(&nats.Msg{Subject: "wf.sig.signal-contract.other.go", Data: []byte("unrelated")})
	signal := func(generation uint64, name string) *nats.Msg {
		message := &nats.Msg{Subject: "wf.sig." + typ + "." + id + "." + name, Data: []byte(name), Header: nats.Header{}}
		message.Header.Set("Wf-Inv-Seq", strconv.FormatUint(generation, 10))
		return message
	}
	if _, err := all[0].PublishMsg(ctx, signal(realInv.Sequence+100, "stale")); err != nil {
		t.Fatal(err)
	}
	model.CommitSignal(signal(modelGeneration+100, "stale"))
	realHole, err := all[0].PublishMsg(ctx, signal(realInv.Sequence, "deleted"))
	if err != nil {
		t.Fatal(err)
	}
	modelHole := model.CommitSignal(signal(modelGeneration, "deleted"))
	realStream, err := all[0].Stream(ctx, "WF_SIG")
	if err != nil {
		t.Fatal(err)
	}
	if err := realStream.DeleteMsg(ctx, realHole.Sequence); err != nil {
		t.Fatal(err)
	}
	model.PurgeSignal(modelHole)
	realSecond, err := realClient.Signal(ctx, typ, id, "later", []byte("second"), "second")
	if err != nil {
		t.Fatal(err)
	}
	modelSecond, err := modelClient.Signal(ctx, typ, id, "later", []byte("second"), "second")
	if err != nil {
		t.Fatal(err)
	}
	observer, err := all[1].Stream(ctx, "WF_SIG")
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := observer.GetMsg(ctx, realSecond); err == nil {
			break
		} else if ctx.Err() != nil {
			t.Fatalf("second signal never visible on observer: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	var realRecords, modelRecords []journal.Record
	appendTo := func(records *[]journal.Record) func(journal.Kind, json.RawMessage) error {
		return func(kind journal.Kind, payload json.RawMessage) error {
			*records = append(*records, journal.Record{Entry: journal.Entry{Kind: kind, Index: uint64(len(*records)), Payload: append([]byte(nil), payload...)}})
			return nil
		}
	}
	realSignals, realErr := worker.DrainSignalsWithPort(ctx, worker.NewSignalDrainPort(all[1]), typ, id, realInv.Sequence, nil, appendTo(&realRecords))
	modelSignals, modelErr := worker.DrainSignalsWithPort(ctx, model, typ, id, modelGeneration, nil, appendTo(&modelRecords))
	if realErr != nil || modelErr != nil || len(realSignals) != 2 || len(modelSignals) != 2 || len(realRecords) != 2 || len(modelRecords) != 2 {
		t.Fatalf("drain: real=%+v records=%d err=%v model=%+v records=%d err=%v", realSignals, len(realRecords), realErr, modelSignals, len(modelRecords), modelErr)
	}
	for i, expected := range []struct {
		name    string
		payload []byte
		realSeq uint64
		modSeq  uint64
	}{{"go", []byte("first"), realFirst, modelFirst}, {"later", []byte("second"), realSecond, modelSecond}} {
		if realSignals[i].Name != expected.name || modelSignals[i].Name != expected.name || !bytes.Equal(realSignals[i].Payload, expected.payload) || !bytes.Equal(modelSignals[i].Payload, expected.payload) || realSignals[i].Sequence != expected.realSeq || modelSignals[i].Sequence != expected.modSeq {
			t.Fatalf("signal %d differs: real=%+v model=%+v", i, realSignals[i], modelSignals[i])
		}
	}
	realReplay, realErr := worker.DrainSignalsWithPort(ctx, worker.NewSignalDrainPort(all[2]), typ, id, realInv.Sequence, realRecords, appendTo(&realRecords))
	modelReplay, modelErr := worker.DrainSignalsWithPort(ctx, model, typ, id, modelGeneration, modelRecords, appendTo(&modelRecords))
	if realErr != nil || modelErr != nil || len(realReplay) != 2 || len(modelReplay) != 2 || len(realRecords) != 2 || len(modelRecords) != 2 {
		t.Fatalf("replay: real=%+v records=%d err=%v model=%+v records=%d err=%v", realReplay, len(realRecords), realErr, modelReplay, len(modelRecords), modelErr)
	}
}
