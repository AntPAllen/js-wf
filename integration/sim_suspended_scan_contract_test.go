package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/reconcile"
	"js-wf/sim"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestSimSuspendedScanContractAgainstRealCluster(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	model := sim.NewSignalTransport(sim.NewScheduler(83))
	realPort := reconcile.NewSuspendedScanPort(all[2])
	realScan := reconcile.NewSuspendedScanWithPort(realPort)
	modelScan := reconcile.NewSuspendedScanWithPort(model)
	base := time.Now().UTC()
	realScan.Now = func() time.Time { return base }
	modelScan.Now = func() time.Time { return base }
	c := client.New(all[0])
	j := journal.New(all[0])
	var signalGeneration uint64
	run, err := all[0].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	for _, fixture := range []struct {
		id        string
		request   any
		waitingOn string
	}{
		{"suspended-contract-timer", struct {
			Kind   string    `json:"kind"`
			Name   string    `json:"name"`
			FireAt time.Time `json:"fire_at"`
		}{"timer", "sleep", base.Add(-2 * time.Second)}, "timer:sleep"},
		{"suspended-contract-signal", struct {
			Kind string `json:"kind"`
			Name string `json:"name"`
		}{"signal", "go"}, "signal:go"},
	} {
		handle, err := c.Start(ctx, "test", fixture.id, []byte(`null`))
		if err != nil {
			t.Fatal(err)
		}
		if fixture.id == "suspended-contract-signal" {
			signalGeneration = handle.InvSeq
		}
		modeled, err := model.PublishInvocation(ctx, &nats.Msg{Subject: identity.InvocationSubject("test", fixture.id), Data: []byte(`null`)})
		if err != nil || modeled != handle.InvSeq {
			t.Fatalf("invocation %s real=%d model=%d err=%v", fixture.id, handle.InvSeq, modeled, err)
		}
		payload, err := json.Marshal(fixture.request)
		if err != nil {
			t.Fatal(err)
		}
		seq, err := j.Append(ctx, "test", fixture.id, journal.Entry{Index: 0, Kind: journal.Started}, 0)
		if err != nil {
			t.Fatal(err)
		}
		seq, err = j.Append(ctx, "test", fixture.id, journal.Entry{Index: 1, Kind: journal.StepRequested, Payload: payload}, seq)
		if err != nil {
			t.Fatal(err)
		}
		waiting, err := json.Marshal(struct {
			WaitingOn string `json:"waiting_on"`
		}{fixture.waitingOn})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := j.Append(ctx, "test", fixture.id, journal.Entry{Index: 2, Kind: journal.Suspended, Payload: waiting}, seq); err != nil {
			t.Fatal(err)
		}
		records, _, err := j.Read(ctx, "test", fixture.id)
		if err != nil {
			t.Fatal(err)
		}
		model.SetJournal("test", fixture.id, records)
	}
	if err := run.Purge(ctx); err != nil {
		t.Fatal(err)
	}
	sig, err := all[0].Stream(ctx, "WF_SIG")
	if err != nil {
		t.Fatal(err)
	}
	subject := "wf.sig.test.suspended-contract-signal.go"
	publish := func(name string, generation uint64) uint64 {
		t.Helper()
		msg := &nats.Msg{Subject: "wf.sig.test.suspended-contract-signal." + name, Data: []byte(name), Header: nats.Header{}}
		msg.Header.Set("Wf-Inv-Seq", strconv.FormatUint(generation, 10))
		real, err := all[0].PublishMsg(ctx, msg)
		if err != nil {
			t.Fatal(err)
		}
		modeled := model.CommitSignal(msg)
		if real.Sequence != modeled {
			t.Fatalf("signal sequence real=%d model=%d", real.Sequence, modeled)
		}
		return real.Sequence
	}
	first := publish("other", signalGeneration)
	hole := publish("go", signalGeneration)
	stale := publish("go", signalGeneration+2000)
	valid := publish("go", signalGeneration)
	if err := sig.DeleteMsg(ctx, hole); err != nil {
		t.Fatal(err)
	}
	model.PurgeSignal(hole)
	for _, from := range []uint64{first, hole, stale, valid, valid + 1} {
		real, realErr := realPort.GetSignalAfter(ctx, subject, from)
		modeled, modelErr := model.GetSignalAfter(ctx, subject, from)
		if errors.Is(realErr, jetstream.ErrMsgNotFound) && errors.Is(modelErr, jetstream.ErrMsgNotFound) {
			continue
		}
		if realErr != nil || modelErr != nil || real.Sequence != modeled.Sequence || !reflect.DeepEqual(real.Data, modeled.Data) || !reflect.DeepEqual(real.Header, modeled.Header) {
			t.Fatalf("filtered signal from=%d real=%+v %v model=%+v %v", from, real, realErr, modeled, modelErr)
		}
	}
	if real, err := realScan.Scan(ctx, 1, 2, true); err != nil || real.Reenqueued != 2 || len(real.Candidates) != 2 {
		t.Fatalf("real dry scan=%+v err=%v", real, err)
	}
	modelDry, err := modelScan.Scan(ctx, 1, 2, true)
	if err != nil || modelDry.Reenqueued != 2 || len(model.Runs()) != 0 {
		t.Fatalf("model dry scan=%+v err=%v", modelDry, err)
	}
	realRepaired, realErr := realScan.Scan(ctx, 1, 2, false)
	modelRepaired, modelErr := modelScan.Scan(ctx, 1, 2, false)
	if realErr != nil || modelErr != nil || !reflect.DeepEqual(realRepaired, modelRepaired) || len(model.Runs()) != 2 {
		t.Fatalf("repair real=%+v %v model=%+v %v runs=%d", realRepaired, realErr, modelRepaired, modelErr, len(model.Runs()))
	}
	if _, err := realScan.Scan(ctx, 1, 2, false); err != nil {
		t.Fatal(err)
	}
	if _, err := modelScan.Scan(ctx, 1, 2, false); err != nil {
		t.Fatal(err)
	}
	info, err := run.Info(ctx)
	if err != nil || info.State.Msgs != 2 || len(model.Runs()) != 2 {
		t.Fatalf("deduplicated runs real=%+v %v model=%d", info, err, len(model.Runs()))
	}
}
