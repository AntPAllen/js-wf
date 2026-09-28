package integration_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"js-wf/identity"
	"js-wf/reconcile"
	"js-wf/sim"

	"github.com/nats-io/nats.go"
)

func TestSimSignalRepairContractAgainstRealCluster(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	model := sim.NewSignalTransport(sim.NewScheduler(31))
	const typ, id = "signal-contract", "missing-wakeup"
	invocation := &nats.Msg{Subject: identity.InvocationSubject(typ, id), Data: []byte("input")}
	realInvocation, err := all[0].PublishMsg(ctx, invocation)
	if err != nil {
		t.Fatal(err)
	}
	modelGeneration, err := model.PublishInvocation(ctx, invocation)
	if err != nil {
		t.Fatal(err)
	}
	message := func(generation uint64) *nats.Msg {
		msg := &nats.Msg{Subject: "wf.sig." + typ + "." + id + ".go", Data: []byte("payload"), Header: nats.Header{}}
		msg.Header.Set("Wf-Inv-Seq", strconv.FormatUint(generation, 10))
		return msg
	}
	realSignal, err := all[0].PublishMsg(ctx, message(realInvocation.Sequence))
	if err != nil {
		t.Fatal(err)
	}
	modelSequence := model.PublishSignal(message(modelGeneration))
	realScan := reconcile.NewSignalScan(all[1])
	modelScan := reconcile.NewSignalScanWithPort(model)
	realDry, realErr := realScan.Scan(ctx, realSignal.Sequence, 1, true)
	modelDry, modelErr := modelScan.Scan(ctx, modelSequence, 1, true)
	if realErr != nil || modelErr != nil || realDry.Reenqueued != 1 || modelDry.Reenqueued != 1 || len(model.Runs()) != 0 {
		t.Fatalf("dry repair: real=%+v %v model=%+v %v runs=%d", realDry, realErr, modelDry, modelErr, len(model.Runs()))
	}
	realRepaired, realErr := realScan.Scan(ctx, realSignal.Sequence, 1, false)
	modelRepaired, modelErr := modelScan.Scan(ctx, modelSequence, 1, false)
	if realErr != nil || modelErr != nil || realRepaired.Reenqueued != 1 || modelRepaired.Reenqueued != 1 || len(model.Runs()) != 1 {
		t.Fatalf("repair: real=%+v %v model=%+v %v runs=%d", realRepaired, realErr, modelRepaired, modelErr, len(model.Runs()))
	}
	realAgain, realErr := realScan.Scan(ctx, realSignal.Sequence, 1, false)
	modelAgain, modelErr := modelScan.Scan(ctx, modelSequence, 1, false)
	if realErr != nil || modelErr != nil || realAgain.Reenqueued != 1 || modelAgain.Reenqueued != 1 || len(model.Runs()) != 1 {
		t.Fatalf("deduplicated repair: real=%+v %v model=%+v %v runs=%d", realAgain, realErr, modelAgain, modelErr, len(model.Runs()))
	}
	realStale, err := all[0].PublishMsg(ctx, message(realInvocation.Sequence+100))
	if err != nil {
		t.Fatal(err)
	}
	modelStale := model.PublishSignal(message(modelGeneration + 100))
	realSkipped, realErr := realScan.Scan(ctx, realStale.Sequence, 1, false)
	modelSkipped, modelErr := modelScan.Scan(ctx, modelStale, 1, false)
	if realErr != nil || modelErr != nil || realSkipped.Reenqueued != 0 || modelSkipped.Reenqueued != 0 || len(model.Runs()) != 1 {
		t.Fatalf("stale generation: real=%+v %v model=%+v %v runs=%d", realSkipped, realErr, modelSkipped, modelErr, len(model.Runs()))
	}
	realHole, err := all[0].PublishMsg(ctx, message(realInvocation.Sequence))
	if err != nil {
		t.Fatal(err)
	}
	modelHole := model.PublishSignal(message(modelGeneration))
	sig, err := all[0].Stream(ctx, "WF_SIG")
	if err != nil {
		t.Fatal(err)
	}
	if err := sig.DeleteMsg(ctx, realHole.Sequence); err != nil {
		t.Fatal(err)
	}
	model.PurgeSignal(modelHole)
	realSkipped, realErr = realScan.Scan(ctx, realHole.Sequence, 1, false)
	modelSkipped, modelErr = modelScan.Scan(ctx, modelHole, 1, false)
	if realErr != nil || modelErr != nil || realSkipped.Inspected != 0 || modelSkipped.Inspected != 0 || len(model.Runs()) != 1 {
		t.Fatalf("purged sequence hole: real=%+v %v model=%+v %v runs=%d", realSkipped, realErr, modelSkipped, modelErr, len(model.Runs()))
	}
	run, err := all[2].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	info, err := run.Info(ctx)
	if err != nil || info.State.Msgs != 1 {
		t.Fatalf("retained real wakeups: info=%+v err=%v", info, err)
	}
}
