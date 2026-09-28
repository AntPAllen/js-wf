package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/sim"
)

func TestSimClientSignalContractAgainstRealCluster(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	real := client.New(all[0])
	model := sim.NewSignalTransport(sim.NewScheduler(51))
	simulated := client.NewWithSignalPorts(model, model)
	const typ, id = "signal-contract", "client-port"
	input := []byte(`"input"`)
	payload := []byte(`"first"`)
	realHandle, realSeq, realErr := real.SignalWithStart(ctx, typ, id, "ready", payload, "key", input)
	modelHandle, modelSeq, modelErr := simulated.SignalWithStart(ctx, typ, id, "ready", payload, "key", input)
	if realErr != nil || modelErr != nil || realHandle.InvSeq == 0 || modelHandle.InvSeq == 0 || realSeq == 0 || modelSeq == 0 {
		t.Fatalf("signal with start: real=%+v seq=%d err=%v model=%+v seq=%d err=%v", realHandle, realSeq, realErr, modelHandle, modelSeq, modelErr)
	}
	realRetry, realDup, realErr := real.SignalWithStart(ctx, typ, id, "ready", payload, "key", input)
	modelRetry, modelDup, modelErr := simulated.SignalWithStart(ctx, typ, id, "ready", payload, "key", input)
	if realErr != nil || modelErr != nil || realRetry.InvSeq != realHandle.InvSeq || modelRetry.InvSeq != modelHandle.InvSeq || realDup != realSeq || modelDup != modelSeq {
		t.Fatalf("matching retry: real=%+v seq=%d err=%v model=%+v seq=%d err=%v", realRetry, realDup, realErr, modelRetry, modelDup, modelErr)
	}
	_, realErr = real.Signal(ctx, typ, id, "ready", []byte(`"changed"`), "key")
	_, modelErr = simulated.Signal(ctx, typ, id, "ready", []byte(`"changed"`), "key")
	if !errors.Is(realErr, client.ErrSignalMismatch) || !errors.Is(modelErr, client.ErrSignalMismatch) {
		t.Fatalf("changed retry: real=%v model=%v", realErr, modelErr)
	}
	realNext, realErr := real.SignalWithOptions(ctx, typ, id, "go", []byte(`"second"`), "next", client.SignalOptions{RequireRunning: true})
	modelNext, modelErr := simulated.SignalWithOptions(ctx, typ, id, "go", []byte(`"second"`), "next", client.SignalOptions{RequireRunning: true})
	if realErr != nil || modelErr != nil || realNext <= realSeq || modelNext <= modelSeq {
		t.Fatalf("running signal: real=%d err=%v model=%d err=%v", realNext, realErr, modelNext, modelErr)
	}
	realSignals, err := all[1].Stream(ctx, "WF_SIG")
	if err != nil {
		t.Fatal(err)
	}
	signalInfo, err := realSignals.Info(ctx)
	if err != nil || signalInfo.State.Msgs != 2 || len(model.SignalFor(typ, id, "ready")) != 1 || len(model.SignalFor(typ, id, "go")) != 1 {
		t.Fatalf("retained signals: info=%+v model ready=%v go=%v err=%v", signalInfo, model.SignalFor(typ, id, "ready"), model.SignalFor(typ, id, "go"), err)
	}
	realRuns, err := all[2].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	runInfo, err := realRuns.Info(ctx)
	if err != nil || runInfo.State.Msgs != 3 || len(model.Runs()) != 3 {
		t.Fatalf("retained wakeups: info=%+v model=%v err=%v", runInfo, model.Runs(), err)
	}
}
