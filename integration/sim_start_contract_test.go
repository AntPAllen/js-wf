package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/reconcile"
	"js-wf/sim"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestSimStartContractAgainstRealCluster(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	real := client.New(all[0])
	model := sim.NewStartTransport(sim.NewScheduler(1))
	simulated := client.NewWithStartPort(model)
	const typ, id = "start-contract", "same-id"
	input := []byte(`{"value":1}`)
	realFirst, realErr := real.Start(ctx, typ, id, input)
	modelFirst, modelErr := simulated.Start(ctx, typ, id, input)
	if realErr != nil || modelErr != nil || realFirst.InvSeq == 0 || modelFirst.InvSeq == 0 {
		t.Fatalf("first start: real=%+v %v model=%+v %v", realFirst, realErr, modelFirst, modelErr)
	}
	realRetry, realErr := real.Start(ctx, typ, id, input)
	modelRetry, modelErr := simulated.Start(ctx, typ, id, input)
	if !errors.Is(realErr, client.ErrAlreadyStarted) || !errors.Is(modelErr, client.ErrAlreadyStarted) || realRetry.InvSeq != realFirst.InvSeq || modelRetry.InvSeq != modelFirst.InvSeq {
		t.Fatalf("matching retry: real=%+v %v model=%+v %v", realRetry, realErr, modelRetry, modelErr)
	}
	_, realErr = real.Start(ctx, typ, id, []byte(`{"value":2}`))
	_, modelErr = simulated.Start(ctx, typ, id, []byte(`{"value":2}`))
	if !errors.Is(realErr, client.ErrInputMismatch) || !errors.Is(modelErr, client.ErrInputMismatch) {
		t.Fatalf("changed retry: real=%v model=%v", realErr, modelErr)
	}
	realInv, err := all[1].Stream(ctx, "WF_INV")
	if err != nil {
		t.Fatal(err)
	}
	realStored, err := realInv.GetLastMsgForSubject(ctx, identity.InvocationSubject(typ, id))
	if err != nil || realStored.Sequence != realFirst.InvSeq || len(model.Runs()) != 1 {
		t.Fatalf("retained invocation/run: real=%+v err=%v model runs=%v", realStored, err, model.Runs())
	}
	realRun, err := all[2].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	info, err := realRun.Info(ctx)
	if err != nil || info.State.Msgs != 1 {
		t.Fatalf("retained real run: info=%+v err=%v", info, err)
	}
}

// Isolate the invocation CAS from WF_INV's usual per-subject limit. This is
// the real-server contract for the model's bypass_subject_limit fault.
func TestStartSubjectTailCASWithoutSubjectLimit(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	inv, err := all[0].Stream(ctx, "WF_INV")
	if err != nil {
		t.Fatal(err)
	}
	info, err := inv.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	normalSubject := identity.InvocationSubject("cas-contract", "normal-limit")
	normal := &nats.Msg{Subject: normalSubject, Data: []byte(`1`), Header: nats.Header{}}
	normal.Header.Set(jetstream.ExpectedLastSubjSeqHeader, "0")
	if _, err := all[0].PublishMsg(ctx, normal); err != nil {
		t.Fatalf("first guarded publish with subject limit: %v", err)
	}
	if _, err := all[0].PublishMsg(ctx, normal); err == nil {
		t.Fatal("second guarded publish passed both CAS and subject limit")
	} else {
		var apiErr *jetstream.APIError
		if !errors.As(err, &apiErr) || apiErr.ErrorCode != jetstream.JSErrCodeStreamWrongLastSequence {
			t.Fatalf("guarded duplicate should report CAS rejection before subject limit: %v", err)
		}
	}
	config := info.Config
	config.MaxMsgsPerSubject = -1
	config.DiscardNewPerSubject = false
	if _, err := all[0].UpdateStream(ctx, config); err != nil {
		t.Fatalf("remove per-subject limit: %v", err)
	}
	const typ, id = "cas-contract", "same-id"
	input := []byte(`{"value":1}`)
	first, err := client.New(all[0]).Start(ctx, typ, id, input)
	if err != nil || first.InvSeq == 0 {
		t.Fatalf("first start: handle=%+v err=%v", first, err)
	}
	retry, err := client.New(all[1]).Start(ctx, typ, id, input)
	if !errors.Is(err, client.ErrAlreadyStarted) || retry.InvSeq != first.InvSeq {
		t.Fatalf("CAS-protected retry: first=%+v retry=%+v err=%v", first, retry, err)
	}
	stored, err := inv.GetLastMsgForSubject(ctx, identity.InvocationSubject(typ, id))
	if err != nil || stored.Sequence != first.InvSeq {
		t.Fatalf("retained CAS generation: message=%+v err=%v", stored, err)
	}
	// Without the header, this deliberately permissive stream accepts a new
	// generation. That confirms the preceding rejection came from the CAS.
	ack, err := all[2].PublishMsg(ctx, &nats.Msg{Subject: identity.InvocationSubject(typ, id), Data: input})
	if err != nil || ack.Sequence <= first.InvSeq {
		t.Fatalf("unguarded publish: ack=%+v err=%v", ack, err)
	}
	stored, err = inv.GetLastMsgForSubject(ctx, identity.InvocationSubject(typ, id))
	if err != nil || stored.Sequence != ack.Sequence {
		t.Fatalf("unguarded generation not retained: message=%+v ack=%+v err=%v", stored, ack, err)
	}
}

func TestSimRunDedupWindowExpiryAgainstRealCluster(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	run, err := all[0].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	runInfo, err := run.Info(ctx)
	if err != nil || runInfo.Config.Duplicates != 2*time.Minute {
		t.Fatalf("WF_RUN duplicate window: info=%+v err=%v", runInfo, err)
	}
	const subject, messageID = "wf.sim.run.dedup", "start:test.one:1"
	stream, err := all[0].CreateStream(ctx, jetstream.StreamConfig{
		Name: "SIM_RUN_DEDUP", Subjects: []string{subject}, Storage: jetstream.FileStorage,
		Replicas: 3, Duplicates: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	model := sim.NewStartTransport(sim.NewScheduler(19))
	if err := model.SetRunDedupWindow(time.Second); err != nil {
		t.Fatal(err)
	}
	msg := &nats.Msg{Subject: subject, Data: []byte("test.one")}
	publish := func() *jetstream.PubAck {
		t.Helper()
		ack, err := all[0].PublishMsg(ctx, msg, jetstream.WithMsgID(messageID))
		if err != nil {
			t.Fatal(err)
		}
		if err := model.EnqueueRun(ctx, subject, msg.Data, messageID); err != nil {
			t.Fatal(err)
		}
		return ack
	}
	first := publish()
	second := publish()
	if first.Duplicate || !second.Duplicate || first.Sequence != second.Sequence || len(model.Runs()) != 1 {
		t.Fatalf("inside-window dedup: first=%+v second=%+v model=%v", first, second, model.Runs())
	}
	time.Sleep(3 * time.Second)
	if err := model.Wait(ctx, 3*time.Second); err != nil {
		t.Fatal(err)
	}
	third := publish()
	if third.Duplicate || third.Sequence <= first.Sequence || len(model.Runs()) != 2 {
		t.Fatalf("expired-window publish: first=%+v third=%+v model=%v", first, third, model.Runs())
	}
	info, err := stream.Info(ctx)
	if err != nil || info.State.Msgs != 2 {
		t.Fatalf("retained runs after expiry: info=%+v err=%v", info, err)
	}
	// Removing Nats-Msg-Id must leave two distinct run messages. This is
	// the transport negative control for the missing-ID chaos mutation.
	noID := &nats.Msg{Subject: subject, Data: []byte("without-id")}
	for i := 0; i < 2; i++ {
		ack, publishErr := all[0].PublishMsg(ctx, noID)
		if publishErr != nil || ack.Duplicate || ack.Sequence != third.Sequence+uint64(i)+1 {
			t.Fatalf("publish without message ID %d: ack=%+v err=%v", i, ack, publishErr)
		}
		if err := model.EnqueueRun(ctx, subject, noID.Data, ""); err != nil {
			t.Fatal(err)
		}
	}
	info, err = stream.Info(ctx)
	if err != nil || info.State.Msgs != 4 || len(model.Runs()) != 4 {
		t.Fatalf("missing-ID contract: real=%+v err=%v model runs=%d", info, err, len(model.Runs()))
	}
}

func TestSimStartRepairContractAgainstRealCluster(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	model := sim.NewStartTransport(sim.NewScheduler(2))
	const typ, id = "start-contract", "missing-run"
	msg := &nats.Msg{Subject: identity.InvocationSubject(typ, id), Data: []byte(`input`), Header: nats.Header{}}
	realAck, err := all[0].PublishMsg(ctx, msg)
	if err != nil {
		t.Fatal(err)
	}
	modelSequence, err := model.PublishInvocation(ctx, msg)
	if err != nil || modelSequence == 0 || realAck.Sequence == 0 {
		t.Fatalf("unreconciled invocation: real=%+v model=%d err=%v", realAck, modelSequence, err)
	}
	realScan := reconcile.NewStartScan(all[1])
	modelScan := reconcile.NewStartScanWithPort(model)
	realDry, realErr := realScan.Scan(ctx, realAck.Sequence, 1, true)
	modelDry, modelErr := modelScan.Scan(ctx, modelSequence, 1, true)
	if realErr != nil || modelErr != nil || realDry.Reenqueued != 1 || modelDry.Reenqueued != 1 || len(model.Runs()) != 0 {
		t.Fatalf("dry repair: real=%+v %v model=%+v %v", realDry, realErr, modelDry, modelErr)
	}
	realRepaired, realErr := realScan.Scan(ctx, realAck.Sequence, 1, false)
	modelRepaired, modelErr := modelScan.Scan(ctx, modelSequence, 1, false)
	if realErr != nil || modelErr != nil || realRepaired.Reenqueued != 1 || modelRepaired.Reenqueued != 1 || len(model.Runs()) != 1 {
		t.Fatalf("repair: real=%+v %v model=%+v %v", realRepaired, realErr, modelRepaired, modelErr)
	}
	realAgain, realErr := realScan.Scan(ctx, realAck.Sequence, 1, false)
	modelAgain, modelErr := modelScan.Scan(ctx, modelSequence, 1, false)
	if realErr != nil || modelErr != nil || realAgain.Reenqueued != 1 || modelAgain.Reenqueued != 1 || len(model.Runs()) != 1 {
		t.Fatalf("deduplicated repair: real=%+v %v model=%+v %v", realAgain, realErr, modelAgain, modelErr)
	}
	realRun, err := all[2].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	info, err := realRun.Info(ctx)
	if err != nil || info.State.Msgs != 1 {
		t.Fatalf("real repair run count: info=%+v err=%v", info, err)
	}
}
