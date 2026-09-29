package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"js-wf/sim"

	"github.com/nats-io/nats.go/jetstream"
)

func TestSimDispatchProgressExtendsAckWaitContract(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	const ackWait = 4 * time.Second
	model := sim.NewDispatchTransport(sim.NewScheduler(19), ackWait)
	model.PublishRun("wf.run.0", []byte(`work`))
	modelConsumer, err := model.Consumer(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	run, err := all[0].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := all[0].Publish(ctx, "wf.run.0", []byte(`work`)); err != nil {
		t.Fatal(err)
	}
	realConsumer, err := run.CreateConsumer(ctx, jetstream.ConsumerConfig{
		Name: "WF_SIM_PROGRESS", Durable: "WF_SIM_PROGRESS", FilterSubject: "wf.run.0",
		AckPolicy: jetstream.AckExplicitPolicy, AckWait: ackWait, MaxDeliver: -1, MaxAckPending: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	realFirst, err := realConsumer.Fetch(1, jetstream.FetchMaxWait(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	firstReal := <-realFirst.Messages()
	firstModelBatch, err := modelConsumer.FetchOne(ctx)
	if err != nil {
		t.Fatal(err)
	}
	firstModel := <-firstModelBatch.Messages()
	if firstReal == nil || firstModel == nil {
		t.Fatal("first delivery missing")
	}
	time.Sleep(time.Second)
	if err := firstReal.InProgress(); err != nil {
		t.Fatal(err)
	}
	if err := model.Wait(ctx, time.Second); err != nil {
		t.Fatal(err)
	}
	if err := firstModel.InProgress(); err != nil {
		t.Fatal(err)
	}
	// This crosses the original deadline at four seconds while remaining
	// before the five-second deadline set by InProgress.
	time.Sleep(3100 * time.Millisecond)
	noReal, err := realConsumer.FetchNoWait(1)
	if err != nil {
		t.Fatal(err)
	}
	for message := range noReal.Messages() {
		t.Fatalf("real consumer redelivered early: %q", message.Data())
	}
	if err := noReal.Error(); err != nil && !errors.Is(err, jetstream.ErrNoMessages) {
		t.Fatal(err)
	}
	if err := model.Wait(ctx, 3100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if _, err := modelConsumer.FetchOne(ctx); !errors.Is(err, jetstream.ErrNoMessages) {
		t.Fatalf("model redelivered early: %v", err)
	}
	realSecond, err := realConsumer.Fetch(1, jetstream.FetchMaxWait(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	secondReal := <-realSecond.Messages()
	secondModelBatch, err := modelConsumer.FetchOne(ctx)
	if err != nil {
		t.Fatal(err)
	}
	secondModel := <-secondModelBatch.Messages()
	if secondReal == nil || secondModel == nil {
		t.Fatal("redelivery after extended deadline missing")
	}
	realMeta, realErr := secondReal.Metadata()
	modelMeta, modelErr := secondModel.Metadata()
	if realErr != nil || modelErr != nil || realMeta.NumDelivered != 2 || modelMeta.NumDelivered != 2 {
		t.Fatalf("redelivery metadata real=%+v %v model=%+v %v", realMeta, realErr, modelMeta, modelErr)
	}
	if err := secondReal.DoubleAck(ctx); err != nil {
		t.Fatal(err)
	}
	if err := secondModel.Ack(); err != nil {
		t.Fatal(err)
	}
}

func TestSimDispatchAckWaitContractAgainstRealCluster(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	const ackWait = time.Second
	model := sim.NewDispatchTransport(sim.NewScheduler(1), ackWait)
	model.PublishRun("wf.run.0", []byte(`work`))
	modeledConsumer, err := model.Consumer(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	realRun, err := all[0].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := all[0].Publish(ctx, "wf.run.0", []byte(`work`)); err != nil {
		t.Fatal(err)
	}
	realConsumer, err := realRun.CreateConsumer(ctx, jetstream.ConsumerConfig{
		Name: "WF_SIM_DISPATCH", Durable: "WF_SIM_DISPATCH", FilterSubject: "wf.run.0",
		AckPolicy: jetstream.AckExplicitPolicy, AckWait: ackWait, MaxDeliver: -1, MaxAckPending: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	realFirst, err := realConsumer.Fetch(1, jetstream.FetchMaxWait(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	firstReal := <-realFirst.Messages()
	firstModelBatch, err := modeledConsumer.FetchOne(ctx)
	if err != nil {
		t.Fatal(err)
	}
	firstModel := <-firstModelBatch.Messages()
	realMetadata, realErr := firstReal.Metadata()
	modelMetadata, modelErr := firstModel.Metadata()
	if realErr != nil || modelErr != nil || realMetadata.NumDelivered != 1 || modelMetadata.NumDelivered != 1 {
		t.Fatalf("first delivery: real=%+v %v model=%+v %v", realMetadata, realErr, modelMetadata, modelErr)
	}
	if err := model.Wait(ctx, ackWait); err != nil {
		t.Fatal(err)
	}
	realSecond, err := realConsumer.Fetch(1, jetstream.FetchMaxWait(4*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	secondReal := <-realSecond.Messages()
	secondModelBatch, err := modeledConsumer.FetchOne(ctx)
	if err != nil {
		t.Fatal(err)
	}
	secondModel := <-secondModelBatch.Messages()
	realMetadata, realErr = secondReal.Metadata()
	modelMetadata, modelErr = secondModel.Metadata()
	if realErr != nil || modelErr != nil || realMetadata.NumDelivered != 2 || modelMetadata.NumDelivered != 2 {
		t.Fatalf("redelivery: real=%+v %v model=%+v %v", realMetadata, realErr, modelMetadata, modelErr)
	}
	if err := secondReal.DoubleAck(ctx); err != nil {
		t.Fatal(err)
	}
	if err := secondModel.Ack(); err != nil {
		t.Fatal(err)
	}
	if model.Pending() != 0 {
		t.Fatalf("model retained %d pending messages", model.Pending())
	}
	info, err := realConsumer.Info(ctx)
	if err != nil || info.NumAckPending != 0 {
		t.Fatalf("real consumer ack state=%+v err=%v", info, err)
	}
}

func TestSimDispatchDroppedNakHandoffContract(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 18*time.Second)
	defer cancel()
	const ackWait = 3 * time.Second
	model := sim.NewDispatchTransport(sim.NewScheduler(23), ackWait)
	model.PublishRun("wf.run.0", []byte(`original`))
	modeledConsumer, err := model.Consumer(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	realRun, err := all[0].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := all[0].Publish(ctx, "wf.run.0", []byte(`original`)); err != nil {
		t.Fatal(err)
	}
	realConsumer, err := realRun.CreateConsumer(ctx, jetstream.ConsumerConfig{
		Name: "WF_SIM_NAK_DROP", Durable: "WF_SIM_NAK_DROP", FilterSubject: "wf.run.0",
		AckPolicy: jetstream.AckExplicitPolicy, AckWait: ackWait, MaxDeliver: -1, MaxAckPending: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	realFirstBatch, err := realConsumer.Fetch(1, jetstream.FetchMaxWait(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	realFirst := <-realFirstBatch.Messages()
	modelFirstBatch, err := modeledConsumer.FetchOne(ctx)
	if err != nil {
		t.Fatal(err)
	}
	modelFirst := <-modelFirstBatch.Messages()
	if realFirst == nil || modelFirst == nil || string(realFirst.Data()) != "original" || string(modelFirst.Data()) != "original" {
		t.Fatalf("initial delivery real=%v model=%v", realFirst, modelFirst)
	}
	// Skipping the real Nak is equivalent to dropping it before the server
	// commits it. The model records that fault and returns an unknown reply.
	if err := model.QueueFault(sim.DispatchFault{Operation: "nak", Kind: "drop_before_commit"}); err != nil {
		t.Fatal(err)
	}
	if err := modelFirst.Nak(); !errors.Is(err, sim.ErrTransportLost) {
		t.Fatalf("modeled dropped nak: %v", err)
	}
	model.PublishRun("wf.run.0", []byte(`handoff`))
	if _, err := all[0].Publish(ctx, "wf.run.0", []byte(`handoff`)); err != nil {
		t.Fatal(err)
	}
	realHandoffBatch, err := realConsumer.Fetch(1, jetstream.FetchMaxWait(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	realHandoff := <-realHandoffBatch.Messages()
	modelHandoffBatch, err := modeledConsumer.FetchOne(ctx)
	if err != nil {
		t.Fatal(err)
	}
	modelHandoff := <-modelHandoffBatch.Messages()
	if realHandoff == nil || modelHandoff == nil || string(realHandoff.Data()) != "handoff" || string(modelHandoff.Data()) != "handoff" {
		t.Fatalf("handoff delivery real=%v model=%v", realHandoff, modelHandoff)
	}
	if err := realHandoff.DoubleAck(ctx); err != nil {
		t.Fatal(err)
	}
	if err := modelHandoff.Ack(); err != nil {
		t.Fatal(err)
	}
	if err := model.Wait(ctx, ackWait); err != nil {
		t.Fatal(err)
	}
	realRedeliveryBatch, err := realConsumer.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	realRedelivery := <-realRedeliveryBatch.Messages()
	modelRedeliveryBatch, err := modeledConsumer.FetchOne(ctx)
	if err != nil {
		t.Fatal(err)
	}
	modelRedelivery := <-modelRedeliveryBatch.Messages()
	if realRedelivery == nil || modelRedelivery == nil || string(realRedelivery.Data()) != "original" || string(modelRedelivery.Data()) != "original" {
		t.Fatalf("original redelivery real=%v model=%v", realRedelivery, modelRedelivery)
	}
	realMeta, realErr := realRedelivery.Metadata()
	modelMeta, modelErr := modelRedelivery.Metadata()
	if realErr != nil || modelErr != nil || realMeta.NumDelivered != 2 || modelMeta.NumDelivered != 2 {
		t.Fatalf("redelivery count real=%+v %v model=%+v %v", realMeta, realErr, modelMeta, modelErr)
	}
	if err := realRedelivery.DoubleAck(ctx); err != nil {
		t.Fatal(err)
	}
	if err := modelRedelivery.Ack(); err != nil {
		t.Fatal(err)
	}
	if model.Pending() != 0 {
		t.Fatalf("model retained %d pending messages", model.Pending())
	}
	info, err := realConsumer.Info(ctx)
	if err != nil || info.NumAckPending != 0 || info.NumPending != 0 {
		t.Fatalf("real consumer state=%+v err=%v", info, err)
	}
}

func TestSimDispatchDelayedNakContract(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	const ackWait = 4 * time.Second
	const nakDelay = time.Second
	model := sim.NewDispatchTransport(sim.NewScheduler(24), ackWait)
	model.PublishRun("wf.run.0", []byte(`work`))
	modeledConsumer, err := model.Consumer(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	realRun, err := all[0].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := all[0].Publish(ctx, "wf.run.0", []byte(`work`)); err != nil {
		t.Fatal(err)
	}
	realConsumer, err := realRun.CreateConsumer(ctx, jetstream.ConsumerConfig{
		Name: "WF_SIM_DELAYED_NAK", Durable: "WF_SIM_DELAYED_NAK", FilterSubject: "wf.run.0",
		AckPolicy: jetstream.AckExplicitPolicy, AckWait: ackWait, MaxDeliver: -1, MaxAckPending: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	realFirstBatch, err := realConsumer.Fetch(1, jetstream.FetchMaxWait(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	realFirst := <-realFirstBatch.Messages()
	modelFirstBatch, err := modeledConsumer.FetchOne(ctx)
	if err != nil {
		t.Fatal(err)
	}
	modelFirst := <-modelFirstBatch.Messages()
	if realFirst == nil || modelFirst == nil {
		t.Fatal("initial delivery missing")
	}
	if err := realFirst.NakWithDelay(nakDelay); err != nil {
		t.Fatal(err)
	}
	if err := modelFirst.NakWithDelay(nakDelay); err != nil {
		t.Fatal(err)
	}
	if err := model.Wait(ctx, nakDelay); err != nil {
		t.Fatal(err)
	}
	realSecondBatch, err := realConsumer.Fetch(1, jetstream.FetchMaxWait(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	realSecond := <-realSecondBatch.Messages()
	modelSecondBatch, err := modeledConsumer.FetchOne(ctx)
	if err != nil {
		t.Fatal(err)
	}
	modelSecond := <-modelSecondBatch.Messages()
	if realSecond == nil || modelSecond == nil || string(realSecond.Data()) != "work" || string(modelSecond.Data()) != "work" {
		t.Fatalf("delayed redelivery real=%v model=%v", realSecond, modelSecond)
	}
	realMeta, realErr := realSecond.Metadata()
	modelMeta, modelErr := modelSecond.Metadata()
	if realErr != nil || modelErr != nil || realMeta.NumDelivered != 2 || modelMeta.NumDelivered != 2 {
		t.Fatalf("delayed redelivery count real=%+v %v model=%+v %v", realMeta, realErr, modelMeta, modelErr)
	}
	if err := realSecond.DoubleAck(ctx); err != nil {
		t.Fatal(err)
	}
	if err := modelSecond.Ack(); err != nil {
		t.Fatal(err)
	}
	if model.Pending() != 0 {
		t.Fatalf("model retained %d pending messages", model.Pending())
	}
	info, err := realConsumer.Info(ctx)
	if err != nil || info.NumAckPending != 0 || info.NumPending != 0 {
		t.Fatalf("real consumer state=%+v err=%v", info, err)
	}
}

func TestSimDispatchDurableRestartAndSharedConsumerContract(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 18*time.Second)
	defer cancel()
	const ackWait = time.Second
	model := sim.NewDispatchTransport(sim.NewScheduler(2), ackWait)
	model.PublishRun("wf.run.0", []byte(`first`))
	modelBefore, err := model.Consumer(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	firstModelBatch, err := modelBefore.FetchOne(ctx)
	if err != nil {
		t.Fatal(err)
	}
	firstModel := <-firstModelBatch.Messages()
	realRun, err := all[0].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := all[0].Publish(ctx, "wf.run.0", []byte(`first`)); err != nil {
		t.Fatal(err)
	}
	const durable = "WF_SIM_RESTART"
	realBefore, err := realRun.CreateConsumer(ctx, jetstream.ConsumerConfig{
		Name: durable, Durable: durable, FilterSubject: "wf.run.0", AckPolicy: jetstream.AckExplicitPolicy,
		AckWait: ackWait, MaxDeliver: -1, MaxAckPending: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	firstRealBatch, err := realBefore.Fetch(1, jetstream.FetchMaxWait(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	firstReal := <-firstRealBatch.Messages()
	firstRealMeta, realErr := firstReal.Metadata()
	firstModelMeta, modelErr := firstModel.Metadata()
	if realErr != nil || modelErr != nil || firstRealMeta.NumDelivered != 1 || firstModelMeta.NumDelivered != 1 {
		t.Fatalf("initial delivery: real=%+v %v model=%+v %v", firstRealMeta, realErr, firstModelMeta, modelErr)
	}
	all[0].Conn().Close()
	realRunAfter, err := all[1].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	realAfter, err := realRunAfter.Consumer(ctx, durable)
	if err != nil {
		t.Fatal(err)
	}
	modelAfter, err := model.Consumer(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := model.Wait(ctx, ackWait); err != nil {
		t.Fatal(err)
	}
	secondRealBatch, err := realAfter.Fetch(1, jetstream.FetchMaxWait(4*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	secondReal := <-secondRealBatch.Messages()
	secondModelBatch, err := modelAfter.FetchOne(ctx)
	if err != nil {
		t.Fatal(err)
	}
	secondModel := <-secondModelBatch.Messages()
	secondRealMeta, realErr := secondReal.Metadata()
	secondModelMeta, modelErr := secondModel.Metadata()
	if realErr != nil || modelErr != nil || secondRealMeta.NumDelivered != 2 || secondModelMeta.NumDelivered != 2 || secondRealMeta.Sequence.Stream != firstRealMeta.Sequence.Stream || secondModelMeta.Sequence.Stream != firstModelMeta.Sequence.Stream {
		t.Fatalf("restart redelivery: real=%+v %v model=%+v %v", secondRealMeta, realErr, secondModelMeta, modelErr)
	}
	if err := secondReal.DoubleAck(ctx); err != nil {
		t.Fatal(err)
	}
	if err := secondModel.Ack(); err != nil {
		t.Fatal(err)
	}
	realRunPeer, err := all[2].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	realPeer, err := realRunPeer.Consumer(ctx, durable)
	if err != nil {
		t.Fatal(err)
	}
	modelPeer, err := model.Consumer(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, data := range []string{"second", "third"} {
		if _, err := all[1].Publish(ctx, "wf.run.0", []byte(data)); err != nil {
			t.Fatal(err)
		}
		model.PublishRun("wf.run.0", []byte(data))
	}
	firstNewRealBatch, err := realAfter.Fetch(1, jetstream.FetchMaxWait(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	secondNewRealBatch, err := realPeer.Fetch(1, jetstream.FetchMaxWait(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	firstNewModelBatch, err := modelAfter.FetchOne(ctx)
	if err != nil {
		t.Fatal(err)
	}
	secondNewModelBatch, err := modelPeer.FetchOne(ctx)
	if err != nil {
		t.Fatal(err)
	}
	realOne, realTwo := <-firstNewRealBatch.Messages(), <-secondNewRealBatch.Messages()
	modelOne, modelTwo := <-firstNewModelBatch.Messages(), <-secondNewModelBatch.Messages()
	realOneMeta, errOne := realOne.Metadata()
	realTwoMeta, errTwo := realTwo.Metadata()
	modelOneMeta, modelErrOne := modelOne.Metadata()
	modelTwoMeta, modelErrTwo := modelTwo.Metadata()
	if errOne != nil || errTwo != nil || modelErrOne != nil || modelErrTwo != nil || realOneMeta.Sequence.Stream == realTwoMeta.Sequence.Stream || modelOneMeta.Sequence.Stream == modelTwoMeta.Sequence.Stream || realOneMeta.NumDelivered != 1 || realTwoMeta.NumDelivered != 1 || modelOneMeta.NumDelivered != 1 || modelTwoMeta.NumDelivered != 1 {
		t.Fatalf("shared durable: real=%+v %+v errors=%v %v model=%+v %+v errors=%v %v", realOneMeta, realTwoMeta, errOne, errTwo, modelOneMeta, modelTwoMeta, modelErrOne, modelErrTwo)
	}
	if err := realOne.DoubleAck(ctx); err != nil {
		t.Fatal(err)
	}
	if err := realTwo.DoubleAck(ctx); err != nil {
		t.Fatal(err)
	}
	if err := modelOne.Ack(); err != nil {
		t.Fatal(err)
	}
	if err := modelTwo.Ack(); err != nil {
		t.Fatal(err)
	}
	if model.Pending() != 0 {
		t.Fatalf("modeled durable has %d pending", model.Pending())
	}
	info, err := realAfter.Info(ctx)
	if err != nil || info.NumAckPending != 0 {
		t.Fatalf("real durable ack state=%+v err=%v", info, err)
	}
}
