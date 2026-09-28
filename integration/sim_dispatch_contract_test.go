package integration_test

import (
	"context"
	"testing"
	"time"

	"js-wf/sim"

	"github.com/nats-io/nats.go/jetstream"
)

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
