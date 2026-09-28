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
