package integration_test

import (
	"context"
	"testing"
	"time"

	"js-wf/sim"

	"github.com/nats-io/nats.go/jetstream"
)

func TestSimDispatchLateAckAfterRedeliveryContract(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	model := sim.NewDispatchTransport(sim.NewScheduler(3), time.Second)
	model.PublishRun("wf.run.0", []byte(`late-ack`))
	modeledConsumer, err := model.Consumer(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	firstModelBatch, err := modeledConsumer.FetchOne(ctx)
	if err != nil {
		t.Fatal(err)
	}
	firstModel := <-firstModelBatch.Messages()
	run, err := all[0].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := all[0].Publish(ctx, "wf.run.0", []byte(`late-ack`)); err != nil {
		t.Fatal(err)
	}
	consumer, err := run.CreateConsumer(ctx, jetstream.ConsumerConfig{Name: "WF_SIM_LATE_ACK", Durable: "WF_SIM_LATE_ACK", FilterSubject: "wf.run.0", AckPolicy: jetstream.AckExplicitPolicy, AckWait: time.Second, MaxDeliver: -1, MaxAckPending: 1000})
	if err != nil {
		t.Fatal(err)
	}
	firstBatch, err := consumer.Fetch(1, jetstream.FetchMaxWait(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	first := <-firstBatch.Messages()
	if err := model.Wait(ctx, time.Second); err != nil {
		t.Fatal(err)
	}
	secondBatch, err := consumer.Fetch(1, jetstream.FetchMaxWait(4*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	second := <-secondBatch.Messages()
	secondModelBatch, err := modeledConsumer.FetchOne(ctx)
	if err != nil {
		t.Fatal(err)
	}
	secondModel := <-secondModelBatch.Messages()
	metadata, err := second.Metadata()
	modeledMetadata, modelErr := secondModel.Metadata()
	if err != nil || modelErr != nil || metadata.NumDelivered != 2 || modeledMetadata.NumDelivered != 2 {
		t.Fatalf("second delivery real=%+v %v model=%+v %v", metadata, err, modeledMetadata, modelErr)
	}
	if err := first.DoubleAck(ctx); err != nil {
		t.Fatalf("real late first ack: %v", err)
	}
	if err := firstModel.Ack(); err != nil {
		t.Fatalf("modeled late first ack: %v", err)
	}
	info, err := consumer.Info(ctx)
	if err != nil || info.NumAckPending != 0 || model.Pending() != 0 {
		t.Fatalf("late ack did not clear delivery: real=%+v %v model pending=%d", info, err, model.Pending())
	}
	if err := second.DoubleAck(ctx); err != nil {
		t.Fatalf("real duplicate redelivery ack: %v", err)
	}
	if err := secondModel.Ack(); err != nil {
		t.Fatalf("modeled duplicate redelivery ack: %v", err)
	}
	info, err = consumer.Info(ctx)
	if err != nil || info.NumAckPending != 0 || model.Pending() != 0 {
		t.Fatalf("duplicate ack changed state: real=%+v %v model pending=%d", info, err, model.Pending())
	}
}
