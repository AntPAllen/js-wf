package integration_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"js-wf/worker"

	"github.com/nats-io/nats.go/jetstream"
)

// A default timing change must fail closed on an existing durable. An explicit
// operator update, with workers stopped, preserves its unacknowledged delivery.
func TestDispatchTimingUpgradePreservesPendingDelivery(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	run, err := all[0].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	cfg := jetstream.ConsumerConfig{Name: "WF_P_00", Durable: "WF_P_00", FilterSubject: "wf.run.0", AckPolicy: jetstream.AckExplicitPolicy, AckWait: 20 * time.Second, MaxDeliver: -1, MaxAckPending: 1000}
	consumer, err := run.CreateConsumer(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	published, err := all[0].Publish(ctx, "wf.run.0", []byte("upgrade.retained"))
	if err != nil {
		t.Fatal(err)
	}
	batch, err := consumer.Fetch(1, jetstream.FetchMaxWait(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	var msg jetstream.Msg
	for received := range batch.Messages() {
		msg = received
	}
	if msg == nil || batch.Error() != nil {
		t.Fatalf("fetch: msg=%v err=%v", msg, batch.Error())
	}
	before, err := consumer.Info(ctx)
	if err != nil || before.NumAckPending != 1 {
		t.Fatalf("before upgrade: info=%+v err=%v", before, err)
	}
	cfg.AckWait = worker.DefaultAckWait
	if _, err := run.CreateConsumer(ctx, cfg); !errors.Is(err, jetstream.ErrConsumerExists) {
		t.Fatalf("implicit timing adoption: %v", err)
	}
	consumer, err = run.UpdateConsumer(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	after, err := consumer.Info(ctx)
	if err != nil || after.Config.AckWait != worker.DefaultAckWait || after.NumAckPending != 1 || !reflect.DeepEqual(after.Delivered, before.Delivered) || !reflect.DeepEqual(after.AckFloor, before.AckFloor) {
		t.Fatalf("upgrade changed pending delivery: before=%+v after=%+v err=%v", before, after, err)
	}
	if _, err := run.CreateConsumer(ctx, cfg); err != nil {
		t.Fatalf("matching new worker timing: %v", err)
	}
	retained, err := run.GetMsg(ctx, published.Sequence)
	if err != nil || string(retained.Data) != "upgrade.retained" {
		t.Fatalf("retained run changed: msg=%+v err=%v", retained, err)
	}
	if err := msg.DoubleAck(ctx); err != nil {
		t.Fatal(err)
	}
	done, err := consumer.Info(ctx)
	if err != nil || done.NumAckPending != 0 || done.NumPending != 0 {
		t.Fatalf("delivery did not drain: info=%+v err=%v", done, err)
	}
}
