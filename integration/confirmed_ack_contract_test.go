package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/testcluster"
)

func TestReplicatedConfirmedAckWithHeldReply(t *testing.T) {
	all, cluster := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	stream, err := all[0].CreateOrUpdateStream(ctx, jetstream.StreamConfig{Name: "ACK_REPLY_CONTRACT", Subjects: []string{"ack.reply.contract"}, Retention: jetstream.WorkQueuePolicy, Storage: jetstream.FileStorage, Replicas: 3})
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{Durable: "ACK_REPLY_CONTRACT", AckPolicy: jetstream.AckExplicitPolicy, AckWait: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	proxy, err := testcluster.NewClientProxy(cluster.Servers[0].ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	nc, err := proxy.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	remote, err := js.Consumer(ctx, "ACK_REPLY_CONTRACT", "ACK_REPLY_CONTRACT")
	if err != nil {
		t.Fatal(err)
	}
	for _, confirmed := range []bool{false, true} {
		if _, err := all[0].Publish(ctx, "ack.reply.contract", []byte(`true`)); err != nil {
			t.Fatal(err)
		}
		batch, err := remote.Fetch(1, jetstream.FetchMaxWait(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		var msg jetstream.Msg
		for item := range batch.Messages() {
			msg = item
		}
		if msg == nil {
			t.Fatal("no delivery", batch.Error())
		}
		proxy.HoldResponses()
		if confirmed {
			attempt, stop := context.WithTimeout(ctx, 250*time.Millisecond)
			err = msg.DoubleAck(attempt)
			stop()
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("held confirmation returned %v", err)
			}
		} else if err := msg.Ack(); err != nil {
			t.Fatal(err)
		}
		// Observe through a different connection while the sender cannot receive
		// any server bytes. The consumer really committed the acknowledgement.
		for {
			info, err := consumer.Info(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if info.NumAckPending == 0 {
				break
			}
			if ctx.Err() != nil {
				t.Fatal(ctx.Err())
			}
			time.Sleep(5 * time.Millisecond)
		}
		proxy.ResumeResponses()
		if confirmed {
			attempt, stop := context.WithTimeout(ctx, time.Second)
			err = msg.DoubleAck(attempt)
			stop()
			if err != nil {
				t.Fatalf("retry ambiguous confirmed ack: %v", err)
			}
		}
		for {
			info, err := stream.Info(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if info.State.Msgs == 0 {
				break
			}
			if ctx.Err() != nil {
				t.Fatal(ctx.Err())
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Logf("confirmed=%v: held server replies, committed consumer ACK observed independently, stream drain; ambiguous DoubleAck retry confirmed", confirmed)
	}
}
