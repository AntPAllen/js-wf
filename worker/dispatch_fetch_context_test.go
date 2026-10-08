package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// A core NATS server supplies a controlled consumer API, not a JetStream
// replica. Repeated informational replies exercise the actual SDK fetch loop.
// They must not extend the worker adapter's deadline or survive cancellation.
func TestDispatchFetchContextBoundsInformationalReplies(t *testing.T) {
	for _, mode := range []string{"deadline", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true})
			if err != nil {
				t.Fatal(err)
			}
			go s.Start()
			if !s.ReadyForConnections(2 * time.Second) {
				t.Fatal("server not ready")
			}
			defer func() { s.Shutdown(); s.WaitForShutdown() }()
			nc, err := nats.Connect(s.ClientURL())
			if err != nil {
				t.Fatal(err)
			}
			defer nc.Close()
			_, err = nc.Subscribe("$JS.API.CONSUMER.INFO.RUN.DISPATCH", func(m *nats.Msg) {
				_ = m.Respond([]byte(`{"type":"io.nats.jetstream.api.v1.consumer_info_response","stream_name":"RUN","name":"DISPATCH","config":{"name":"DISPATCH","ack_policy":"explicit"}}`))
			})
			if err != nil {
				t.Fatal(err)
			}
			entered := make(chan struct{}, 1)
			stop := make(chan struct{})
			var senders sync.WaitGroup
			var replies atomic.Int64
			_, err = nc.Subscribe("$JS.API.CONSUMER.MSG.NEXT.RUN.DISPATCH", func(m *nats.Msg) {
				senders.Add(1)
				go func() {
					defer senders.Done()
					entered <- struct{}{}
					ticker := time.NewTicker(20 * time.Millisecond)
					defer ticker.Stop()
					for {
						select {
						case <-stop:
							return
						case <-ticker.C:
							_ = nc.PublishMsg(&nats.Msg{Subject: m.Reply, Header: nats.Header{"Status": []string{"100"}, "Description": []string{"Idle Heartbeat"}}})
							replies.Add(1)
						}
					}
				}()
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { close(stop); nc.Flush(); senders.Wait() }()
			if err = nc.Flush(); err != nil {
				t.Fatal(err)
			}
			js, err := jetstream.New(nc)
			if err != nil {
				t.Fatal(err)
			}
			setup, setupCancel := context.WithTimeout(context.Background(), time.Second)
			defer setupCancel()
			consumer, err := js.Consumer(setup, "RUN", "DISPATCH")
			if err != nil {
				t.Fatal(err)
			}
			baseline := nc.NumSubscriptions()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			start := time.Now()
			batch, err := (jetStreamDispatchConsumer{consumer: consumer}).FetchOne(ctx)
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("no pull request")
			}
			if mode == "cancel" {
				cancel()
			}
			limit := 1500 * time.Millisecond
			if mode == "cancel" {
				limit = 300 * time.Millisecond
			}
			select {
			case _, open := <-batch.Messages():
				if open {
					t.Fatal("informational reply dispatched as user message")
				}
			case <-time.After(limit):
				t.Fatalf("fetch stayed open under %s after %s", mode, time.Since(start))
			}
			want := context.DeadlineExceeded
			if mode == "cancel" {
				want = context.Canceled
			}
			if !errors.Is(batch.Error(), want) {
				t.Fatalf("error %v, want %v", batch.Error(), want)
			}
			// The SDK closes the batch just before unsubscribing its inbox.
			cleanupDeadline := time.Now().Add(300 * time.Millisecond)
			for nc.NumSubscriptions() != baseline && time.Now().Before(cleanupDeadline) {
				time.Sleep(time.Millisecond)
			}
			if nc.NumSubscriptions() != baseline {
				t.Fatalf("fetch subscription leaked: %d, baseline %d", nc.NumSubscriptions(), baseline)
			}
			if mode == "deadline" && replies.Load() < 10 {
				t.Fatal("deadline case did not exercise repeated informational replies")
			}
			if mode != "cancel" && ctx.Err() != nil {
				t.Fatal("fetch canceled the worker context")
			}
			t.Logf("mode=%s elapsed=%s informational_replies=%d inbox_released=true", mode, time.Since(start), replies.Load())
		})
	}
}

func TestDispatchFetchRejectsCanceledContextBeforeConsumer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// A nil consumer would panic if the adapter attempted a transport call.
	batch, err := (jetStreamDispatchConsumer{}).FetchOne(ctx)
	if batch != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("batch=%v error=%v", batch, err)
	}
}

func TestDispatchFetchContextPreservesNativeDelivery(t *testing.T) {
	s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true, JetStream: true, StoreDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	go s.Start()
	if !s.ReadyForConnections(2 * time.Second) {
		t.Fatal("server not ready")
	}
	defer func() { s.Shutdown(); s.WaitForShutdown() }()
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{Name: "RUN", Subjects: []string{"run"}})
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := js.CreateConsumer(ctx, "RUN", jetstream.ConsumerConfig{Name: "DISPATCH", AckPolicy: jetstream.AckExplicitPolicy})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = js.Publish(ctx, "run", []byte("wake")); err != nil {
		t.Fatal(err)
	}
	batch, err := (jetStreamDispatchConsumer{consumer: consumer}).FetchOne(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var received int
	for msg := range batch.Messages() {
		received++
		if string(msg.Data()) != "wake" {
			t.Fatal("payload changed")
		}
		meta, err := msg.Metadata()
		if err != nil || meta.Sequence.Stream != 1 {
			t.Fatalf("metadata=%v error=%v", meta, err)
		}
		if err = msg.DoubleAck(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err = batch.Error(); err != nil || received != 1 {
		t.Fatalf("received=%d error=%v", received, err)
	}
	if ctx.Err() != nil {
		t.Fatal("fetch canceled worker context")
	}
	info, err := consumer.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.NumAckPending != 0 || info.AckFloor.Stream != 1 {
		t.Fatalf("unconfirmed acknowledgment: %+v", info)
	}
}
