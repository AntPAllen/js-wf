package worker

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Delay the first actual SDK option evaluation past the adapter's deadline.
// This models descheduling between context creation and Fetch without changing
// the production timeout or returning a synthetic deadline error.
type delayedOptionConsumer struct {
	jetstream.Consumer
	expired    bool
	calls      int
	firstError error
}

func (c *delayedOptionConsumer) Fetch(n int, opts ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	c.calls++
	if c.calls == 1 {
		if !c.expired {
			c.firstError = fmt.Errorf("%w: controlled invalid option", jetstream.ErrInvalidOption)
			return nil, c.firstError
		}
		<-time.After(1100 * time.Millisecond)
	}
	batch, err := c.Consumer.Fetch(n, opts...)
	if c.calls == 1 {
		c.firstError = err
	}
	return batch, err
}

type optionDispatchPort struct{ consumer DispatchConsumer }

func (p optionDispatchPort) Consumer(context.Context, uint32) (DispatchConsumer, error) {
	return p.consumer, nil
}
func (optionDispatchPort) Wait(context.Context, time.Duration) error { return nil }

func TestDispatchFetchExpiredOptionKeepsPartitionAlive(t *testing.T) {
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
	setup, cancelSetup := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelSetup()
	if _, err = js.CreateStream(setup, jetstream.StreamConfig{Name: "RUN", Subjects: []string{"run"}}); err != nil {
		t.Fatal(err)
	}
	for _, expired := range []bool{true, false} {
		t.Run(fmt.Sprintf("expired=%t", expired), func(t *testing.T) {
			consumer, err := js.CreateConsumer(setup, "RUN", jetstream.ConsumerConfig{Name: fmt.Sprintf("D_%t", expired), AckPolicy: jetstream.AckExplicitPolicy})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = js.Publish(setup, "run", []byte("wake")); err != nil {
				t.Fatal(err)
			}
			delayed := &delayedOptionConsumer{Consumer: consumer, expired: expired}
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			handled := 0
			err = RunPartitionWithPort(ctx, 0, optionDispatchPort{consumer: jetStreamDispatchConsumer{consumer: delayed}}, func(handlerCtx context.Context, msg jetstream.Msg) {
				handled++
				if string(msg.Data()) != "wake" {
					t.Error("changed payload")
				}
				if err := msg.DoubleAck(handlerCtx); err != nil {
					t.Error(err)
				}
				cancel()
			}, 1)
			if !errors.Is(delayed.firstError, jetstream.ErrInvalidOption) || errors.Is(delayed.firstError, context.DeadlineExceeded) {
				t.Fatalf("SDK control did not produce opaque option error: %v", delayed.firstError)
			}
			if expired {
				if err != nil || handled != 1 || delayed.calls != 2 {
					t.Fatalf("partition failed after local expiry: err=%v handled=%d calls=%d", err, handled, delayed.calls)
				}
				info, err := consumer.Info(setup)
				if err != nil || info.NumAckPending != 0 || info.AckFloor.Stream != 1 {
					t.Fatalf("delivery not acknowledged: info=%+v error=%v", info, err)
				}
			} else {
				if !errors.Is(err, jetstream.ErrInvalidOption) || handled != 0 || delayed.calls != 1 || ctx.Err() != nil {
					t.Fatalf("live invalid option must remain fatal: err=%v handled=%d calls=%d context=%v", err, handled, delayed.calls, ctx.Err())
				}
			}
		})
	}
}
