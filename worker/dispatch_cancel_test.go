package worker

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type stalledBatch struct{ messages <-chan jetstream.Msg }

func (b stalledBatch) Messages() <-chan jetstream.Msg { return b.messages }
func (stalledBatch) Error() error                     { return nil }

type stalledConsumer struct {
	entered  chan<- struct{}
	messages <-chan jetstream.Msg
}

func (c stalledConsumer) FetchOne(context.Context) (DispatchBatch, error) {
	c.entered <- struct{}{}
	return stalledBatch{messages: c.messages}, nil
}
func (stalledConsumer) Info(context.Context) (uint64, int, error) { return 0, 0, nil }

type stalledDispatch struct{ consumer stalledConsumer }

func (p stalledDispatch) Consumer(context.Context, uint32) (DispatchConsumer, error) {
	return p.consumer, nil
}
func (stalledDispatch) Wait(context.Context, time.Duration) error { return nil }

func TestPartitionCancellationDuringStalledBatch(t *testing.T) {
	for _, concurrency := range []int{1, 4} {
		t.Run(string(rune('0'+concurrency)), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered := make(chan struct{}, 1)
			messages := make(chan jetstream.Msg)
			port := stalledDispatch{consumer: stalledConsumer{entered: entered, messages: messages}}
			done := make(chan error, 1)
			go func() {
				done <- RunPartitionWithPort(ctx, 0, port, func(context.Context, jetstream.Msg) {
					t.Error("unexpected message")
				}, concurrency)
			}()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("partition did not fetch")
			}
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("partition did not stop while batch channel stayed open")
			}
		})
	}
}
