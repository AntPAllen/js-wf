package worker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type observedConsumer struct{ messages <-chan jetstream.Msg }

func (c observedConsumer) FetchOne(context.Context) (DispatchBatch, error) {
	return stalledBatch{messages: c.messages}, nil
}
func (observedConsumer) Info(context.Context) (uint64, int, error) { return 0, 0, nil }

type observedPort struct{ consumer DispatchConsumer }

func (p observedPort) Consumer(context.Context, uint32) (DispatchConsumer, error) {
	return p.consumer, nil
}
func (observedPort) Wait(ctx context.Context, _ time.Duration) error { return ctx.Err() }

func TestPartitionObserverSeparatesSlotWaitAndPull(t *testing.T) {
	for _, mode := range []string{"slot", "pull"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			messages := make(chan jetstream.Msg)
			// One-message batches allow two handlers to occupy both reservations.
			port := observedPort{consumer: observedConsumer{messages: messages}}
			if mode == "slot" {
				port.consumer = singleMessageConsumer{}
			}
			var mu sync.Mutex
			var events []PartitionEvent
			blocked := make(chan struct{}, 1)
			ended := make(chan struct{}, 1)
			release := make(chan struct{})
			var releaseOnce sync.Once
			releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
			defer releaseAll()
			observe := func(e PartitionEvent) {
				mu.Lock()
				events = append(events, e)
				mu.Unlock()
				match := mode == "pull" && e.Operation == "pull" && e.Attempt == 1 || mode == "slot" && e.Operation == "slot_wait" && e.Attempt == 3
				if match {
					if e.Phase == "begin" {
						blocked <- struct{}{}
					} else {
						ended <- struct{}{}
					}
				}
			}
			done := make(chan error, 1)
			go func() {
				if mode == "pull" {
					w := &Worker{ID: "observer-worker", partitionConcurrency: 2}
					_ = WithPartitionObserver(observe)(w)
					done <- w.RunPartitionWithTransport(ctx, 7, port)
					return
				}
				done <- runPartitionWithPort(ctx, 7, port, func(ctx context.Context, _ jetstream.Msg) { <-release }, 2, "observer-worker", observe)
			}()
			select {
			case <-blocked:
			case <-time.After(2 * time.Second):
				t.Fatal("expected blocked operation did not begin")
			}
			cancel()
			select {
			case <-ended:
			case <-time.After(2 * time.Second):
				t.Fatal("cancelled operation did not end")
			}
			// Let handlers exit only after the slot wait has reported cancellation.
			releaseAll()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("partition did not stop")
			}
			mu.Lock()
			defer mu.Unlock()
			type pairKey struct {
				operation string
				attempt   uint64
			}
			pairs := map[pairKey]PartitionEvent{}
			for _, e := range events {
				if e.Worker != "observer-worker" || e.Partition != 7 || e.Concurrency != 2 || e.SlotsReserved < 0 || e.SlotsReserved > 2 {
					t.Fatalf("invalid identity/occupancy: %+v", e)
				}
				key := pairKey{e.Operation, e.Attempt}
				if e.Phase == "begin" {
					if _, ok := pairs[key]; ok {
						t.Fatal("duplicate begin")
					}
					pairs[key] = e
					continue
				}
				start, ok := pairs[key]
				if !ok || e.Phase != "end" || e.At.Before(start.At) || e.Duration < 0 {
					t.Fatalf("invalid pair: %+v", e)
				}
				delete(pairs, key)
				if mode == "slot" && e.Attempt == 3 && e.Operation == "pull" {
					t.Fatal("pull began while slots were full")
				}
				if e.Attempt == 3 && e.Operation == "slot_wait" || mode == "pull" && e.Operation == "pull" {
					if e.Error != context.Canceled.Error() {
						t.Fatalf("missing cancellation: %+v", e)
					}
				}
			}
			if len(pairs) != 0 {
				t.Fatalf("unclosed observations: %+v", pairs)
			}
		})
	}
}

type singleMessageConsumer struct{}

func (singleMessageConsumer) FetchOne(context.Context) (DispatchBatch, error) {
	ch := make(chan jetstream.Msg, 1)
	ch <- nil
	close(ch)
	return stalledBatch{messages: ch}, nil
}
func (singleMessageConsumer) Info(context.Context) (uint64, int, error) { return 0, 0, nil }
