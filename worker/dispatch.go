package worker

import (
	"context"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// DispatchPort is the durable pull-consumer boundary used by RunPartition.
// The production adapter calls JetStream; a deterministic model can supply
// fetch, leader movement, redelivery, and virtual waits.
type DispatchPort interface {
	Consumer(context.Context, uint32) (DispatchConsumer, error)
	Wait(context.Context, time.Duration) error
}

type DispatchConsumer interface {
	FetchOne(context.Context) (DispatchBatch, error)
	Info(context.Context) (pending uint64, ackPending int, err error)
}

type DispatchBatch interface {
	Messages() <-chan jetstream.Msg
	Error() error
}

type jetStreamDispatchPort struct{ worker *Worker }

func (p jetStreamDispatchPort) Consumer(ctx context.Context, partition uint32) (DispatchConsumer, error) {
	c, err := p.worker.consumer(ctx, partition)
	if err != nil {
		return nil, err
	}
	return jetStreamDispatchConsumer{consumer: c}, nil
}

func (jetStreamDispatchPort) Wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type jetStreamDispatchConsumer struct{ consumer jetstream.Consumer }

func (c jetStreamDispatchConsumer) FetchOne(_ context.Context) (DispatchBatch, error) {
	return c.consumer.Fetch(1, jetstream.FetchMaxWait(time.Second))
}

func (c jetStreamDispatchConsumer) Info(ctx context.Context) (uint64, int, error) {
	info, err := c.consumer.Info(ctx)
	if err != nil {
		return 0, 0, err
	}
	return info.NumPending, info.NumAckPending, nil
}
