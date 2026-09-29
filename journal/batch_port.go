package journal

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type jetStreamBatchReadPort struct{ stream jetstream.Stream }

func (jetStreamBatchReadPort) Wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (p jetStreamBatchReadPort) Open(ctx context.Context, subject string, sequence uint64) (BatchReadCursor, error) {
	consumer, err := p.stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		FilterSubject: subject, DeliverPolicy: jetstream.DeliverByStartSequencePolicy,
		OptStartSeq: sequence, AckPolicy: jetstream.AckNonePolicy,
		Replicas: 1, InactiveThreshold: time.Minute,
	})
	if err != nil {
		return nil, err
	}
	cursor := &jetStreamBatchCursor{stream: p.stream, consumer: consumer}
	if info := consumer.CachedInfo(); info != nil {
		cursor.name = info.Name
	}
	return cursor, nil
}

func (p jetStreamBatchReadPort) Probe(ctx context.Context, subject string, sequence uint64) (bool, error) {
	_, err := p.stream.GetMsg(ctx, sequence, jetstream.WithGetMsgSubject(subject))
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return false, nil
	}
	return err == nil, err
}

type jetStreamBatchCursor struct {
	stream   jetstream.Stream
	consumer jetstream.Consumer
	name     string
}

func (c *jetStreamBatchCursor) Fetch(_ context.Context, limit int) ([]AppendTail, error) {
	batch, fetchErr := c.consumer.Fetch(limit, jetstream.FetchMaxWait(250*time.Millisecond))
	if batch == nil {
		return nil, fetchErr
	}
	messages := make([]AppendTail, 0, limit)
	for msg := range batch.Messages() {
		metadata, err := msg.Metadata()
		if err != nil {
			return messages, err
		}
		messages = append(messages, AppendTail{Sequence: metadata.Sequence.Stream, Data: msg.Data()})
	}
	if fetchErr != nil {
		return messages, fetchErr
	}
	return messages, batch.Error()
}

func (c *jetStreamBatchCursor) Close(ctx context.Context) error {
	if c.name == "" {
		return nil
	}
	return c.stream.DeleteConsumer(ctx, c.name)
}
