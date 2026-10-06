//go:build linux

package integration_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

type bulkFaultControlMsg struct {
	jetstream.Msg
	sequence uint64
}

func (m *bulkFaultControlMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{Sequence: jetstream.SequencePair{Stream: m.sequence}}, nil
}

type bulkFaultControlConsumer struct {
	jetstream.Consumer
	messages []jetstream.Msg
	info     *jetstream.ConsumerInfo
	infoErr  error
	queries  int
}

func (c *bulkFaultControlConsumer) Info(context.Context) (*jetstream.ConsumerInfo, error) {
	c.queries++
	return c.info, c.infoErr
}

func (c *bulkFaultControlConsumer) Consume(handler jetstream.MessageHandler, _ ...jetstream.PullConsumeOpt) (jetstream.ConsumeContext, error) {
	for _, msg := range c.messages {
		handler(msg)
	}
	return nil, nil
}

type bulkFaultControlStream struct {
	jetstream.Stream
	consumer jetstream.Consumer
}

func (s bulkFaultControlStream) CreateConsumer(context.Context, jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	return s.consumer, nil
}

func TestMatrixBulkFaultRejectsUnprovenTargetWithoutChangingDelivery(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	fault := &matrixBulkCursorFault{ctx: ctx, cancel: cancel}
	// Invalid public metadata must abort before any server operation. Multiple
	// delivered messages must neither retry the destructive action nor mutate,
	// duplicate, omit or reorder the underlying handler's input.
	consumer := &bulkFaultControlConsumer{info: &jetstream.ConsumerInfo{Stream: "WF_JRN", Config: jetstream.ConsumerConfig{Replicas: 5}}}
	for _, sequence := range []uint64{127, 128, 129, 130} {
		consumer.messages = append(consumer.messages, &bulkFaultControlMsg{sequence: sequence})
	}
	wrapped := matrixBulkFaultConsumer{Consumer: consumer, fault: fault}
	var delivered []jetstream.Msg
	_, err := wrapped.Consume(func(msg jetstream.Msg) { delivered = append(delivered, msg) })
	if err != nil || !reflect.DeepEqual(delivered, consumer.messages) || consumer.queries != 1 {
		t.Fatalf("fault adapter changed delivery or retried injection: %v queries=%d", err, consumer.queries)
	}
	proof := fault.snapshot()
	if ctx.Err() == nil || proof.Error == "" || proof.Target != nil || !proof.Kill.SourceStopped.IsZero() {
		t.Fatal("unproven target was accepted or stage remained active")
	}
}

func TestMatrixBulkFaultMetadataFailureLeavesActualCursorForCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	failure := errors.New("public metadata unavailable")
	actual := &bulkFaultControlConsumer{infoErr: failure}
	fault := &matrixBulkCursorFault{ctx: ctx, cancel: cancel}
	stream := matrixBulkFaultStream{Stream: bulkFaultControlStream{consumer: actual}, fault: fault}
	consumer, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{})
	if err != nil || consumer != actual || ctx.Err() == nil || fault.snapshot().Error != failure.Error() {
		t.Fatal("metadata failure hid the actual cursor from normal scanner cleanup")
	}
}
