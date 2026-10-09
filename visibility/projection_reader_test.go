package visibility

import (
	"context"
	"errors"
	"math/rand"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type projectionReaderMessage struct {
	jetstream.Msg
	metadata *jetstream.MsgMetadata
	err      error
}

func (m projectionReaderMessage) Metadata() (*jetstream.MsgMetadata, error) { return m.metadata, m.err }

type projectionReaderIterator struct {
	jetstream.MessagesContext
	messages []jetstream.Msg
	position int
	quiet    bool
	err      error
	stopped  atomic.Int32
	read     chan struct{}
}

func (i *projectionReaderIterator) Next(opts ...jetstream.NextOpt) (jetstream.Msg, error) {
	if len(opts) != 1 {
		return nil, errors.New("Next must carry the read context")
	}
	if i.quiet {
		i.quiet = false
		return nil, nats.ErrTimeout
	}
	if i.err != nil {
		return nil, i.err
	}
	if i.position == len(i.messages) {
		return nil, nats.ErrTimeout
	}
	msg := i.messages[i.position]
	i.position++
	if i.read != nil {
		close(i.read)
		i.read = nil
	}
	return msg, nil
}
func (i *projectionReaderIterator) Stop() { i.stopped.Add(1) }

type projectionReaderConsumer struct {
	jetstream.Consumer
	iterator      *projectionReaderIterator
	messagesCalls int
	startErr      error
	infoErr       error
	info          *jetstream.ConsumerInfo
}

func (c *projectionReaderConsumer) Messages(opts ...jetstream.PullMessagesOpt) (jetstream.MessagesContext, error) {
	c.messagesCalls++
	if len(opts) != 2 || opts[0] != jetstream.PullMaxMessages(256) || opts[1] != jetstream.PullExpiry(time.Second) {
		return nil, errors.New("unexpected iterator buffering")
	}
	return c.iterator, c.startErr
}
func (c *projectionReaderConsumer) Info(ctx context.Context) (*jetstream.ConsumerInfo, error) {
	if c.info != nil {
		return c.info, c.infoErr
	}
	return &jetstream.ConsumerInfo{NumPending: uint64(len(c.iterator.messages) - c.iterator.position)}, c.infoErr
}
func TestProjectionReaderSeededRetainedHolesAndWatermark(t *testing.T) {
	// Seeds vary purged sequence holes, final watermark and a quiet delivery.
	// The production reader must preserve every retained sequence through the
	// cut, exclude later publications, and retain one iterator across windows.
	for seed := int64(1); seed <= 1000; seed++ {
		rng := rand.New(rand.NewSource(seed))
		n := rng.Intn(1024) + 1
		var messages []jetstream.Msg
		var wanted []uint64
		sequence := uint64(0)
		cut := rng.Intn(n) + 1
		var through uint64
		for j := 0; j < n; j++ {
			sequence += uint64(rng.Intn(8) + 1)
			messages = append(messages, projectionReaderMessage{metadata: &jetstream.MsgMetadata{Sequence: jetstream.SequencePair{Stream: sequence}, NumPending: uint64(n - j - 1)}})
			if j < cut {
				wanted = append(wanted, sequence)
				through = sequence
			}
		}
		iterator := &projectionReaderIterator{messages: messages, quiet: rng.Intn(2) == 0}
		consumer := &projectionReaderConsumer{iterator: iterator}
		jobs := make(chan uint64, n)
		if err := enqueueProjectionInvocations(context.Background(), consumer, through, jobs); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		close(jobs)
		var got []uint64
		for seq := range jobs {
			got = append(got, seq)
		}
		if !reflect.DeepEqual(got, wanted) || consumer.messagesCalls != 1 || iterator.stopped.Load() != 1 {
			t.Fatalf("seed=%d: got=%v want=%v starts=%d stops=%d", seed, got, wanted, consumer.messagesCalls, iterator.stopped.Load())
		}
	}
}
func TestProjectionReaderFailureDoesNotCertifyEmptySource(t *testing.T) {
	sentinel := errors.New("permanent projection source failure")
	for _, failure := range []error{sentinel, nats.ErrNoResponders, jetstream.ErrMsgIteratorClosed} {
		t.Run(failure.Error(), func(t *testing.T) {
			iterator := &projectionReaderIterator{err: failure}
			consumer := &projectionReaderConsumer{iterator: iterator}
			if err := enqueueProjectionInvocations(context.Background(), consumer, 100, make(chan uint64)); err != failure {
				t.Fatalf("error identity lost: %v", err)
			}
			if iterator.stopped.Load() != 1 {
				t.Fatal("iterator not stopped")
			}
		})
	}
	iterator := &projectionReaderIterator{quiet: true}
	consumer := &projectionReaderConsumer{iterator: iterator, infoErr: sentinel}
	if err := enqueueProjectionInvocations(context.Background(), consumer, 100, make(chan uint64)); err != sentinel {
		t.Fatalf("info error hidden: %v", err)
	}
	iterator = &projectionReaderIterator{}
	consumer = &projectionReaderConsumer{iterator: iterator, startErr: sentinel}
	if err := enqueueProjectionInvocations(context.Background(), consumer, 100, make(chan uint64)); err != sentinel || iterator.stopped.Load() != 0 {
		t.Fatalf("constructor failure changed: %v", err)
	}
	iterator = &projectionReaderIterator{messages: []jetstream.Msg{projectionReaderMessage{err: sentinel}}}
	consumer = &projectionReaderConsumer{iterator: iterator}
	if err := enqueueProjectionInvocations(context.Background(), consumer, 100, make(chan uint64)); err != sentinel {
		t.Fatalf("metadata error hidden: %v", err)
	}
}
func TestProjectionReaderCancellationWhileOutputBlocked(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	iterator := &projectionReaderIterator{messages: []jetstream.Msg{projectionReaderMessage{metadata: &jetstream.MsgMetadata{Sequence: jetstream.SequencePair{Stream: 7}, NumPending: 1}}}}
	read := make(chan struct{})
	iterator.read = read
	consumer := &projectionReaderConsumer{iterator: iterator}
	jobs := make(chan uint64)
	done := make(chan error, 1)
	go func() { done <- enqueueProjectionInvocations(ctx, consumer, 100, jobs) }()
	select {
	case <-read:
	case <-time.After(time.Second):
		t.Fatal("reader did not reach output")
	}
	select {
	case err := <-done:
		t.Fatalf("reader escaped blocked output: %v", err)
	default:
	}
	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("cancel identity lost: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("reader ignored cancellation")
	}
	if iterator.stopped.Load() < 1 {
		t.Fatal("iterator not stopped")
	}
}

func TestProjectionReaderZeroPendingWithUnobservedDelivery(t *testing.T) {
	iterator := &projectionReaderIterator{quiet: true, messages: []jetstream.Msg{projectionReaderMessage{metadata: &jetstream.MsgMetadata{Sequence: jetstream.SequencePair{Stream: 7}}}}}
	consumer := &projectionReaderConsumer{iterator: iterator, info: &jetstream.ConsumerInfo{Delivered: jetstream.SequenceInfo{Stream: 7}}}
	jobs := make(chan uint64, 1)
	if err := enqueueProjectionInvocations(context.Background(), consumer, 7, jobs); err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || <-jobs != 7 {
		t.Fatal("server zero pending incorrectly certified an unobserved delivery")
	}
	consumer = &projectionReaderConsumer{iterator: &projectionReaderIterator{}}
	if err := enqueueProjectionInvocations(context.Background(), consumer, 7, make(chan uint64)); err != nil {
		t.Fatalf("confirmed empty source: %v", err)
	}
}

func TestProjectionReaderMissingConsumerDoesNotCertifyEmptySource(t *testing.T) {
	iterator := &projectionReaderIterator{quiet: true, messages: []jetstream.Msg{projectionReaderMessage{metadata: &jetstream.MsgMetadata{Sequence: jetstream.SequencePair{Stream: 41}}}}}
	consumer := &projectionReaderConsumer{iterator: iterator, infoErr: jetstream.ErrConsumerNotFound}
	jobs := make(chan uint64, 1)
	if err := enqueueProjectionInvocations(context.Background(), consumer, 41, jobs); err != nil {
		t.Fatal(err)
	}
	if seq := <-jobs; seq != 41 {
		t.Fatal("missing consumer skipped retained source", seq)
	}
	if iterator.stopped.Load() != 1 {
		t.Fatal("reader not joined")
	}
}
