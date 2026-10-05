package integrity

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type callbackTestContext struct {
	jetstream.ConsumeContext
	requested, closed chan struct{}
	once              sync.Once
}

func (c *callbackTestContext) Stop()                   { c.once.Do(func() { close(c.requested) }) }
func (c *callbackTestContext) Closed() <-chan struct{} { return c.closed }

type callbackTestConsumer struct {
	jetstream.Consumer
	ctx   *callbackTestContext
	count int
}

func (c *callbackTestConsumer) Consume(handler jetstream.MessageHandler, _ ...jetstream.PullConsumeOpt) (jetstream.ConsumeContext, error) {
	go func() {
		defer close(c.ctx.closed)
		for n := 1; n <= c.count; n++ {
			select {
			case <-c.ctx.requested:
				return
			default:
			}
			handler(candidateControlMsg{stream: "CONTROL", seq: uint64(n)})
		}
		<-c.ctx.requested
	}()
	return c.ctx, nil
}

func TestCallbackDeliveryKeepsCursorBetweenWindowsAndJoins(t *testing.T) {
	c := &callbackTestConsumer{ctx: &callbackTestContext{requested: make(chan struct{}), closed: make(chan struct{})}, count: 6}
	d, err := newCallbackDelivery(c)
	if err != nil {
		t.Fatal(err)
	}
	call, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	seen := 0
	for window := 0; window < 2; window++ {
		batch := byteDeliveryBatch(call, 3, func() (jetstream.Msg, error) { return d.next(call) }, d.stop, false)
		for msg := range batch.Messages() {
			meta, err := msg.Metadata()
			seen++
			if err != nil || meta.Sequence.Stream != uint64(seen) {
				t.Fatalf("window=%d seen=%d meta=%+v/%v", window, seen, meta, err)
			}
		}
		if batch.Error() != nil {
			t.Fatal(batch.Error())
		}
		select {
		case <-d.stopped:
			t.Fatal("healthy window stopped cursor")
		default:
		}
	}
	if seen != 6 {
		t.Fatal(seen)
	}
	if err := d.stopAndJoin(call); err != nil {
		t.Fatal(err)
	}
	select {
	case <-c.ctx.closed:
	default:
		t.Fatal("callback not joined")
	}
}

func TestCallbackDeliveryCancellationUnblocksProducerAndJoins(t *testing.T) {
	c := &callbackTestConsumer{ctx: &callbackTestContext{requested: make(chan struct{}), closed: make(chan struct{})}, count: 100}
	d, err := newCallbackDelivery(c)
	if err != nil {
		t.Fatal(err)
	}
	call, cancel := context.WithCancel(context.Background())
	batch := byteDeliveryBatch(call, 4096, func() (jetstream.Msg, error) { return d.next(call) }, d.stop, false)
	// Receive one record, leaving either the callback or adapter blocked at the
	// next record. Cancellation must release both before returning cleanup.
	<-batch.Messages()
	cancel()
	for range batch.Messages() {
	}
	if !errors.Is(batch.Error(), context.Canceled) {
		t.Fatal(batch.Error())
	}
	if err := d.stopAndJoin(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-c.ctx.closed:
	default:
		t.Fatal("blocked callback leaked")
	}
}

func TestCallbackDeliveryPreservesErrorAndBoundsJoin(t *testing.T) {
	d := &callbackDelivery{errors: make(chan error, 1), messages: make(chan jetstream.Msg), stopped: make(chan struct{}), closed: make(chan struct{})}
	sentinel := errors.New("semantic callback failure")
	d.errors <- sentinel
	if _, err := d.next(context.Background()); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	// Model a transport whose Stop returns but whose callback never closes.
	c := &callbackTestContext{requested: make(chan struct{}), closed: make(chan struct{})}
	d.consume = c
	d.closed = c.closed
	call, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := d.stopAndJoin(call); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	close(c.closed)
}
