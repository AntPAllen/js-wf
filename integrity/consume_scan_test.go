package integrity

import (
	"context"
	"errors"
	"fmt"
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
	ctx               *callbackTestContext
	count             int
	messages          []jetstream.Msg
	entered, returned chan int
}

func (c *callbackTestConsumer) Consume(handler jetstream.MessageHandler, _ ...jetstream.PullConsumeOpt) (jetstream.ConsumeContext, error) {
	go func() {
		defer close(c.ctx.closed)
		count := c.count
		if c.messages != nil {
			count = len(c.messages)
		}
		for n := 1; n <= count; n++ {
			select {
			case <-c.ctx.requested:
				return
			default:
			}
			if c.entered != nil {
				c.entered <- n
			}
			if c.messages != nil {
				handler(c.messages[n-1])
			} else {
				handler(candidateControlMsg{stream: "CONTROL", seq: uint64(n)})
			}
			if c.returned != nil {
				c.returned <- n
			}
		}
		<-c.ctx.requested
	}()
	return c.ctx, nil
}

type callbackPayloadMsg struct {
	candidateControlMsg
	data []byte
}

func (m callbackPayloadMsg) Data() []byte { return m.data }

func TestCallbackBufferedPayloadBoundOversizeAndStop(t *testing.T) {
	for _, size := range []int{6, 20} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			c := &callbackTestConsumer{ctx: &callbackTestContext{requested: make(chan struct{}), closed: make(chan struct{})}, entered: make(chan int, 2), returned: make(chan int, 2)}
			c.messages = []jetstream.Msg{callbackPayloadMsg{data: make([]byte, size)}, callbackPayloadMsg{data: make([]byte, 6)}}
			d, err := newCallbackDeliveryWithBuffer(c, 2, 10)
			if err != nil {
				t.Fatal(err)
			}
			call, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			for _, ch := range []chan int{c.entered, c.returned, c.entered} {
				select {
				case <-ch:
				case <-call.Done():
					t.Fatal("callback did not reach byte pressure")
				}
			}
			d.budgetMu.Lock()
			used, records := d.queuedPayload, d.queuedRecords
			d.budgetMu.Unlock()
			if used != size || records != 1 || len(d.messages) != 1 {
				t.Fatalf("used=%d records=%d queue=%d", used, records, len(d.messages))
			}
			select {
			case <-c.returned:
				t.Fatal("second payload exceeded byte bound")
			default:
			}
			msg, err := d.next(call)
			if err != nil || len(msg.Data()) != size {
				t.Fatalf("first payload %v/%v", msg, err)
			}
			select {
			case <-c.returned:
			case <-call.Done():
				t.Fatal("byte credit did not release producer")
			}
			if err := d.stopAndJoin(call); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCallbackBufferedStopUnblocksByteAndRecordPressure(t *testing.T) {
	for _, bytes := range []int{0, 6} {
		c := &callbackTestConsumer{ctx: &callbackTestContext{requested: make(chan struct{}), closed: make(chan struct{})}, entered: make(chan int, 5), returned: make(chan int, 5)}
		for n := 0; n < 4; n++ {
			c.messages = append(c.messages, callbackPayloadMsg{data: make([]byte, bytes)})
		}
		d, err := newCallbackDeliveryWithBuffer(c, 2, 10)
		if err != nil {
			t.Fatal(err)
		}
		call, cancel := context.WithTimeout(context.Background(), time.Second)
		// Empty payloads fill the record queue; six-byte payloads fill the
		// byte budget. Both leave the callback blocked on the next record.
		target := 3
		if bytes != 0 {
			target = 2
		}
		for n := 0; n < target; n++ {
			select {
			case <-c.entered:
			case <-call.Done():
				t.Fatal("producer did not fill queue")
			}
		}
		if len(d.messages) > 2 {
			t.Fatal("record bound exceeded")
		}
		if err := d.stopAndJoin(call); err != nil {
			t.Fatal(err)
		}
		cancel()
		select {
		case <-c.ctx.closed:
		default:
			t.Fatal("pressured callback leaked")
		}
	}
}

func TestCallbackBufferedRejectsInvalidBounds(t *testing.T) {
	for _, bounds := range [][2]int{{-1, 1}, {257, 1}, {0, 1}, {1, 0}} {
		if _, err := newCallbackDeliveryWithBuffer(&callbackTestConsumer{}, bounds[0], bounds[1]); err == nil {
			t.Fatal(bounds)
		}
	}
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
