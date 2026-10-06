package integrity

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestChunkedCallbackPressureOrderingAndStop(t *testing.T) {
	for _, size := range []int{0, callbackChunkBytes/2 + 1, callbackChunkBytes + 1} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			count := 600
			if size != 0 {
				count = 4
			}
			c := &callbackTestConsumer{ctx: &callbackTestContext{requested: make(chan struct{}), closed: make(chan struct{})}, entered: make(chan int, count), returned: make(chan int, count)}
			for n := 1; n <= count; n++ {
				c.messages = append(c.messages, callbackPayloadMsg{candidateControlMsg: candidateControlMsg{stream: "CONTROL", seq: uint64(n)}, data: make([]byte, size)})
			}
			d, err := newChunkedCallbackDelivery(c)
			if err != nil {
				t.Fatal(err)
			}
			call, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			defer d.stopAndJoin(call)
			capacity := callbackChunkRecords
			if size != 0 {
				capacity = 1
			}
			for n := 0; n <= capacity; n++ {
				select {
				case <-c.entered:
				case <-call.Done():
					t.Fatal("producer did not reach pressure")
				}
			}
			d.mu.Lock()
			queued, bytes := d.count, d.bytes
			d.mu.Unlock()
			if queued != capacity || bytes != capacity*size {
				t.Fatalf("queue=%d/%d bytes=%d", queued, capacity, bytes)
			}
			for n := 0; n < capacity; n++ {
				<-c.returned
			}
			select {
			case <-c.returned:
				t.Fatal("producer exceeded queue bound")
			default:
			}
			for n := 1; n <= count; n++ {
				msg, err := d.next(call)
				if err != nil {
					t.Fatal(err)
				}
				meta, err := msg.Metadata()
				if err != nil || meta.Sequence.Stream != uint64(n) || len(msg.Data()) != size {
					t.Fatalf("delivery %d: %+v / %v", n, meta, err)
				}
			}
			if err := d.stopAndJoin(call); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestChunkedCallbackStopUnblocksPressure(t *testing.T) {
	for _, size := range []int{0, callbackChunkBytes + 1} {
		c := &callbackTestConsumer{ctx: &callbackTestContext{requested: make(chan struct{}), closed: make(chan struct{})}, entered: make(chan int, 300)}
		count := 300
		if size != 0 {
			count = 4
		}
		for n := 0; n < count; n++ {
			c.messages = append(c.messages, callbackPayloadMsg{data: make([]byte, size)})
		}
		d, err := newChunkedCallbackDelivery(c)
		if err != nil {
			t.Fatal(err)
		}
		call, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		target := callbackChunkRecords + 1
		if size != 0 {
			target = 2
		}
		for n := 0; n < target; n++ {
			select {
			case <-c.entered:
			case <-call.Done():
				t.Fatal("no pressure")
			}
		}
		if err := d.stopAndJoin(call); err != nil {
			t.Fatal(err)
		}
		cancel()
	}
}

func TestChunkedCallbackCancelAndErrorWinOverBufferedRecords(t *testing.T) {
	c := &callbackTestConsumer{ctx: &callbackTestContext{requested: make(chan struct{}), closed: make(chan struct{})}, count: 10, returned: make(chan int, 10)}
	d, err := newChunkedCallbackDelivery(c)
	if err != nil {
		t.Fatal(err)
	}
	call, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	defer d.stopAndJoin(call)
	for n := 0; n < 10; n++ {
		select {
		case <-c.returned:
		case <-call.Done():
			t.Fatal("no delivery")
		}
	}
	if _, err := d.next(call); err != nil {
		t.Fatal(err)
	}
	d.errors <- jetstream.ErrServerShutdown
	if _, err := d.next(call); !errors.Is(err, jetstream.ErrServerShutdown) {
		t.Fatalf("error lost: %v", err)
	}
	canceled, stop := context.WithCancel(call)
	stop()
	if _, err := d.next(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel lost: %v", err)
	}
	if err := d.stopAndJoin(call); err != nil {
		t.Fatal(err)
	}
	if _, err := d.next(call); !errors.Is(err, jetstream.ErrMsgIteratorClosed) {
		t.Fatalf("stop lost: %v", err)
	}
}
