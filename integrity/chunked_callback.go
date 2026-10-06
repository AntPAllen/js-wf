package integrity

import (
	"context"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const callbackChunkRecords = 256
const callbackChunkBytes = 1 << 20

// Only next's caller owns active. The SDK handler owns enqueue operations.
// Each buffer holds at most 256 records / 1MiB payload, or one oversized record.
// Total adapter retention is at most two such buffers, plus the SDK callback's
// current message. Headers, object overhead and SDK's 8MiB buffer are separate.
// No flush timer is required: the first queued message wakes the reader.
type chunkedCallbackDelivery struct {
	callbackDelivery
	mu            sync.Mutex
	space         *sync.Cond
	ready         chan struct{}
	pending       [callbackChunkRecords]jetstream.Msg
	count, bytes  int
	active        [callbackChunkRecords]jetstream.Msg
	position, end int
}

func newChunkedCallbackDelivery(c jetstream.Consumer) (*chunkedCallbackDelivery, error) {
	d := &chunkedCallbackDelivery{callbackDelivery: callbackDelivery{
		errors: make(chan error, 1), stopped: make(chan struct{}),
	}, ready: make(chan struct{}, 1)}
	d.space = sync.NewCond(&d.mu)
	cc, err := c.Consume(d.enqueue, jetstream.PullMaxBytes(8<<20), jetstream.PullExpiry(2*time.Second), jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
		select {
		case d.errors <- err:
		default:
		}
	}))
	if err != nil {
		return nil, err
	}
	d.consume, d.closed = cc, cc.Closed()
	return d, nil
}

func (d *chunkedCallbackDelivery) enqueue(msg jetstream.Msg) {
	size := len(msg.Data())
	d.mu.Lock()
	defer d.mu.Unlock()
	for {
		select {
		case <-d.stopped:
			return
		default:
		}
		if d.count < callbackChunkRecords && (d.count == 0 || size <= callbackChunkBytes-d.bytes) {
			break
		}
		d.space.Wait()
	}
	d.pending[d.count] = msg
	d.count++
	d.bytes += size
	if d.count == 1 {
		select {
		case d.ready <- struct{}{}:
		default:
		}
	}
}

func (d *chunkedCallbackDelivery) next(ctx context.Context) (jetstream.Msg, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		select {
		case err := <-d.errors:
			return nil, err
		case <-d.stopped:
			return nil, jetstream.ErrMsgIteratorClosed
		default:
		}
		if d.position < d.end {
			msg := d.active[d.position]
			d.active[d.position] = nil
			d.position++
			return msg, nil
		}
		d.mu.Lock()
		d.end, d.position = d.count, 0
		copy(d.active[:], d.pending[:d.count])
		clear(d.pending[:d.count])
		d.count, d.bytes = 0, 0
		d.space.Signal()
		d.mu.Unlock()
		if d.end != 0 {
			continue
		}
		select {
		case <-d.ready:
		case err := <-d.errors:
			return nil, err
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-d.stopped:
			return nil, jetstream.ErrMsgIteratorClosed
		case <-d.closed:
			select {
			case err := <-d.errors:
				return nil, err
			default:
			}
			return nil, jetstream.ErrMsgIteratorClosed
		}
	}
}

func (d *chunkedCallbackDelivery) stop() {
	d.once.Do(func() {
		close(d.stopped)
		d.mu.Lock()
		d.space.Broadcast()
		d.mu.Unlock()
		d.consume.Stop()
	})
}

func (d *chunkedCallbackDelivery) stopAndJoin(ctx context.Context) error {
	d.stop()
	return d.callbackDelivery.stopAndJoin(ctx)
}
