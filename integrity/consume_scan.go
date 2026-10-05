package integrity

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Explicit candidate: the common scanner still owns bounds, gap/absence proof,
// order checks, two-resume recovery and all invariant reductions. No public
// reader selects this path. Consume reuses its heartbeat monitor across records.
func scanConsumeByteBoundedThrough(ctx context.Context, stream jetstream.Stream, cutoff *uint64, visit func(*jetstream.RawStreamMsg) error) (failure error) {
	var delivery *callbackDelivery
	var name, consumerStream, initialLeader string
	defer func() {
		if delivery != nil {
			failure = errors.Join(failure, delivery.stopAndJoin(ctx))
		}
	}()
	return scanBatchThroughWithCoordinates(ctx, stream, cutoff, visit, 4096, func(call context.Context, c jetstream.Consumer, n int) (jetstream.MessageBatch, error) {
		if err := call.Err(); err != nil {
			return nil, err
		}
		info := c.CachedInfo()
		if info == nil || info.Name == "" {
			return nil, fmt.Errorf("retained callback scan: missing consumer identity")
		}
		if delivery == nil || name != info.Name {
			if delivery != nil {
				if err := delivery.stopAndJoin(ctx); err != nil {
					return nil, err
				}
			}
			var err error
			delivery, err = newCallbackDelivery(c)
			if err != nil {
				return nil, err
			}
			name, consumerStream, initialLeader = info.Name, info.Stream, ""
			if info.Cluster != nil {
				initialLeader = info.Cluster.Leader
			}
		}
		current := delivery
		return byteDeliveryBatch(call, n, func() (jetstream.Msg, error) { return current.next(call) }, current.stop, false), nil
	}, func(call context.Context, c jetstream.Consumer) (bool, error) {
		return confirmedByteConsumerLeaderMove(call, c, name, consumerStream, initialLeader)
	}, compactMessageCoordinates)
}

type callbackDelivery struct {
	messages chan jetstream.Msg
	errors   chan error
	stopped  chan struct{}
	once     sync.Once
	consume  jetstream.ConsumeContext
	closed   <-chan struct{}
}

func newCallbackDelivery(c jetstream.Consumer) (*callbackDelivery, error) {
	d := &callbackDelivery{messages: make(chan jetstream.Msg), errors: make(chan error, 1), stopped: make(chan struct{})}
	cc, err := c.Consume(func(msg jetstream.Msg) {
		// No additional payload queue. Stop unblocks a callback waiting for the
		// adapter before unsubscribing, so shutdown cannot strand the handler.
		select {
		case d.messages <- msg:
		case <-d.stopped:
		}
	}, jetstream.PullMaxBytes(8<<20), jetstream.PullExpiry(2*time.Second), jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
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

func (d *callbackDelivery) next(ctx context.Context) (jetstream.Msg, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case err := <-d.errors:
		return nil, err
	default:
	}
	select {
	case msg := <-d.messages:
		return msg, nil
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

func (d *callbackDelivery) stop() {
	d.once.Do(func() { close(d.stopped); d.consume.Stop() })
}

func (d *callbackDelivery) stopAndJoin(ctx context.Context) error {
	d.stop()
	// Join within the caller's remaining deadline. Without a deadline, cap
	// cleanup independently. Completed shutdown wins over expired context.
	select {
	case <-d.closed:
		return nil
	default:
	}
	call, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	if deadline, ok := ctx.Deadline(); ok {
		bounded, stop := context.WithDeadline(call, deadline)
		defer stop()
		call = bounded
	}
	select {
	case <-d.closed:
		return nil
	case <-call.Done():
		return fmt.Errorf("retained callback cleanup: %w", call.Err())
	}
}
