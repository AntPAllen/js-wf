package integrity

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
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

type callbackWindowDelivery interface {
	next(context.Context) (jetstream.Msg, error)
	stop()
	stopAndJoin(context.Context) error
}

// Explicit candidate removing only the additional batch relay goroutine/channel.
func scanConsumeDirectWindowsThrough(ctx context.Context, stream jetstream.Stream, cutoff *uint64, visit func(*jetstream.RawStreamMsg) error) error {
	return scanConsumeDirectWindowsWithFactory(ctx, stream, cutoff, visit, func(c jetstream.Consumer) (callbackWindowDelivery, error) {
		return newCallbackDelivery(c)
	})
}

// Explicit diagnostic candidate: transfer bounded chunks to amortize adapter
// waits. The common scanner retains all ordering, gap, recovery and reductions.
func scanConsumeChunkedWindowsThrough(ctx context.Context, stream jetstream.Stream, cutoff *uint64, visit func(*jetstream.RawStreamMsg) error) error {
	return scanConsumeDirectWindowsWithFactory(ctx, stream, cutoff, visit, func(c jetstream.Consumer) (callbackWindowDelivery, error) {
		return newChunkedCallbackDelivery(c)
	})
}

// Only the explicitly selected chunked audit creates R1 memory cursors. The
// retained source still uses its original replication and leader-read oracle.
type singleReplicaChunkedAuditStream struct{ jetstream.Stream }

func (s singleReplicaChunkedAuditStream) CreateConsumer(ctx context.Context, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	if cfg.Name == "" || !cfg.MemoryStorage || cfg.AckPolicy != jetstream.AckNonePolicy {
		return nil, errors.New("chunked audit: unexpected cursor configuration")
	}
	source := s.Stream.CachedInfo()
	if source == nil || source.Config.Name == "" {
		return nil, errors.New("chunked audit: missing retained stream identity")
	}
	cfg.Replicas = 1
	c, err := s.Stream.CreateConsumer(ctx, cfg)
	if err != nil {
		return nil, err
	}
	info := c.CachedInfo()
	if info == nil || info.Name != cfg.Name || info.Stream != source.Config.Name || info.Config.Replicas != 1 || !info.Config.MemoryStorage || info.Config.AckPolicy != jetstream.AckNonePolicy {
		return nil, errors.New("chunked audit: actual cursor identity/configuration differs")
	}
	return c, nil
}

func scanSingleReplicaChunkedThrough(ctx context.Context, stream jetstream.Stream, cutoff *uint64, visit func(*jetstream.RawStreamMsg) error) error {
	return scanConsumeChunkedWindowsThrough(ctx, singleReplicaChunkedAuditStream{stream}, cutoff, visit)
}

func scanConsumeDirectWindowsWithFactory(ctx context.Context, stream jetstream.Stream, cutoff *uint64, visit func(*jetstream.RawStreamMsg) error, create func(jetstream.Consumer) (callbackWindowDelivery, error)) (failure error) {
	var delivery callbackWindowDelivery
	var name, consumerStream, initialLeader string
	var initial *jetstream.ConsumerInfo
	var lastAccepted uint64
	defer func() {
		if delivery != nil {
			failure = errors.Join(failure, delivery.stopAndJoin(ctx))
		}
	}()
	return scanRetainedThroughWithWindow(ctx, stream, cutoff, func(msg *jetstream.RawStreamMsg) error {
		if err := visit(msg); err != nil {
			return err
		}
		lastAccepted = msg.Sequence
		return nil
	}, 4096, func(call context.Context, c jetstream.Consumer, n int) (retainedMessageWindow, error) {
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
			delivery, err = create(c)
			if err != nil {
				return nil, err
			}
			name, consumerStream, initialLeader = info.Name, info.Stream, ""
			initial = info
			if info.Cluster != nil {
				initialLeader = info.Cluster.Leader
			}
		}
		current := delivery
		return func(accept func(jetstream.Msg)) error { return walkCallbackWindow(call, n, current, accept) }, nil
	}, func(call context.Context, c jetstream.Consumer) (bool, error) {
		if initial != nil && initial.Config.Replicas == 1 {
			return confirmedCallbackConsumerPositionLoss(call, c, initial, lastAccepted)
		}
		return confirmedByteConsumerLeaderMove(call, c, name, consumerStream, initialLeader)
	}, compactMessageCoordinates)
}

func walkCallbackWindow(ctx context.Context, n int, d callbackWindowDelivery, accept func(jetstream.Msg)) error {
	stopOnCancel := context.AfterFunc(ctx, d.stop)
	defer stopOnCancel()
	for i := 0; i < n; i++ {
		msg, err := d.next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(err, jetstream.ErrMsgIteratorClosed) {
				return nil
			}
			if errors.Is(err, jetstream.ErrNoHeartbeat) {
				return errors.Join(nats.ErrTimeout, err)
			}
			return err
		}
		// A window cancellation wins over a just-received message as in the relay.
		if err := ctx.Err(); err != nil {
			return err
		}
		accept(msg)
	}
	return nil
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

// R1 memory consumer assignments survive owner restart, but their volatile
// delivery position can reset. Same name/created/leader alone is not recovery
// proof: require a fresh API observation of both AckNone positions behind an
// already accepted source sequence, with the original complete config unchanged.
// The common scanner replaces the cursor at the unvisited sequence; replayed
// messages never reach the visitor. Ordinary overlap remains a semantic error.
func confirmedCallbackConsumerPositionLoss(ctx context.Context, c jetstream.Consumer, initial *jetstream.ConsumerInfo, accepted uint64) (bool, error) {
	if initial == nil || accepted == 0 || initial.Name == "" || initial.Stream == "" || initial.Created.IsZero() || initial.Cluster == nil || initial.Cluster.Leader == "" || initial.Config.Replicas != 1 || !initial.Config.MemoryStorage || initial.Config.AckPolicy != jetstream.AckNonePolicy {
		return false, nil
	}
	info, err := auditRead(ctx, func(call context.Context) (*jetstream.ConsumerInfo, error) { return c.Info(call) })
	if err != nil {
		return false, err
	}
	return info != nil && info.Name == initial.Name && info.Stream == initial.Stream && info.Created.Equal(initial.Created) && info.Cluster != nil && info.Cluster.Leader == initial.Cluster.Leader && reflect.DeepEqual(info.Config, initial.Config) && info.NumAckPending == 0 && info.NumRedelivered == 0 && info.Delivered.Consumer > 0 && info.Delivered.Stream > 0 && info.Delivered.Stream < accepted && info.AckFloor.Consumer == info.Delivered.Consumer && info.AckFloor.Stream == info.Delivered.Stream, nil
}
