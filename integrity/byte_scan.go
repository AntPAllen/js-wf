package integrity

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Streaming audit delivery shares the bulk reader's invariant algorithm and
// captured bounds. The point reader and non-streaming bulk reader are separate.
func scanByteBoundedThrough(ctx context.Context, stream jetstream.Stream, cutoff *uint64, visit func(*jetstream.RawStreamMsg) error) error {
	return scanByteBoundedThroughWithBatchTransform(ctx, stream, cutoff, visit, nil)
}

// The nil transform is the production path. Native controls may interrupt a
// batch without replacing the production iterator lifecycle or recovery logic.
func scanByteBoundedThroughWithBatchTransform(ctx context.Context, stream jetstream.Stream, cutoff *uint64, visit func(*jetstream.RawStreamMsg) error, transform func(jetstream.MessageBatch) jetstream.MessageBatch) error {
	// Keep the byte-bounded iterator across record batches. Stopping it at each
	// record cap would discard prefetched AckNone messages and force gap reads.
	var iterator jetstream.MessagesContext
	var consumerName, initialLeader, consumerStream string
	defer func() {
		if iterator != nil {
			iterator.Stop()
		}
	}()
	return scanBatchThroughWithReplayCheck(ctx, stream, cutoff, visit, 4096, func(call context.Context, c jetstream.Consumer, n int) (jetstream.MessageBatch, error) {
		if err := call.Err(); err != nil {
			return nil, err
		}
		cached := c.CachedInfo()
		if cached == nil || cached.Name == "" {
			return nil, fmt.Errorf("retained byte scan: missing consumer identity")
		}
		name := cached.Name
		if iterator == nil || name != consumerName {
			if iterator != nil {
				iterator.Stop()
			}
			var err error
			iterator, err = c.Messages(jetstream.PullMaxBytes(8<<20), jetstream.PullExpiry(2*time.Second))
			if err != nil {
				return nil, err
			}
			consumerName = name
			consumerStream = cached.Stream
			initialLeader = ""
			if cached.Cluster != nil {
				initialLeader = cached.Cluster.Leader
			}
		}
		batch := byteIteratorBatch(call, iterator, n, false)
		if transform != nil {
			batch = transform(batch)
		}
		return batch, nil
	}, func(call context.Context, c jetstream.Consumer) (bool, error) {
		return confirmedByteConsumerLeaderMove(call, c, consumerName, consumerStream, initialLeader)
	})
}

// A backward sequence alone cannot justify recovery. Compare the actual current
// replicated memory/AckNone consumer with its captured initial leader. A fresh
// cursor resumes at the first record not accepted by the visitor; the common
// scanner retains its original two-resume budget and leader gap oracle.
func confirmedByteConsumerLeaderMove(ctx context.Context, c jetstream.Consumer, name, stream, leader string) (bool, error) {
	if leader == "" {
		return false, nil
	}
	info, err := auditRead(ctx, func(call context.Context) (*jetstream.ConsumerInfo, error) { return c.Info(call) })
	if err != nil {
		return false, err
	}
	return info != nil && info.Name == name && info.Stream == stream && info.Config.MemoryStorage && info.Config.AckPolicy == jetstream.AckNonePolicy && info.Config.Replicas > 1 && info.Cluster != nil && info.Cluster.Leader != "" && info.Cluster.Leader != leader, nil
}

type byteBoundedBatch struct {
	messages chan jetstream.Msg
	err      error
}

func (b *byteBoundedBatch) Messages() <-chan jetstream.Msg { return b.messages }

// Read only after Messages closes, as required by the MessageBatch contract.
func (b *byteBoundedBatch) Error() error { return b.err }

// PullMaxBytes is the documented client-buffer byte limit. The combined
// PullMaxMessagesWithBytesLimit option only limits individual server requests
// and is unsuitable for this purpose. The adapter bounds delivered records.
// Combining SDK StopAfter with PullMaxBytes prevents healthy refill in the pinned
// SDK: its pending message count uses the uncapped request size. Do not combine
// those options. The scanner retains the iterator between record windows.
func fetchByteBounded(ctx context.Context, c jetstream.Consumer, n, maxBytes int) (jetstream.MessageBatch, error) {
	if n < 1 || n > 4096 || maxBytes < 1 {
		return nil, fmt.Errorf("retained byte scan: invalid window")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	iterator, err := c.Messages(jetstream.PullMaxBytes(maxBytes), jetstream.PullExpiry(2*time.Second))
	if err != nil {
		return nil, err
	}
	return byteIteratorBatch(ctx, iterator, n, true), nil
}

func byteIteratorBatch(ctx context.Context, iterator jetstream.MessagesContext, n int, stopAfterBatch bool) jetstream.MessageBatch {
	batch := &byteBoundedBatch{messages: make(chan jetstream.Msg)}
	stopOnCancel := context.AfterFunc(ctx, iterator.Stop)
	go func() {
		defer close(batch.messages)
		if stopAfterBatch {
			defer iterator.Stop()
		}
		defer stopOnCancel()
		// NextContext only assigns this window's immutable context. Reuse its
		// option rather than allocating a new closure for every delivered record.
		nextContext := jetstream.NextContext(ctx)
		for i := 0; i < n; i++ {
			msg, err := iterator.Next(nextContext)
			if err != nil {
				if ctx.Err() != nil {
					batch.err = ctx.Err()
				} else if !errors.Is(err, jetstream.ErrMsgIteratorClosed) {
					batch.err = err
					// Missing delivery heartbeats are transport failures, not
					// evidence about retained data. Preserve the original error
					// while admitting the shared bounded timeout recovery path.
					if errors.Is(err, jetstream.ErrNoHeartbeat) {
						batch.err = errors.Join(nats.ErrTimeout, err)
					}
				}
				return
			}
			select {
			case batch.messages <- msg:
			case <-ctx.Done():
				batch.err = ctx.Err()
				return
			}
		}
	}()
	return batch
}
