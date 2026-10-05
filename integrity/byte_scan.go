package integrity

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Experimental reader: the invariant algorithm and captured bounds are shared
// with the bulk reader; defaults do not select this transport yet.
func scanByteBoundedThrough(ctx context.Context, stream jetstream.Stream, cutoff *uint64, visit func(*jetstream.RawStreamMsg) error) error {
	return scanBatchThroughWithFetcher(ctx, stream, cutoff, visit, 4096, func(call context.Context, c jetstream.Consumer, n int) (jetstream.MessageBatch, error) {
		return fetchByteBounded(call, c, n, 8<<20)
	})
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
// and is unsuitable for this purpose. StopAfter bounds delivered records.
func fetchByteBounded(ctx context.Context, c jetstream.Consumer, n, maxBytes int) (jetstream.MessageBatch, error) {
	if n < 1 || n > 4096 || maxBytes < 1 {
		return nil, fmt.Errorf("retained byte scan: invalid window")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	iterator, err := c.Messages(jetstream.PullMaxBytes(maxBytes), jetstream.StopAfter(n), jetstream.PullExpiry(2*time.Second))
	if err != nil {
		return nil, err
	}
	batch := &byteBoundedBatch{messages: make(chan jetstream.Msg)}
	stopOnCancel := context.AfterFunc(ctx, iterator.Stop)
	go func() {
		defer close(batch.messages)
		defer iterator.Stop()
		defer stopOnCancel()
		for i := 0; i < n; i++ {
			msg, err := iterator.Next(jetstream.NextContext(ctx))
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
	return batch, nil
}
