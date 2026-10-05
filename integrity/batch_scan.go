package integrity

// Opt-in bulk retained reads use the same captured bounds and invariant checker.
import (
	"context"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
	"js-wf/internal/natsutil"
	"time"
)

func scanBatchThrough(ctx context.Context, stream jetstream.Stream, cutoff *uint64, visit func(*jetstream.RawStreamMsg) error) error {
	return scanBatchThroughWithSize(ctx, stream, cutoff, visit, 512)
}

// Size controls one bounded delivery window, not the captured audit cohort or
// its deadline. Native controls compare the previous 512-record window on the
// same retained stores using the same invariant-checking body.
func scanBatchThroughWithSize(ctx context.Context, stream jetstream.Stream, cutoff *uint64, visit func(*jetstream.RawStreamMsg) error, batchSize uint64) error {
	return scanBatchThroughWithFetcher(ctx, stream, cutoff, visit, batchSize, nil)
}

type retainedBatchFetcher func(context.Context, jetstream.Consumer, int) (jetstream.MessageBatch, error)

func scanBatchThroughWithFetcher(ctx context.Context, stream jetstream.Stream, cutoff *uint64, visit func(*jetstream.RawStreamMsg) error, batchSize uint64, fetch retainedBatchFetcher) error {
	if fetch == nil {
		fetch = func(call context.Context, c jetstream.Consumer, n int) (jetstream.MessageBatch, error) {
			return c.Fetch(n, jetstream.FetchContext(call))
		}
	}
	if batchSize == 0 || batchSize > 4096 {
		return errors.New("retained batch scan: invalid delivery window")
	}
	info, err := auditRead(ctx, func(call context.Context) (*jetstream.StreamInfo, error) { return stream.Info(call) })
	if err != nil {
		return err
	}
	first, last := info.State.FirstSeq, info.State.LastSeq
	if cutoff != nil && last > *cutoff {
		last = *cutoff
	}
	if first == 0 || first > last {
		return nil
	}
	// Each cursor has a stable identity across lost-create retries. A resumed
	// cursor starts at the first record not yet accepted by the visitor.
	var consumerNames []string
	defer func() {
		cleanup := context.WithoutCancel(ctx)
		if deadline, ok := ctx.Deadline(); ok {
			var stop context.CancelFunc
			cleanup, stop = context.WithDeadline(cleanup, deadline)
			defer stop()
		}
		call, stop := context.WithTimeout(cleanup, 2*time.Second)
		defer stop()
		for _, name := range consumerNames {
			_ = stream.DeleteConsumer(call, name)
		}
	}()
	create := func() (jetstream.Consumer, error) {
		name := "wf-audit-" + nuid.Next()
		consumerNames = append(consumerNames, name)
		return auditRead(ctx, func(call context.Context) (jetstream.Consumer, error) {
			return stream.CreateConsumer(call, jetstream.ConsumerConfig{
				Name: name, DeliverPolicy: jetstream.DeliverByStartSequencePolicy, OptStartSeq: first,
				AckPolicy: jetstream.AckNonePolicy, ReplayPolicy: jetstream.ReplayInstantPolicy,
				MemoryStorage: true, Replicas: info.Config.Replicas, InactiveThreshold: 30 * time.Second,
			})
		})
	}
	consumer, err := create()
	if err != nil {
		return err
	}
	resumes := 0
	// Bound recovery even for callers without a deadline; creation retries and
	// fetches share the original audit context. Exhaustion keeps the leader-read
	// fallback, and semantic errors remain fatal.
	resume := func() (bool, error) {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		if first > last || resumes >= 2 {
			return false, nil
		}
		resumes++
		replacement, err := create()
		if err != nil {
			return false, err
		}
		consumer = replacement
		return true, nil
	}
	// Consumer delivery cannot prove absence. The leader's documented next
	// message query checks gaps/tails without one request per deleted sequence.
	// An existing omitted record is visited before later consumer delivery.
	resolveRecords := func(end uint64, limit int) error {
		resolved := 0
		for first <= end {
			msg, err := auditRead(ctx, func(call context.Context) (*jetstream.RawStreamMsg, error) {
				return stream.GetMsg(call, first, jetstream.WithGetMsgSubject(">"))
			})
			if errors.Is(err, jetstream.ErrMsgNotFound) {
				first = end + 1
				return nil
			}
			if err != nil {
				return err
			}
			if msg == nil || msg.Sequence < first {
				return fmt.Errorf("retained batch scan: invalid next sequence at %d", first)
			}
			if msg.Sequence > end {
				first = end + 1
				return nil
			}
			if err := visit(msg); err != nil {
				return err
			}
			first = msg.Sequence + 1
			resolved++
			if limit > 0 && resolved >= limit {
				return nil
			}
		}
		return nil
	}
	resolve := func(end uint64) error { return resolveRecords(end, 0) }
	for first <= last {
		call, stop := context.WithTimeout(ctx, 2*time.Second)
		requested := int(min(batchSize, last-first+1))
		batch, err := fetch(call, consumer, requested)
		if err != nil {
			stop()
			if batchReadTransportError(err) {
				resumed, resumeErr := resume()
				if resumeErr != nil {
					return resumeErr
				}
				if resumed {
					continue
				}
				return resolve(last)
			}
			return err
		}
		var visitErr error
		delivered := 0
		for msg := range batch.Messages() {
			delivered++
			if visitErr == nil && ctx.Err() != nil {
				visitErr = ctx.Err()
				stop()
			}
			if visitErr != nil {
				continue
			} // Drain before cancellation/cleanup.
			meta, err := msg.Metadata()
			if err != nil {
				visitErr = err
				continue
			}
			if meta == nil {
				visitErr = errors.New("retained batch scan: missing metadata")
				continue
			}
			seq := meta.Sequence.Stream
			if meta.Stream != info.Config.Name || seq < first {
				visitErr = fmt.Errorf("retained batch scan: unexpected stream/order %s/%d", meta.Stream, seq)
				continue
			}
			if seq > last {
				visitErr = resolve(last)
				continue
			}
			if seq > first {
				visitErr = resolve(seq - 1)
			}
			if visitErr != nil {
				continue
			}
			visitErr = visit(&jetstream.RawStreamMsg{Subject: msg.Subject(), Sequence: seq, Header: msg.Headers(), Data: msg.Data(), Time: meta.Timestamp})
			first = seq + 1
		}
		batchErr := batch.Error()
		stop()
		if visitErr != nil {
			return visitErr
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if batchErr != nil {
			if !batchReadTransportError(batchErr) {
				return batchErr
			}
			resumed, resumeErr := resume()
			if resumeErr != nil {
				return resumeErr
			}
			if resumed {
				continue
			}
			return resolve(last)
		}
		if delivered < requested {
			// Fetch may report timeout/no-message status as a successful short
			// batch. Prove whether the tail exists through the stream leader,
			// accepting at most one record before restarting bulk delivery.
			if err := resolveRecords(last, 1); err != nil {
				return err
			}
			resumed, resumeErr := resume()
			if resumeErr != nil {
				return resumeErr
			}
			if resumed {
				continue
			}
			return resolve(last)
		}
	}
	return nil
}

// Only transport interruptions permit cursor recovery or leader-read fallback.
// Semantic metadata/payload/visitor errors never fall back.
func batchReadTransportError(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrTimeout) ||
		errors.Is(err, nats.ErrNoResponders) || errors.Is(err, jetstream.ErrNoStreamResponse) || natsutil.IsUnavailable(err)
}
