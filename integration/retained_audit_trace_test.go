//go:build linux

package integration_test

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Opt-in audit-only delegation. It never changes request contexts/options,
// retries or results. Counts are per method; only 64 completed calls are kept.
type retainedAuditTrace struct {
	mu     sync.Mutex
	counts map[string]retainedAuditCount
	recent []retainedAuditCall
}
type retainedAuditCount struct {
	Started       int       `json:"started"`
	Completed     int       `json:"completed"`
	Errors        int       `json:"errors"`
	Bytes         int       `json:"bytes"`
	ElapsedNS     int64     `json:"elapsed_ns"`
	MaxNS         int64     `json:"max_ns"`
	LastCompleted time.Time `json:"last_completed,omitempty"`
	LastSuccess   time.Time `json:"last_success,omitempty"`
}
type retainedAuditCall struct {
	Operation string    `json:"operation"`
	Detail    string    `json:"detail,omitempty"`
	Started   time.Time `json:"started"`
	Completed time.Time `json:"completed"`
	Deadline  time.Time `json:"deadline,omitempty"`
	Error     string    `json:"error,omitempty"`
}
type retainedAuditTraceSnapshot struct {
	Counts map[string]retainedAuditCount `json:"counts"`
	Recent []retainedAuditCall           `json:"recent"`
}

func (t *retainedAuditTrace) begin(ctx context.Context, operation, detail string) func(error, int) {
	return t.beginSampled(ctx, operation, detail, true)
}

// Iterator calls aggregate every returned byte; only slow/error calls enter the
// bounded recent history so buffered delivery does not evict recovery evidence.
func (t *retainedAuditTrace) beginSampled(ctx context.Context, operation, detail string, retainFast bool) func(error, int) {
	began := time.Now().UTC()
	deadline, _ := ctx.Deadline()
	t.mu.Lock()
	if t.counts == nil {
		t.counts = make(map[string]retainedAuditCount)
	}
	count := t.counts[operation]
	count.Started++
	t.counts[operation] = count
	t.mu.Unlock()
	return func(err error, bytes int) {
		completed := time.Now().UTC()
		elapsed := completed.Sub(began).Nanoseconds()
		call := retainedAuditCall{Operation: operation, Detail: detail, Started: began, Completed: completed, Deadline: deadline}
		if err != nil {
			call.Error = err.Error()
		}
		t.mu.Lock()
		defer t.mu.Unlock()
		count := t.counts[operation]
		count.Completed++
		if completed.After(count.LastCompleted) {
			count.LastCompleted = completed
		}
		if err == nil && completed.After(count.LastSuccess) {
			count.LastSuccess = completed
		}
		count.Bytes += bytes
		count.ElapsedNS += elapsed
		if elapsed > count.MaxNS {
			count.MaxNS = elapsed
		}
		if err != nil {
			count.Errors++
		}
		t.counts[operation] = count
		if !retainFast && err == nil && elapsed < int64(10*time.Millisecond) {
			return
		}
		if len(t.recent) == 64 {
			copy(t.recent, t.recent[1:])
			t.recent = t.recent[:63]
		}
		t.recent = append(t.recent, call)
	}
}
func (t *retainedAuditTrace) snapshot() retainedAuditTraceSnapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	snapshot := retainedAuditTraceSnapshot{Counts: make(map[string]retainedAuditCount), Recent: append([]retainedAuditCall(nil), t.recent...)}
	for k, v := range t.counts {
		snapshot.Counts[k] = v
	}
	return snapshot
}

type tracedAuditJS struct {
	jetstream.JetStream
	trace *retainedAuditTrace
}

func (j tracedAuditJS) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	done := j.trace.begin(ctx, name+".Stream", "")
	stream, err := j.JetStream.Stream(ctx, name)
	done(err, 0)
	if err != nil {
		return stream, err
	}
	return tracedAuditStream{Stream: stream, name: name, trace: j.trace}, nil
}
func (j tracedAuditJS) KeyValue(ctx context.Context, name string) (jetstream.KeyValue, error) {
	done := j.trace.begin(ctx, name+".KeyValue", "")
	kv, err := j.JetStream.KeyValue(ctx, name)
	done(err, 0)
	if err != nil {
		return kv, err
	}
	return tracedAuditKV{KeyValue: kv, name: name, trace: j.trace}, nil
}

type tracedAuditStream struct {
	jetstream.Stream
	name  string
	trace *retainedAuditTrace
}

func (s tracedAuditStream) Info(ctx context.Context, opts ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
	done := s.trace.begin(ctx, s.name+".Info", "")
	info, err := s.Stream.Info(ctx, opts...)
	done(err, 0)
	return info, err
}
func (s tracedAuditStream) GetMsg(ctx context.Context, sequence uint64, opts ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
	done := s.trace.begin(ctx, s.name+".GetMsg", strconv.FormatUint(sequence, 10))
	msg, err := s.Stream.GetMsg(ctx, sequence, opts...)
	bytes := 0
	if msg != nil {
		bytes = len(msg.Data)
	}
	done(err, bytes)
	return msg, err
}
func (s tracedAuditStream) GetLastMsgForSubject(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	done := s.trace.begin(ctx, s.name+".GetLastMsgForSubject", subject)
	msg, err := s.Stream.GetLastMsgForSubject(ctx, subject)
	bytes := 0
	if msg != nil {
		bytes = len(msg.Data)
	}
	done(err, bytes)
	return msg, err
}

// Bulk bytes and the Fetch context are not inspected: the options and message
// channel are delegated unchanged. Fetch elapsed time spans delivery and caller
// processing, and completion is observed when the caller checks batch.Error.
func (s tracedAuditStream) CreateConsumer(ctx context.Context, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	done := s.trace.begin(ctx, s.name+".CreateConsumer", fmt.Sprintf("name=%s replicas=%d", cfg.Name, cfg.Replicas))
	consumer, err := s.Stream.CreateConsumer(ctx, cfg)
	done(err, 0)
	if err != nil {
		return consumer, err
	}
	return tracedAuditConsumer{Consumer: consumer, name: s.name, consumer: cfg.Name, trace: s.trace}, nil
}
func (s tracedAuditStream) DeleteConsumer(ctx context.Context, name string) error {
	done := s.trace.begin(ctx, s.name+".DeleteConsumer", name)
	err := s.Stream.DeleteConsumer(ctx, name)
	done(err, 0)
	return err
}

type tracedAuditConsumer struct {
	jetstream.Consumer
	consumer string
	name     string
	trace    *retainedAuditTrace
}

func (c tracedAuditConsumer) Fetch(batch int, opts ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	done := c.trace.begin(context.Background(), c.name+".Fetch", strconv.Itoa(batch))
	messages, err := c.Consumer.Fetch(batch, opts...)
	if err != nil {
		done(err, 0)
		return messages, err
	}
	return &tracedAuditBatch{MessageBatch: messages, done: done}, nil
}

// Pull/Next options are opaque SDK interfaces. Forward them exactly; no caller
// context or deadline is invented for these trace records. Next measures only
// the SDK delivery wait, excluding the audit's later decoding and reduction.
func (c tracedAuditConsumer) Messages(opts ...jetstream.PullMessagesOpt) (jetstream.MessagesContext, error) {
	done := c.trace.begin(context.Background(), c.name+".Messages", c.consumer)
	iterator, err := c.Consumer.Messages(opts...)
	done(err, 0)
	if err != nil {
		return iterator, err
	}
	return tracedAuditIterator{MessagesContext: iterator, name: c.name, consumer: c.consumer, trace: c.trace}, nil
}

type tracedAuditIterator struct {
	jetstream.MessagesContext
	name, consumer string
	trace          *retainedAuditTrace
}

func (i tracedAuditIterator) Next(opts ...jetstream.NextOpt) (jetstream.Msg, error) {
	done := i.trace.beginSampled(context.Background(), i.name+".Next", i.consumer, false)
	msg, err := i.MessagesContext.Next(opts...)
	size := 0
	if msg != nil {
		size = len(msg.Data())
	}
	done(err, size)
	return msg, err
}

type tracedAuditBatch struct {
	jetstream.MessageBatch
	once sync.Once
	done func(error, int)
}

func (b *tracedAuditBatch) Error() error {
	err := b.MessageBatch.Error()
	b.once.Do(func() { b.done(err, 0) })
	return err
}

type tracedAuditKV struct {
	jetstream.KeyValue
	name  string
	trace *retainedAuditTrace
}

func (s tracedAuditKV) WatchAll(ctx context.Context, opts ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
	done := s.trace.begin(ctx, s.name+".WatchAll", "")
	watch, err := s.KeyValue.WatchAll(ctx, opts...)
	done(err, 0)
	if err != nil {
		return watch, err
	}
	return tracedAuditWatch{KeyWatcher: watch, name: s.name, trace: s.trace}, nil
}

// Keep the native update channel and its buffering. A successful Stop is only
// cleanup evidence; it does not certify the initial-set completion barrier.
type tracedAuditWatch struct {
	jetstream.KeyWatcher
	name  string
	trace *retainedAuditTrace
}

func (w tracedAuditWatch) Stop() error {
	done := w.trace.begin(context.Background(), w.name+".WatchStop", "")
	err := w.KeyWatcher.Stop()
	done(err, 0)
	return err
}

func (s tracedAuditKV) Keys(ctx context.Context, opts ...jetstream.WatchOpt) ([]string, error) {
	done := s.trace.begin(ctx, s.name+".Keys", "")
	keys, err := s.KeyValue.Keys(ctx, opts...)
	done(err, 0)
	return keys, err
}
func (s tracedAuditKV) Get(ctx context.Context, key string) (jetstream.KeyValueEntry, error) {
	done := s.trace.begin(ctx, s.name+".Get", key)
	value, err := s.KeyValue.Get(ctx, key)
	bytes := 0
	if value != nil {
		bytes = len(value.Value())
	}
	done(err, bytes)
	return value, err
}
