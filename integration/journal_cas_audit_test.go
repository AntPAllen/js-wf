package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/journal"
)

type casAuditCursor interface {
	Fetch(int, ...jetstream.FetchOpt) (jetstream.MessageBatch, error)
}

// This independent raw audit preserves its verified prefix when replacing a
// transiently unavailable consumer. Semantic validation always fails directly.
func auditCASJournal(ctx context.Context, total int, open func(uint64) (casAuditCursor, error)) (count int, tail uint64, err error) {
	var cursor casAuditCursor
	var epoch uint64
	failures := 0
	for count < total {
		if err := ctx.Err(); err != nil {
			return count, tail, err
		}
		if cursor == nil {
			cursor, err = open(tail + 1)
			if err != nil {
				return count, tail, err
			}
		}
		limit := min(512, total-count)
		batch, fetchErr := cursor.Fetch(limit, jetstream.FetchMaxWait(5*time.Second))
		before := count
		if batch != nil {
			for msg := range batch.Messages() {
				metadata, err := msg.Metadata()
				if err != nil {
					return count, tail, fmt.Errorf("metadata at %d: %w", count, err)
				}
				var entry journal.Entry
				if err := json.Unmarshal(msg.Data(), &entry); err != nil {
					return count, tail, fmt.Errorf("decode at %d: %w", count, err)
				}
				kind := journal.StepRequested
				if count%2 == 0 {
					kind = journal.StepCompleted
				}
				if count >= total || msg.Subject() != "wf.jrn.cas.scale" || entry.Index != uint64(count) || metadata.Sequence.Stream <= tail || entry.Epoch < epoch ||
					(count == 0 && entry.Kind != journal.Started) || (count > 0 && (entry.Kind != kind || entry.WorkerID != "a" && entry.WorkerID != "b")) {
					return count, tail, fmt.Errorf("invalid entry %d: subject=%s seq=%d entry=%+v prior_seq=%d prior_epoch=%d", count, msg.Subject(), metadata.Sequence.Stream, entry, tail, epoch)
				}
				tail, epoch = metadata.Sequence.Stream, entry.Epoch
				count++
			}
			// A semantic batch error must remain visible even after full delivery.
			if batchErr := batch.Error(); batchErr != nil {
				fetchErr = errors.Join(fetchErr, batchErr)
			}
		}
		if count > before {
			failures = 0
		}
		if fetchErr == nil && count > before {
			continue
		}
		if fetchErr == nil {
			fetchErr = fmt.Errorf("audit made no progress at index %d", count)
			return count, tail, fetchErr
		}
		if !casAuditTransient(fetchErr) || failures >= 3 {
			return count, tail, fetchErr
		}
		failures++
		cursor = nil
		select {
		case <-ctx.Done():
			return count, tail, ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}
	return count, tail, nil
}

func casAuditTransient(err error) bool {
	// Inspect each joined cause: a transport error cannot hide a semantic error.
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, cause := range joined.Unwrap() {
			if !casAuditTransient(cause) {
				return false
			}
		}
		return true
	}
	if err == nats.ErrNoResponders || err == nats.ErrTimeout || err == jetstream.ErrConsumerDeleted || err == jetstream.ErrNoStreamResponse {
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return casAuditTransient(wrapped.Unwrap())
	}
	var api *jetstream.APIError
	return errors.Is(err, nats.ErrNoResponders) || errors.Is(err, nats.ErrTimeout) || errors.Is(err, jetstream.ErrConsumerDeleted) || errors.Is(err, jetstream.ErrNoStreamResponse) || errors.As(err, &api) && api.ErrorCode == 10008
}

type casAuditMessage struct {
	jetstream.Msg
	sequence uint64
	data     []byte
}

func (m casAuditMessage) Subject() string { return "wf.jrn.cas.scale" }
func (m casAuditMessage) Data() []byte    { return m.data }
func (m casAuditMessage) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{Sequence: jetstream.SequencePair{Stream: m.sequence}}, nil
}

type casAuditBatch struct {
	messages []jetstream.Msg
	err      error
}

func (b casAuditBatch) Messages() <-chan jetstream.Msg {
	ch := make(chan jetstream.Msg, len(b.messages))
	for _, m := range b.messages {
		ch <- m
	}
	close(ch)
	return ch
}
func (b casAuditBatch) Error() error { return b.err }

type casAuditFakeCursor struct{ batches []casAuditBatch }

func (c *casAuditFakeCursor) Fetch(int, ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	if len(c.batches) == 0 {
		return nil, nats.ErrNoResponders
	}
	b := c.batches[0]
	c.batches = c.batches[1:]
	return b, nil
}
func casAuditRecord(index uint64) jetstream.Msg {
	kind := journal.StepRequested
	if index == 0 {
		kind = journal.Started
	} else if index%2 == 0 {
		kind = journal.StepCompleted
	}
	data, _ := json.Marshal(journal.Entry{Epoch: 1, Index: index, Kind: kind, WorkerID: "a"})
	return casAuditMessage{sequence: index + 10, data: data}
}

func TestCASAuditResumesAfterPartialTransientBatch(t *testing.T) {
	for _, fault := range []error{nats.ErrNoResponders, jetstream.ErrConsumerDeleted} {
		t.Run(fault.Error(), func(t *testing.T) {
			var starts []uint64
			open := func(from uint64) (casAuditCursor, error) {
				starts = append(starts, from)
				if len(starts) == 1 {
					return &casAuditFakeCursor{batches: []casAuditBatch{{messages: []jetstream.Msg{casAuditRecord(0), casAuditRecord(1)}, err: fault}}}, nil
				}
				return &casAuditFakeCursor{batches: []casAuditBatch{{messages: []jetstream.Msg{casAuditRecord(2), casAuditRecord(3)}}}}, nil
			}
			n, tail, err := auditCASJournal(context.Background(), 4, open)
			if err != nil || n != 4 || tail != 13 || len(starts) != 2 || starts[0] != 1 || starts[1] != 12 {
				t.Fatalf("audit n=%d tail=%d starts=%v err=%v", n, tail, starts, err)
			}
		})
	}
}

func TestCASAuditDoesNotHideGapOrSemanticError(t *testing.T) {
	for _, mode := range []string{"gap", "duplicate", "corrupt", "wrong_worker", "semantic_after_all_entries"} {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			open := func(uint64) (casAuditCursor, error) {
				calls++
				if calls == 1 {
					return &casAuditFakeCursor{batches: []casAuditBatch{{messages: []jetstream.Msg{casAuditRecord(0)}, err: nats.ErrNoResponders}}}, nil
				}
				batch := casAuditBatch{messages: []jetstream.Msg{casAuditRecord(2)}}
				if mode == "duplicate" {
					batch.messages = []jetstream.Msg{casAuditRecord(0)}
				}
				if mode == "corrupt" {
					batch.messages = []jetstream.Msg{casAuditMessage{sequence: 11, data: []byte("invalid-json")}}
				}
				if mode == "wrong_worker" {
					data, _ := json.Marshal(journal.Entry{Epoch: 1, Index: 1, Kind: journal.StepRequested, WorkerID: "unknown"})
					batch.messages = []jetstream.Msg{casAuditMessage{sequence: 11, data: data}}
				}
				if mode == "semantic_after_all_entries" {
					batch.messages = []jetstream.Msg{casAuditRecord(1)}
					batch.err = fmt.Errorf("wrapped batch: %w", errors.Join(nats.ErrNoResponders, errors.New("permission denied")))
				}
				return &casAuditFakeCursor{batches: []casAuditBatch{batch}}, nil
			}
			_, _, err := auditCASJournal(context.Background(), 2, open)
			if err == nil || calls != 2 {
				t.Fatalf("bad audit accepted or retried: calls=%d err=%v", calls, err)
			}
		})
	}
}

func TestCASAuditBoundsUnavailableConsumer(t *testing.T) {
	calls := 0
	_, _, err := auditCASJournal(context.Background(), 1, func(uint64) (casAuditCursor, error) { calls++; return &casAuditFakeCursor{}, nil })
	if !errors.Is(err, nats.ErrNoResponders) || calls != 4 {
		t.Fatalf("unbounded retry: calls=%d err=%v", calls, err)
	}
}
