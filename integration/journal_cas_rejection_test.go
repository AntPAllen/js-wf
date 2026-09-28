package integration_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/journal"

	"github.com/nats-io/nats.go/jetstream"
)

// fakeCASRejectJS reproduces the observed wrong-last-sequence reply when the
// retained subject tail has not changed. It does not forward rejected writes.
type fakeCASRejectJS struct {
	jetstream.JetStream
	remaining atomic.Int32
	calls     atomic.Int32
}

func (f *fakeCASRejectJS) Publish(ctx context.Context, subject string, payload []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	f.calls.Add(1)
	for {
		remaining := f.remaining.Load()
		if remaining <= 0 {
			return f.JetStream.Publish(ctx, subject, payload, opts...)
		}
		if f.remaining.CompareAndSwap(remaining, remaining-1) {
			return nil, fmt.Errorf("nats: %w", &jetstream.APIError{Code: 400, ErrorCode: jetstream.JSErrCodeStreamWrongLastSequenceConstant, Description: "wrong last sequence"})
		}
	}
}

func TestJournalCASRejectionWithUnchangedTail(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	base := journal.New(all[0])
	first, err := base.Append(ctx, "cas", "false-stale", journal.Entry{Epoch: 1, Index: 0, Kind: journal.Started}, 0)
	if err != nil {
		t.Fatal(err)
	}
	one := &fakeCASRejectJS{JetStream: all[1]}
	one.remaining.Store(1)
	second, err := journal.New(one).Append(ctx, "cas", "false-stale", journal.Entry{Epoch: 1, Index: 1, Kind: journal.StepRequested, WorkerID: "one"}, first)
	if err != nil || second <= first || one.calls.Load() != 2 {
		t.Fatalf("one unchanged-tail rejection: first=%d second=%d publish_calls=%d err=%v", first, second, one.calls.Load(), err)
	}
	three := &fakeCASRejectJS{JetStream: all[2]}
	three.remaining.Store(3)
	_, err = journal.New(three).Append(ctx, "cas", "false-stale", journal.Entry{Epoch: 1, Index: 2, Kind: journal.StepCompleted, WorkerID: "three"}, second)
	if !errors.Is(err, journal.ErrUnknown) || errors.Is(err, journal.ErrStale) || three.calls.Load() != 3 {
		t.Fatalf("persistent unchanged-tail rejections: calls=%d err=%v", three.calls.Load(), err)
	}
	third, err := base.Append(ctx, "cas", "false-stale", journal.Entry{Epoch: 1, Index: 2, Kind: journal.StepCompleted, WorkerID: "winner"}, second)
	if err != nil || third <= second {
		t.Fatalf("winner after false rejections: seq=%d err=%v", third, err)
	}
	_, err = journal.New(all[1]).Append(ctx, "cas", "false-stale", journal.Entry{Epoch: 1, Index: 2, Kind: journal.StepCompleted, WorkerID: "loser"}, second)
	if !errors.Is(err, journal.ErrStale) {
		t.Fatalf("advanced tail must be stale: %v", err)
	}
	records, tail, err := base.Read(ctx, "cas", "false-stale")
	if err != nil || len(records) != 3 || tail != third || records[2].WorkerID != "winner" {
		t.Fatalf("journal after CAS rejections: records=%v tail=%d err=%v", records, tail, err)
	}
}
