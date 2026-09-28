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
	four := &fakeCASRejectJS{JetStream: all[1]}
	four.remaining.Store(4)
	third, err := journal.New(four).Append(ctx, "cas", "false-stale", journal.Entry{Epoch: 1, Index: 2, Kind: journal.StepCompleted, WorkerID: "four"}, second)
	if err != nil || third <= second || four.calls.Load() != 5 {
		t.Fatalf("four unchanged-tail rejections: seq=%d calls=%d err=%v", third, four.calls.Load(), err)
	}
	forty := &fakeCASRejectJS{JetStream: all[2]}
	forty.remaining.Store(40)
	_, err = journal.New(forty).Append(ctx, "cas", "false-stale", journal.Entry{Epoch: 1, Index: 3, Kind: journal.StepRequested, WorkerID: "rejected"}, third)
	if !errors.Is(err, journal.ErrUnknown) || errors.Is(err, journal.ErrStale) || forty.calls.Load() != 40 {
		t.Fatalf("persistent unchanged-tail rejections: calls=%d err=%v", forty.calls.Load(), err)
	}
	fourth, err := base.Append(ctx, "cas", "false-stale", journal.Entry{Epoch: 1, Index: 3, Kind: journal.StepRequested, WorkerID: "winner"}, third)
	if err != nil || fourth <= third {
		t.Fatalf("winner after false rejections: seq=%d err=%v", fourth, err)
	}
	_, err = journal.New(all[1]).Append(ctx, "cas", "false-stale", journal.Entry{Epoch: 1, Index: 3, Kind: journal.StepRequested, WorkerID: "loser"}, third)
	if !errors.Is(err, journal.ErrStale) {
		t.Fatalf("advanced tail must be stale: %v", err)
	}
	records, tail, err := base.Read(ctx, "cas", "false-stale")
	if err != nil || len(records) != 4 || tail != fourth || records[3].WorkerID != "winner" {
		t.Fatalf("journal after CAS rejections: records=%v tail=%d err=%v", records, tail, err)
	}
}
