package reconcile

import (
	"context"
	"fmt"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/journal"
)

type signalPrefixPort struct{ stage string }

func (p signalPrefixPort) GetSignal(_ context.Context, seq uint64) (*jetstream.RawStreamMsg, error) {
	if p.stage == "first_read" || (seq == 3 && p.stage == "read") {
		return nil, nats.ErrTimeout
	}
	if seq == 3 && p.stage == "hole_info" {
		return nil, jetstream.ErrMsgNotFound
	}
	return &jetstream.RawStreamMsg{Subject: fmt.Sprintf("wf.sig.test.prefix-%d.go", seq), Sequence: seq}, nil
}
func (p signalPrefixPort) LastSignalSequence(context.Context) (uint64, error) {
	return 3, nats.ErrTimeout
}
func (p signalPrefixPort) ReadJournal(_ context.Context, _, id string) ([]journal.Record, error) {
	if id == "prefix-3" {
		if p.stage == "journal" {
			return nil, context.DeadlineExceeded
		}
		return nil, nil
	}
	return []journal.Record{{Entry: journal.Entry{Kind: journal.Completed}}}, nil
}
func (p signalPrefixPort) LastInvocation(context.Context, string) (*jetstream.RawStreamMsg, error) {
	if p.stage == "invocation" {
		return nil, nats.ErrTimeout
	}
	return &jetstream.RawStreamMsg{Sequence: 10}, nil
}
func (p signalPrefixPort) EnqueueSignal(context.Context, string, string, uint64) error {
	return nats.ErrTimeout
}

func TestSignalPartialCursorDoesNotSkipUnconfirmedSignal(t *testing.T) {
	for _, stage := range []string{"first_read", "read", "hole_info", "journal", "invocation", "enqueue"} {
		t.Run(stage, func(t *testing.T) {
			result, err := NewSignalScanWithPort(signalPrefixPort{stage}).Scan(context.Background(), 1, 500, false)
			if err == nil {
				t.Fatal("injected failure not observed")
			}
			want := uint64(3)
			if stage == "first_read" {
				want = 0
			}
			if result.RetrySequence != want {
				t.Fatalf("retry=%d want%d result=%+v", result.RetrySequence, want, result)
			}
		})
	}
}
