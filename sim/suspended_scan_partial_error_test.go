package sim

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/identity"
	"js-wf/reconcile"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type oneReadTimeout struct {
	reconcile.SuspendedScanPort
	sequence uint64
	failed   atomic.Bool
}

func (p *oneReadTimeout) GetInvocation(ctx context.Context, sequence uint64) (*jetstream.RawStreamMsg, error) {
	if sequence == p.sequence && !p.failed.Swap(true) {
		return nil, nats.ErrTimeout
	}
	return p.SuspendedScanPort.GetInvocation(ctx, sequence)
}

func TestSuspendedScanKeepsVerifiedWakeupsOnTransientRead(t *testing.T) {
	for faultSequence := uint64(1); faultSequence <= 3; faultSequence++ {
		t.Run(fmt.Sprintf("sequence-%d", faultSequence), func(t *testing.T) {
			ctx := context.Background()
			model := NewSignalTransport(NewScheduler(int64(faultSequence)))
			now := time.Unix(1_700_000_000, 0).UTC()
			for sequence := uint64(1); sequence <= 3; sequence++ {
				id := fmt.Sprintf("ready-%d", sequence)
				got, err := model.PublishInvocation(ctx, &nats.Msg{Subject: identity.InvocationSubject("test", id)})
				if err != nil || got != sequence {
					t.Fatalf("invocation %d: sequence=%d err=%v", sequence, got, err)
				}
				records, err := suspendedFixture(sequence, "due_timer", now.Add(-2*time.Second), 0)
				if err != nil {
					t.Fatal(err)
				}
				model.SetJournal("test", id, records)
			}
			port := &oneReadTimeout{SuspendedScanPort: model, sequence: faultSequence}
			scan := reconcile.NewSuspendedScanWithPort(port)
			scan.Now = func() time.Time { return now }
			first, err := scan.Scan(ctx, 1, 3, false)
			if !errors.Is(err, nats.ErrTimeout) || first.NextSequence != 1 || first.Reenqueued != 2 || len(model.Runs()) != 2 {
				t.Fatalf("partial scan=%+v runs=%d err=%v", first, len(model.Runs()), err)
			}
			for _, run := range model.Runs() {
				if string(run.Data) == identity.Key("test", fmt.Sprintf("ready-%d", faultSequence)) {
					t.Fatalf("failed read was enqueued: %+v", run)
				}
			}
			second, err := scan.Scan(ctx, first.NextSequence, 3, false)
			if err != nil || second.Reenqueued != 3 || len(model.Runs()) != 3 {
				t.Fatalf("retry scan=%+v runs=%d err=%v", second, len(model.Runs()), err)
			}
			if last := model.Runs()[2]; string(last.Data) != identity.Key("test", fmt.Sprintf("ready-%d", faultSequence)) {
				t.Fatalf("retry did not recover failed read: %+v", last)
			}
		})
	}
}
