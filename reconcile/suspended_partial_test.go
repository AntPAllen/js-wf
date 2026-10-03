package reconcile

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/journal"
)

type suspendedPrefixPort struct {
	readError, journalError, retirementError, enqueueError, extraReady, hole uint64
	enqueues                                                                 []uint64
}

func (p *suspendedPrefixPort) LastInvocationSequence(context.Context) (uint64, error) { return 5, nil }
func (p *suspendedPrefixPort) GetInvocation(_ context.Context, seq uint64) (*jetstream.RawStreamMsg, error) {
	if seq == p.readError {
		return nil, nats.ErrTimeout
	}
	if seq == p.hole {
		return nil, jetstream.ErrMsgNotFound
	}
	return &jetstream.RawStreamMsg{Subject: fmt.Sprintf("wf.inv.test.prefix-%d", seq), Sequence: seq}, nil
}
func (p *suspendedPrefixPort) ReadJournal(_ context.Context, _, id string) ([]journal.Record, error) {
	seq, _ := strconv.ParseUint(strings.TrimPrefix(id, "prefix-"), 10, 64)
	if seq == p.journalError {
		return nil, context.DeadlineExceeded
	}
	if seq == 5 || seq == p.extraReady {
		return []journal.Record{{Entry: journal.Entry{Kind: journal.Suspended, Payload: []byte(`{"waiting_on":"continuation:next"}`)}, Sequence: seq}}, nil
	}
	return []journal.Record{{Entry: journal.Entry{Kind: journal.Completed}}}, nil
}
func (p *suspendedPrefixPort) GetSignalAfter(context.Context, string, uint64) (*jetstream.RawStreamMsg, error) {
	return nil, jetstream.ErrMsgNotFound
}
func (p *suspendedPrefixPort) EnqueueSuspended(_ context.Context, _, _ string, seq uint64, _ int64) error {
	p.enqueues = append(p.enqueues, seq)
	if seq == p.enqueueError {
		return nats.ErrTimeout
	}
	return nil
}
func (p *suspendedPrefixPort) NativeTimerSubjects(_ context.Context, _, id string) ([]string, error) {
	if id == fmt.Sprintf("prefix-%d", p.retirementError) {
		return nil, nats.ErrTimeout
	}
	return nil, nil
}
func (p *suspendedPrefixPort) LastNativeTimer(context.Context, string) (*jetstream.RawStreamMsg, error) {
	panic("no subjects")
}
func (p *suspendedPrefixPort) DeleteNativeTimer(context.Context, uint64) error { panic("no subjects") }

func TestSuspendedPartialCursorCertifiesOnlyContiguousAcknowledgedPrefix(t *testing.T) {
	for _, tc := range []struct {
		name     string
		port     suspendedPrefixPort
		dry      bool
		want     uint64
		enqueues []uint64
	}{
		{"first_read", suspendedPrefixPort{readError: 1}, false, 0, []uint64{5}},
		{"read", suspendedPrefixPort{readError: 3}, false, 3, []uint64{5}},
		{"journal", suspendedPrefixPort{journalError: 3}, false, 3, []uint64{5}},
		{"retirement", suspendedPrefixPort{retirementError: 3}, false, 3, []uint64{5}},
		{"enqueue", suspendedPrefixPort{enqueueError: 5}, false, 5, []uint64{5}},
		{"earlier_uncertain_enqueue", suspendedPrefixPort{extraReady: 2, enqueueError: 2, readError: 3}, false, 2, []uint64{2}},
		{"later_uncertain_enqueue", suspendedPrefixPort{enqueueError: 5, readError: 3}, false, 3, []uint64{5}},
		{"acknowledged_prefix_and_later_wakeup", suspendedPrefixPort{extraReady: 2, readError: 3}, false, 3, []uint64{2, 5}},
		{"proven_hole", suspendedPrefixPort{hole: 3, readError: 4}, false, 4, []uint64{5}},
		{"dry_read", suspendedPrefixPort{readError: 3}, true, 3, nil},
		{"success_wrap", suspendedPrefixPort{}, false, 0, []uint64{5}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := NewSuspendedScanWithPort(&tc.port).Scan(context.Background(), 1, 500, tc.dry)
			if (err == nil) != (tc.name == "success_wrap") {
				t.Fatalf("unexpected error:%v", err)
			}
			if result.RetrySequence != tc.want {
				t.Fatalf("retry=%d want%d result=%+v", result.RetrySequence, tc.want, result)
			}
			if !reflect.DeepEqual(tc.port.enqueues, tc.enqueues) {
				t.Fatalf("wakeups=%v want%v", tc.port.enqueues, tc.enqueues)
			}
			if err == nil && result.NextSequence != 1 {
				t.Fatalf("successful wrap changed:%+v", result)
			}
		})
	}
}
