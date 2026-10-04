//go:build linux

package integration_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

var auditTraceSentinel = errors.New("semantic audit read failure")

type auditTraceControlStream struct {
	jetstream.Stream
	expected context.Context
}

func (s auditTraceControlStream) GetMsg(ctx context.Context, seq uint64, opts ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
	if ctx != s.expected || len(opts) != 1 || opts[0] != nil {
		return nil, errors.New("context or options changed")
	}
	if seq == 7 {
		return nil, auditTraceSentinel
	}
	if seq == 999 {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return &jetstream.RawStreamMsg{Sequence: seq, Data: []byte("abc")}, nil
}
func TestRetainedAuditTracePreservesDelegationAndBoundsConcurrentHistory(t *testing.T) {
	ctx := context.Background()
	trace := &retainedAuditTrace{}
	stream := tracedAuditStream{Stream: auditTraceControlStream{expected: ctx}, name: "WF_JRN", trace: trace}
	var group sync.WaitGroup
	for i := uint64(100); i < 300; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			msg, err := stream.GetMsg(ctx, i, nil)
			if err != nil || msg.Sequence != i {
				t.Errorf("seq=%d msg=%v err=%v", i, msg, err)
			}
		}()
	}
	group.Wait()
	if _, err := stream.GetMsg(ctx, 7, nil); err != auditTraceSentinel {
		t.Fatal(err)
	}
	snapshot := trace.snapshot()
	count := snapshot.Counts["WF_JRN.GetMsg"]
	if count.Started != 201 || count.Completed != 201 || count.Errors != 1 || count.Bytes != 600 || len(snapshot.Recent) != 64 || snapshot.Recent[63].Error != auditTraceSentinel.Error() {
		t.Fatalf("count=%+v recent=%d", count, len(snapshot.Recent))
	}
	snapshot.Counts["WF_JRN.GetMsg"] = retainedAuditCount{}
	snapshot.Recent[0].Operation = "mutated copy"
	again := trace.snapshot()
	if again.Counts["WF_JRN.GetMsg"].Completed != 201 || again.Recent[0].Operation == "mutated copy" {
		t.Fatal("snapshot aliases live state")
	}
}
func TestRetainedAuditTracePreservesDeadlineFailure(t *testing.T) {
	ctx, stop := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer stop()
	trace := &retainedAuditTrace{}
	stream := tracedAuditStream{Stream: auditTraceControlStream{expected: ctx}, name: "WF_JRN", trace: trace}
	_, err := stream.GetMsg(ctx, 999, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	snapshot := trace.snapshot()
	deadline, _ := ctx.Deadline()
	if len(snapshot.Recent) != 1 || snapshot.Recent[0].Deadline != deadline || snapshot.Counts["WF_JRN.GetMsg"].Errors != 1 {
		t.Fatalf("snapshot=%+v", snapshot)
	}
}
