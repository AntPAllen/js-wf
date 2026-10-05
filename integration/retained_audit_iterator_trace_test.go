//go:build linux

package integration_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/testcluster"
)

type traceIteratorMsg struct {
	jetstream.Msg
	payload []byte
}

func (m *traceIteratorMsg) Data() []byte { return m.payload }

type traceIteratorControl struct {
	jetstream.MessagesContext
	next             func(...jetstream.NextOpt) (jetstream.Msg, error)
	stopped, drained atomic.Int64
}

func (i *traceIteratorControl) Next(opts ...jetstream.NextOpt) (jetstream.Msg, error) {
	return i.next(opts...)
}
func (i *traceIteratorControl) Stop()  { i.stopped.Add(1) }
func (i *traceIteratorControl) Drain() { i.drained.Add(1) }

type traceIteratorConsumerControl struct {
	jetstream.Consumer
	iterator jetstream.MessagesContext
	failure  error
}

func (c traceIteratorConsumerControl) Messages(opts ...jetstream.PullMessagesOpt) (jetstream.MessagesContext, error) {
	if len(opts) != 3 || opts[0] != jetstream.PullMaxBytes(1024) || opts[1] != jetstream.StopAfter(3) || opts[2] != jetstream.PullExpiry(2*time.Second) {
		return nil, errors.New("iterator options changed")
	}
	return c.iterator, c.failure
}
func TestRetainedAuditTraceIteratorDelegatesAndMeasuresDelivery(t *testing.T) {
	trace := &retainedAuditTrace{}
	msg := &traceIteratorMsg{payload: []byte("abc")}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	option := jetstream.NextContext(ctx)
	control := &traceIteratorControl{next: func(opts ...jetstream.NextOpt) (jetstream.Msg, error) {
		if len(opts) != 1 || reflect.ValueOf(opts[0]).Pointer() != reflect.ValueOf(option).Pointer() {
			return nil, errors.New("next options changed")
		}
		return msg, nil
	}}
	consumer := tracedAuditConsumer{Consumer: traceIteratorConsumerControl{iterator: control}, name: "WF_JRN", consumer: "audit-one", trace: trace}
	iterator, err := consumer.Messages(jetstream.PullMaxBytes(1024), jetstream.StopAfter(3), jetstream.PullExpiry(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	for n := 0; n < 1000; n++ {
		got, err := iterator.Next(option)
		if got != msg || err != nil {
			t.Fatalf("identity: %v %v", got, err)
		}
	}
	control.next = func(...jetstream.NextOpt) (jetstream.Msg, error) { return nil, jetstream.ErrNoHeartbeat }
	if _, err := iterator.Next(option); err != jetstream.ErrNoHeartbeat {
		t.Fatal(err)
	}
	iterator.Stop()
	iterator.Drain()
	if control.stopped.Load() != 1 || control.drained.Load() != 1 {
		t.Fatal("lifecycle delegation changed")
	}
	snapshot := trace.snapshot()
	count := snapshot.Counts["WF_JRN.Next"]
	if count.Started != 1001 || count.Completed != 1001 || count.Errors != 1 || count.Bytes != 3000 || count.LastSuccess.IsZero() || count.LastCompleted.Before(count.LastSuccess) {
		t.Fatalf("count=%+v", count)
	}
	if len(snapshot.Recent) > 64 || snapshot.Recent[len(snapshot.Recent)-1].Error != jetstream.ErrNoHeartbeat.Error() {
		t.Fatalf("recent=%+v", snapshot.Recent)
	}
	for _, call := range snapshot.Recent {
		if !call.Deadline.IsZero() {
			t.Fatal("invented opaque-option deadline")
		}
	}
	consumer.Consumer = traceIteratorConsumerControl{failure: auditTraceSentinel}
	if got, err := consumer.Messages(jetstream.PullMaxBytes(1024), jetstream.StopAfter(3), jetstream.PullExpiry(2*time.Second)); got != nil || err != auditTraceSentinel {
		t.Fatalf("creation error changed: %v %v", got, err)
	}
}
func TestRetainedAuditTraceIteratorCapturesOutstandingWait(t *testing.T) {
	trace := &retainedAuditTrace{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{})
	done := make(chan error, 1)
	iterator := tracedAuditIterator{MessagesContext: &traceIteratorControl{next: func(...jetstream.NextOpt) (jetstream.Msg, error) { close(entered); <-ctx.Done(); return nil, ctx.Err() }}, name: "WF_JRN", trace: trace}
	go func() { _, err := iterator.Next(jetstream.NextContext(ctx)); done <- err }()
	<-entered
	if c := trace.snapshot().Counts["WF_JRN.Next"]; c.Started != 1 || c.Completed != 0 {
		t.Fatalf("pending=%+v", c)
	}
	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked trace call")
	}
	if c := trace.snapshot().Counts["WF_JRN.Next"]; c.Completed != 1 || c.Errors != 1 || !c.LastSuccess.IsZero() {
		t.Fatalf("canceled=%+v", c)
	}
}

func TestRetainedAuditTraceIteratorNativeRefillAndContext(t *testing.T) {
	base := os.Getenv("WF_AUDIT_ITERATOR_TRACE_ROOT")
	if base == "" {
		t.Skip("set WF_AUDIT_ITERATOR_TRACE_ROOT to preserve native iterator controls")
	}
	root := filepath.Join(base, t.Name())
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	cluster, err := testcluster.Start(root, 3)
	if err != nil {
		t.Fatal(err)
	}
	all, _ := setupCluster(t, cluster)
	js := all[0]
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "AUDIT_TRACE", Subjects: []string{"audit.trace"}, Storage: jetstream.FileStorage, Replicas: 3})
	if err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 256<<10)
	for n := 0; n < 48; n++ {
		if _, err := js.Publish(ctx, "audit.trace", payload); err != nil {
			t.Fatal(err)
		}
	}
	// Compare the production byte window with an unwrapped iterator on the
	// same committed payload. Twelve MiB forces buffer refill in both paths.
	plain, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{Name: "trace-plain", AckPolicy: jetstream.AckNonePolicy})
	if err != nil {
		t.Fatal(err)
	}
	original, err := plain.Messages(jetstream.PullMaxBytes(8<<20), jetstream.PullExpiry(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer original.Stop()
	for n := 0; n < 48; n++ {
		msg, err := original.Next(jetstream.NextContext(ctx))
		if err != nil || !bytes.Equal(msg.Data(), payload) {
			t.Fatalf("plain n=%d msg=%v err=%v", n, msg, err)
		}
	}
	trace := &retainedAuditTrace{}
	wrapped := tracedAuditStream{Stream: stream, name: "AUDIT_TRACE", trace: trace}
	consumer, err := wrapped.CreateConsumer(ctx, jetstream.ConsumerConfig{Name: "trace-reader", AckPolicy: jetstream.AckNonePolicy})
	if err != nil {
		t.Fatal(err)
	}
	iterator, err := consumer.Messages(jetstream.PullMaxBytes(8<<20), jetstream.PullExpiry(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer iterator.Stop()
	for n := 0; n < 48; n++ {
		msg, err := iterator.Next(jetstream.NextContext(ctx))
		if err != nil || !bytes.Equal(msg.Data(), payload) {
			t.Fatalf("n=%d msg=%v err=%v", n, msg, err)
		}
	}
	if c := trace.snapshot().Counts["AUDIT_TRACE.Next"]; c.Completed != 48 || c.Bytes != 48*(256<<10) || c.Errors != 0 {
		t.Fatalf("refill=%+v", c)
	}
	empty, err := wrapped.CreateConsumer(ctx, jetstream.ConsumerConfig{Name: "trace-empty", AckPolicy: jetstream.AckNonePolicy, DeliverPolicy: jetstream.DeliverNewPolicy})
	if err != nil {
		t.Fatal(err)
	}
	blocked, err := empty.Messages(jetstream.PullMaxBytes(1024))
	if err != nil {
		t.Fatal(err)
	}
	defer blocked.Stop()
	call, stop := context.WithTimeout(ctx, 25*time.Millisecond)
	defer stop()
	done := make(chan error, 1)
	go func() { _, err := blocked.Next(jetstream.NextContext(call)); done <- err }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		blocked.Stop()
		t.Fatal("NextContext deadline lost")
	}
	call, stop = context.WithCancel(ctx)
	stop()
	if _, err := blocked.Next(jetstream.NextContext(call)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	count := trace.snapshot().Counts["AUDIT_TRACE.Next"]
	if count.Started != 50 || count.Completed != 50 || count.Errors != 2 || count.Bytes != 48*(256<<10) {
		t.Fatalf("final=%+v", count)
	}
	t.Logf("records=48 bytes=12582912 next_calls=50 context_errors=2 max_wait=%s", time.Duration(count.MaxNS))
}
