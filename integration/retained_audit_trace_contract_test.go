//go:build linux

package integration_test

import (
	"context"
	"errors"
	"reflect"
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

type auditTraceBulkControlStream struct {
	jetstream.Stream
	expected context.Context
	cfg      jetstream.ConsumerConfig
	consumer jetstream.Consumer
}

func (s auditTraceBulkControlStream) CreateConsumer(ctx context.Context, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	if ctx != s.expected || cfg.Name != s.cfg.Name || cfg.Replicas != s.cfg.Replicas {
		return nil, errors.New("create delegation changed")
	}
	return s.consumer, nil
}
func (s auditTraceBulkControlStream) DeleteConsumer(ctx context.Context, name string) error {
	if ctx != s.expected || name != s.cfg.Name {
		return errors.New("delete delegation changed")
	}
	return auditTraceSentinel
}

type auditTraceBulkControlConsumer struct {
	jetstream.Consumer
	batch jetstream.MessageBatch
}

func (c auditTraceBulkControlConsumer) Fetch(batch int, opts ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	if batch != 17 || len(opts) != 1 || opts[0] != nil {
		return nil, errors.New("fetch delegation changed")
	}
	return c.batch, nil
}

type auditTraceBulkControlBatch struct{ messages chan jetstream.Msg }

func (b auditTraceBulkControlBatch) Messages() <-chan jetstream.Msg { return b.messages }
func (b auditTraceBulkControlBatch) Error() error                   { return auditTraceSentinel }
func TestRetainedAuditTraceBulkDelegatesChannelAndCompletesOnce(t *testing.T) {
	ctx := context.Background()
	trace := &retainedAuditTrace{}
	messages := make(chan jetstream.Msg)
	close(messages)
	cfg := jetstream.ConsumerConfig{Name: "audit-proof", Replicas: 3}
	stream := tracedAuditStream{Stream: auditTraceBulkControlStream{expected: ctx, cfg: cfg, consumer: auditTraceBulkControlConsumer{batch: auditTraceBulkControlBatch{messages: messages}}}, name: "WF_JRN", trace: trace}
	consumer, err := stream.CreateConsumer(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := consumer.Fetch(17, nil)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Messages() != messages {
		t.Fatal("message channel replaced")
	}
	if batch.Error() != auditTraceSentinel || batch.Error() != auditTraceSentinel {
		t.Fatal("batch error changed")
	}
	if err := stream.DeleteConsumer(ctx, cfg.Name); err != auditTraceSentinel {
		t.Fatal(err)
	}
	snapshot := trace.snapshot()
	if c := snapshot.Counts["WF_JRN.Fetch"]; c.Started != 1 || c.Completed != 1 || c.Errors != 1 || c.Bytes != 0 {
		t.Fatalf("fetch count=%+v", c)
	}
	if c := snapshot.Counts["WF_JRN.CreateConsumer"]; c.Started != 1 || c.Completed != 1 || c.Errors != 0 {
		t.Fatalf("create count=%+v", c)
	}
	if c := snapshot.Counts["WF_JRN.DeleteConsumer"]; c.Started != 1 || c.Completed != 1 || c.Errors != 1 {
		t.Fatalf("delete count=%+v", c)
	}
	for _, call := range snapshot.Recent {
		if call.Operation == "WF_JRN.Fetch" && !call.Deadline.IsZero() {
			t.Fatal("invented fetch deadline")
		}
	}
}

// Exercise the newly traced projection paths with exact context/config/result
// identity, including an error accompanying a nonnil SDK response.
type projectionTraceStreamControl struct {
	jetstream.Stream
	expected context.Context
	cfg      jetstream.OrderedConsumerConfig
	consumer jetstream.Consumer
	reset    *jetstream.ConsumerResetResponse
}

func (s projectionTraceStreamControl) OrderedConsumer(ctx context.Context, cfg jetstream.OrderedConsumerConfig) (jetstream.Consumer, error) {
	if ctx != s.expected || !reflect.DeepEqual(cfg, s.cfg) {
		return nil, errors.New("ordered consumer arguments changed")
	}
	return s.consumer, nil
}
func (s projectionTraceStreamControl) ResetConsumerToSequence(ctx context.Context, name string, seq uint64) (*jetstream.ConsumerResetResponse, error) {
	if ctx != s.expected || name != "WF_VIEW_PG" || seq != 100001 {
		return nil, errors.New("reset arguments changed")
	}
	return s.reset, auditTraceSentinel
}

type projectionTraceConsumerControl struct {
	jetstream.Consumer
	expected context.Context
	info     *jetstream.ConsumerInfo
}

func (c projectionTraceConsumerControl) Info(ctx context.Context) (*jetstream.ConsumerInfo, error) {
	if ctx != c.expected {
		return nil, errors.New("consumer info context changed")
	}
	return c.info, auditTraceSentinel
}
func TestRetainedAuditTraceProjectionConsumerDelegation(t *testing.T) {
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	trace := &retainedAuditTrace{}
	cfg := jetstream.OrderedConsumerConfig{DeliverPolicy: jetstream.DeliverAllPolicy, HeadersOnly: true, InactiveThreshold: time.Minute, FilterSubjects: []string{"wf.inv.*.*"}}
	info := &jetstream.ConsumerInfo{NumPending: 50000}
	reset := &jetstream.ConsumerResetResponse{}
	stream := tracedAuditStream{Stream: projectionTraceStreamControl{expected: ctx, cfg: cfg, consumer: projectionTraceConsumerControl{expected: ctx, info: info}, reset: reset}, name: "WF_INV", trace: trace}
	consumer, err := stream.OrderedConsumer(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := consumer.Info(ctx); got != info || err != auditTraceSentinel {
		t.Fatalf("info identity changed: %v %v", got, err)
	}
	if got, err := stream.ResetConsumerToSequence(ctx, "WF_VIEW_PG", 100001); got != reset || err != auditTraceSentinel {
		t.Fatalf("reset identity changed: %v %v", got, err)
	}
	for operation, errors := range map[string]int{"WF_INV.OrderedConsumer": 0, "WF_INV.ConsumerInfo": 1, "WF_INV.ResetConsumerToSequence": 1} {
		c := trace.snapshot().Counts[operation]
		if c.Started != 1 || c.Completed != 1 || c.Errors != errors {
			t.Fatalf("%s: %+v", operation, c)
		}
	}
}
