package integrity

import (
	"context"
	"errors"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"sync"
	"testing"
	"time"
)

type byteTestIterator struct {
	jetstream.MessagesContext
	stop      chan struct{}
	once      sync.Once
	remaining int
	failure   error
}

func (i *byteTestIterator) Stop() { i.once.Do(func() { close(i.stop) }) }
func (i *byteTestIterator) Next(...jetstream.NextOpt) (jetstream.Msg, error) {
	if i.remaining > 0 {
		i.remaining--
		return candidateControlMsg{seq: uint64(i.remaining + 1), stream: "CONTROL"}, nil
	}
	if i.failure != nil {
		return nil, i.failure
	}
	<-i.stop
	return nil, jetstream.ErrMsgIteratorClosed
}

type byteTestConsumer struct {
	jetstream.Consumer
	iterator *byteTestIterator
	creates  int
}

func (c *byteTestConsumer) Messages(...jetstream.PullMessagesOpt) (jetstream.MessagesContext, error) {
	c.creates++
	return c.iterator, nil
}
func TestByteBoundedFetchStopsAtRecordLimit(t *testing.T) {
	c := &byteTestConsumer{iterator: &byteTestIterator{stop: make(chan struct{}), remaining: 10}}
	b, err := fetchByteBounded(context.Background(), c, 3, 1024)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for range b.Messages() {
		count++
	}
	if b.Error() != nil || count != 3 || c.iterator.remaining != 7 {
		t.Fatalf("count=%d remaining=%d err=%v", count, c.iterator.remaining, b.Error())
	}
	select {
	case <-c.iterator.stop:
	default:
		t.Fatal("iterator not stopped")
	}
}
func TestByteBoundedFetchCancellationStopsBlockedIterator(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	c := &byteTestConsumer{iterator: &byteTestIterator{stop: make(chan struct{})}}
	b, err := fetchByteBounded(ctx, c, 5, 1024)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case _, ok := <-b.Messages():
		if ok {
			t.Fatal("message after cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("blocked iterator leaked")
	}
	if !errors.Is(b.Error(), context.Canceled) {
		t.Fatal(b.Error())
	}
}
func TestByteBoundedFetchRetainsSemanticErrorAndRejectsInvalidWindow(t *testing.T) {
	sentinel := errors.New("invalid retained data")
	c := &byteTestConsumer{iterator: &byteTestIterator{stop: make(chan struct{}), remaining: 1, failure: sentinel}}
	b, err := fetchByteBounded(context.Background(), c, 3, 1024)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for range b.Messages() {
		count++
	}
	if count != 1 || !errors.Is(b.Error(), sentinel) {
		t.Fatalf("count=%d err=%v", count, b.Error())
	}
	for _, n := range []int{0, 4097} {
		if _, err := fetchByteBounded(context.Background(), c, n, 1024); err == nil {
			t.Fatal("invalid window accepted")
		}
	}
}

func TestByteBoundedFetchHeartbeatAdmitsTransportRecovery(t *testing.T) {
	c := &byteTestConsumer{iterator: &byteTestIterator{stop: make(chan struct{}), failure: jetstream.ErrNoHeartbeat}}
	b, err := fetchByteBounded(context.Background(), c, 3, 1024)
	if err != nil {
		t.Fatal(err)
	}
	for range b.Messages() {
		t.Fatal("unexpected message")
	}
	if !errors.Is(b.Error(), jetstream.ErrNoHeartbeat) || !errors.Is(b.Error(), nats.ErrTimeout) || !batchReadTransportError(b.Error()) {
		t.Fatalf("heartbeat identity/recovery lost: %v", b.Error())
	}
}

func TestByteIteratorBatchRetainsPrefetchAcrossRecordWindows(t *testing.T) {
	iterator := &byteTestIterator{stop: make(chan struct{}), remaining: 7}
	first, cancel := context.WithCancel(context.Background())
	batch := byteIteratorBatch(first, iterator, 3, false)
	count := 0
	for range batch.Messages() {
		count++
	}
	if batch.Error() != nil || count != 3 || iterator.remaining != 4 {
		t.Fatalf("first count=%d remaining=%d err=%v", count, iterator.remaining, batch.Error())
	}
	cancel() // A finished batch must unregister its cancellation hook.
	select {
	case <-iterator.stop:
		t.Fatal("record boundary discarded prefetched iterator")
	default:
	}
	batch = byteIteratorBatch(context.Background(), iterator, 4, false)
	count = 0
	for range batch.Messages() {
		count++
	}
	if batch.Error() != nil || count != 4 || iterator.remaining != 0 {
		t.Fatalf("second count=%d remaining=%d err=%v", count, iterator.remaining, batch.Error())
	}
	iterator.Stop()
}

type byteLeaderInfoConsumer struct {
	jetstream.Consumer
	info  *jetstream.ConsumerInfo
	err   error
	calls int
}

func (c *byteLeaderInfoConsumer) Info(context.Context) (*jetstream.ConsumerInfo, error) {
	c.calls++
	return c.info, c.err
}
func TestByteReplayRequiresConfirmedReplicatedConsumerLeaderMove(t *testing.T) {
	for _, kind := range []string{"moved", "same-leader", "unknown-initial", "unknown-current", "other-name", "other-stream", "disk-consumer", "single-replica", "explicit-ack", "api-error"} {
		t.Run(kind, func(t *testing.T) {
			c := &byteLeaderInfoConsumer{info: &jetstream.ConsumerInfo{Name: "audit", Stream: "WF_JRN", Config: jetstream.ConsumerConfig{MemoryStorage: true, Replicas: 3, AckPolicy: jetstream.AckNonePolicy}, Cluster: &jetstream.ClusterInfo{Leader: "b"}}}
			initial := "a"
			switch kind {
			case "same-leader":
				c.info.Cluster.Leader = "a"
			case "unknown-initial":
				initial = ""
			case "unknown-current":
				c.info.Cluster.Leader = ""
			case "other-name":
				c.info.Name = "another"
			case "other-stream":
				c.info.Stream = "WF_INV"
			case "disk-consumer":
				c.info.Config.MemoryStorage = false
			case "single-replica":
				c.info.Config.Replicas = 1
			case "explicit-ack":
				c.info.Config.AckPolicy = jetstream.AckExplicitPolicy
			case "api-error":
				c.err = errors.New("semantic consumer info error")
			}
			confirmed, err := confirmedByteConsumerLeaderMove(context.Background(), c, "audit", "WF_JRN", initial)
			if confirmed != (kind == "moved") || err != c.err {
				t.Fatalf("confirmed=%v err=%v", confirmed, err)
			}
			if kind == "unknown-initial" && c.calls != 0 {
				t.Fatal("queried without initial leader proof")
			}
		})
	}
}
func TestByteReplayRecoveryKeepsUnconfirmedOrderSemantic(t *testing.T) {
	for _, confirmed := range []bool{false, true} {
		stream := &candidateControlStream{consumer: candidateControlConsumer{sequences: []uint64{1, 2, 1, 3, 4}, stream: "CONTROL"}, last: 4}
		var visited []uint64
		checks := 0
		err := scanBatchThroughWithReplayCheck(context.Background(), stream, nil, func(m *jetstream.RawStreamMsg) error { visited = append(visited, m.Sequence); return nil }, 4096, nil, func(context.Context, jetstream.Consumer) (bool, error) { checks++; return confirmed, nil })
		if checks != 1 {
			t.Fatalf("checks=%d", checks)
		}
		if confirmed {
			if err != nil || len(visited) != 4 || visited[0] != 1 || visited[1] != 2 || visited[2] != 3 || visited[3] != 4 || len(stream.createNames) != 2 {
				t.Fatalf("visited=%v cursors=%v err=%v", visited, stream.createNames, err)
			}
		} else if err == nil || len(stream.createNames) != 1 {
			t.Fatalf("unconfirmed order recovered: %v %v", stream.createNames, err)
		}
	}
}
