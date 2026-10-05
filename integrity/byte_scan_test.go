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
