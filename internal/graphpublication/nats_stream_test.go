package graphpublication

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type heldStreamInfo struct {
	jetstream.Stream
	entered, release chan struct{}
	gets             int
}

func (s *heldStreamInfo) Info(context.Context, ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
	close(s.entered)
	<-s.release
	return &jetstream.StreamInfo{}, nil
}
func (s *heldStreamInfo) GetLastMsgForSubject(context.Context, string) (*jetstream.RawStreamMsg, error) {
	s.gets++
	return nil, jetstream.ErrMsgNotFound
}
func TestNativeGraphStreamGuardHonorsWaitingContext(t *testing.T) {
	raw := &heldStreamInfo{entered: make(chan struct{}), release: make(chan struct{})}
	stream := guardStream(raw)
	ownerDone := make(chan error, 1)
	go func() { _, err := stream.Info(context.Background()); ownerDone <- err }()
	<-raw.entered
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := stream.GetLastMsgForSubject(canceled, "scope"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	waiter, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stop()
	started := time.Now()
	if _, err := stream.GetLastMsgForSubject(waiter, "scope"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("waiting context ignored", err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("waiter blocked behind owner")
	}
	close(raw.release)
	if err := <-ownerDone; err != nil {
		t.Fatal(err)
	}
	if raw.gets != 0 {
		t.Fatal("expired waiter entered SDK")
	}
	if _, err := stream.GetLastMsgForSubject(context.Background(), "scope"); !errors.Is(err, jetstream.ErrMsgNotFound) || raw.gets != 1 {
		t.Fatal("handle not released", err, raw.gets)
	}
}
