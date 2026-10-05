package integrity

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type concurrentAuditStream struct {
	jetstream.Stream
	name string
}

type concurrentAuditState struct {
	auditWatchState
	started chan struct{}
}

func (s concurrentAuditState) WatchAll(context.Context, ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
	close(s.started)
	return s.watch, nil
}

type concurrentAuditJS struct {
	jetstream.JetStream
	state jetstream.KeyValue
}

func (j concurrentAuditJS) Stream(_ context.Context, name string) (jetstream.Stream, error) {
	return concurrentAuditStream{name: name}, nil
}
func (j concurrentAuditJS) KeyValue(context.Context, string) (jetstream.KeyValue, error) {
	return j.state, nil
}

func TestConcurrentStateStartsDuringJournalAndJoinsOnFailure(t *testing.T) {
	started := make(chan struct{})
	watch := &auditWatch{updates: make(chan jetstream.KeyValueEntry)}
	js := concurrentAuditJS{state: concurrentAuditState{auditWatchState: auditWatchState{watch: watch}, started: started}}
	failure := errors.New("injected journal read failure")
	read := func(ctx context.Context, stream jetstream.Stream, _ *uint64, _ func(*jetstream.RawStreamMsg) error) error {
		if stream.(concurrentAuditStream).name == "WF_INV" {
			return nil
		}
		select {
		case <-started:
			return failure
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := checkUsingConcurrentOptions(ctx, js, nil, read, true, true, true)
	if !errors.Is(err, failure) {
		t.Fatalf("journal did not overlap state watch: %v", err)
	}
	if !watch.stopped {
		t.Fatal("returned before snapshot watch cleanup")
	}
}
