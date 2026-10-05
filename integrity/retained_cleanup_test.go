package integrity

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type unavailableCleanupStream struct {
	jetstream.Stream
	deadStarted chan struct{}
	deleted     chan string
}

func (s unavailableCleanupStream) DeleteConsumer(ctx context.Context, name string) error {
	if name == "old-owner" {
		close(s.deadStarted)
		<-ctx.Done()
		return ctx.Err()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.deleted <- name:
		return nil
	}
}

func TestRetainedCleanupUnavailableOwnerDoesNotStarveReplacement(t *testing.T) {
	// Cancellation must still permit bounded cleanup; the original deadline
	// remains binding. No simulated server response for the unavailable owner.
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	cancel()
	s := unavailableCleanupStream{deadStarted: make(chan struct{}), deleted: make(chan string, 2)}
	done := make(chan struct{})
	go func() {
		cleanupRetainedConsumers(ctx, s, []string{"old-owner", "replacement-1", "replacement-2"})
		close(done)
	}()
	<-s.deadStarted
	observed := map[string]bool{}
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for len(observed) < 2 {
		select {
		case name := <-s.deleted:
			observed[name] = true
		case <-done:
			t.Fatalf("unavailable owner starved replacements: %v", observed)
		case <-timer.C:
			t.Fatal("cleanup ignored original deadline")
		}
	}
	select {
	case <-done:
	case <-timer.C:
		t.Fatal("unavailable deletion did not join within original deadline")
	}
	if !observed["replacement-1"] || !observed["replacement-2"] {
		t.Fatalf("deleted=%v", observed)
	}
}
