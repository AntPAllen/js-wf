package journal

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type cacheTestStream struct{ jetstream.Stream }
type cacheTestState struct{ jetstream.KeyValue }
type cacheTestObjects struct{ jetstream.ObjectStore }
type cacheMetadataJS struct {
	jetstream.JetStream
	blocked string
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (j *cacheMetadataJS) hold(ctx context.Context, name string) error {
	if name != j.blocked {
		return nil
	}
	j.once.Do(func() { close(j.entered) })
	select {
	case <-j.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (j *cacheMetadataJS) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	if err := j.hold(ctx, name); err != nil {
		return nil, err
	}
	return &cacheTestStream{}, nil
}
func (j *cacheMetadataJS) KeyValue(ctx context.Context, name string) (jetstream.KeyValue, error) {
	if err := j.hold(ctx, name); err != nil {
		return nil, err
	}
	return &cacheTestState{}, nil
}
func (j *cacheMetadataJS) ObjectStore(ctx context.Context, name string) (jetstream.ObjectStore, error) {
	if err := j.hold(ctx, name); err != nil {
		return nil, err
	}
	return &cacheTestObjects{}, nil
}

type cacheObservedContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *cacheObservedContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func TestMetadataHandleWaitersCancelAndSeparateResources(t *testing.T) {
	for _, mode := range []string{"journal_waiter", "state_waiter", "snapshot_state_waiter", "snapshot_object_waiter", "journal_state_independent", "snapshot_state_object_independent"} {
		t.Run(mode, func(t *testing.T) {
			js := &cacheMetadataJS{entered: make(chan struct{}), release: make(chan struct{})}
			store := New(js)
			snapshots := &jetStreamSnapshotReadPort{js: js}
			stream := func(ctx context.Context) error { _, err := store.journalStream(ctx); return err }
			state := func(ctx context.Context) error { _, err := store.stateKV(ctx); return err }
			snapshotState := func(ctx context.Context) error { _, err := snapshots.stateBucket(ctx); return err }
			objects := func(ctx context.Context) error { _, err := snapshots.objectStore(ctx); return err }
			lead, follow, independent := stream, stream, false
			switch mode {
			case "journal_waiter":
				js.blocked = "WF_JRN"
			case "state_waiter":
				js.blocked = "WF_STATE"
				lead, follow = state, state
			case "snapshot_state_waiter":
				js.blocked = "WF_STATE"
				lead, follow = snapshotState, snapshotState
			case "snapshot_object_waiter":
				js.blocked = "WF_BLOB"
				lead, follow = objects, objects
			case "journal_state_independent":
				js.blocked = "WF_JRN"
				follow = state
				independent = true
			case "snapshot_state_object_independent":
				js.blocked = "WF_BLOB"
				lead, follow = objects, snapshotState
				independent = true
			}
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(js.release) }) }
			defer release()
			leader := make(chan error, 1)
			go func() { leader <- lead(context.Background()) }()
			select {
			case <-js.entered:
			case <-time.After(time.Second):
				t.Fatal("leader did not enter metadata request")
			}
			base, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &cacheObservedContext{Context: base, waiting: make(chan struct{})}
			follower := make(chan error, 1)
			go func() { follower <- follow(ctx) }()
			if independent {
				select {
				case err := <-follower:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(time.Second):
					t.Fatal("unrelated metadata handle blocked behind network lookup")
				}
			} else {
				select {
				case <-ctx.waiting:
				case <-time.After(time.Second):
					t.Fatal("metadata waiter did not reach cancellable wait")
				}
				cancel()
				select {
				case err := <-follower:
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("waiter cancellation: %v", err)
					}
				case <-time.After(time.Second):
					t.Fatal("canceled metadata reader remained blocked")
				}
			}
			release()
			select {
			case err := <-leader:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("released lookup did not complete")
			}
		})
	}
}
