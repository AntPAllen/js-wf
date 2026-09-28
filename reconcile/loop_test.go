package reconcile

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"js-wf/provision"
	"js-wf/testcluster"

	"github.com/nats-io/nats.go/jetstream"
)

func TestCursorPersistsAcrossLeadersAndUsesCAS(t *testing.T) {
	cluster, err := testcluster.Start(t.TempDir(), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	a, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	b, err := jetstream.New(cluster.Clients[1])
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for ctx.Err() == nil {
		attempt, done := context.WithTimeout(ctx, 2*time.Second)
		_, err = a.AccountInfo(attempt)
		done()
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		attempt, done = context.WithTimeout(ctx, 3*time.Second)
		err = provision.Ensure(attempt, a, 3)
		done()
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	state, err := a.KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	firstCtx, stopFirst := context.WithCancel(ctx)
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- runLoop(firstCtx, a, "first", "start", 500*time.Millisecond, 1, func(_ context.Context, next uint64, _ int, _ bool) (ScanResult, error) {
			return ScanResult{NextSequence: next + 1, Inspected: 1}, nil
		})
	}()
	var saved uint64
	for ctx.Err() == nil {
		entry, err := state.Get(ctx, "scan.start")
		if err == nil {
			saved, err = strconv.ParseUint(string(entry.Value()), 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			break
		}
		if !errors.Is(err, jetstream.ErrKeyNotFound) {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if saved < 2 {
		t.Fatalf("cursor not persisted: %d", saved)
	}
	stopFirst()
	if err := <-firstDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	// Read the final cursor after the first leader has fully released its lease.
	saved, _, err = loadCursor(ctx, state, "start")
	if err != nil {
		t.Fatal(err)
	}
	secondCtx, stopSecond := context.WithCancel(ctx)
	observed := make(chan uint64, 1)
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- runLoop(secondCtx, b, "second", "start", 500*time.Millisecond, 1, func(_ context.Context, next uint64, _ int, _ bool) (ScanResult, error) {
			select {
			case observed <- next:
			default:
			}
			return ScanResult{NextSequence: next + 1, Inspected: 1}, nil
		})
	}()
	select {
	case next := <-observed:
		if next != saved {
			t.Fatalf("replacement restarted at %d, want %d", next, saved)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	stopSecond()
	if err := <-secondDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	_, revision, err := loadCursor(ctx, state, "start")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := saveCursor(ctx, state, "start", saved+10, revision); err != nil {
		t.Fatal(err)
	}
	if _, err := saveCursor(ctx, state, "start", saved+11, revision); !errors.Is(err, ErrCursorStale) {
		t.Fatalf("stale cursor update: %v", err)
	}
}
