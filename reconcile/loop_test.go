package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/lease"
	"js-wf/provision"
	"js-wf/retention"
	"js-wf/testcluster"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type hiddenCursorAckKV struct {
	jetstream.KeyValue
	key       string
	operation string
}

func (h hiddenCursorAckKV) Create(ctx context.Context, key string, data []byte, opts ...jetstream.KVCreateOpt) (uint64, error) {
	revision, err := h.KeyValue.Create(ctx, key, data, opts...)
	if err == nil && h.key == key && h.operation == "create" {
		return 0, nats.ErrTimeout
	}
	return revision, err
}

func (h hiddenCursorAckKV) Update(ctx context.Context, key string, data []byte, revision uint64) (uint64, error) {
	next, err := h.KeyValue.Update(ctx, key, data, revision)
	if err == nil && h.key == key && h.operation == "update" {
		return 0, nats.ErrTimeout
	}
	return next, err
}

func TestReconcileRetriesLostLeaseInitialization(t *testing.T) {
	err := fmt.Errorf("%w: initialization: context deadline exceeded", lease.ErrLost)
	if !retryableReconcileError(err) {
		t.Fatalf("lost lease initialization should retry: %v", err)
	}
	if retryableReconcileError(errors.New("invalid scan cursor")) {
		t.Fatal("permanent cursor error should stop the loop")
	}
}

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
	var scans atomic.Int32
	retryCtx, stopRetry := context.WithCancel(ctx)
	retryDone := make(chan error, 1)
	observedRetry := make(chan struct{}, 1)
	go func() {
		retryDone <- runLoop(retryCtx, a, "retry", "timer", 50*time.Millisecond, 1, func(_ context.Context, next uint64, _ int, _ bool) (ScanResult, error) {
			if scans.Add(1) == 1 {
				return ScanResult{}, jetstream.ErrNoStreamResponse
			}
			select {
			case observedRetry <- struct{}{}:
			default:
			}
			return ScanResult{NextSequence: next + 1}, nil
		})
	}()
	select {
	case <-observedRetry:
	case err := <-retryDone:
		t.Fatalf("reconciler exited on transient stream error: %v", err)
	case <-ctx.Done():
		t.Fatal("reconciler did not retry transient stream error")
	}
	stopRetry()
	if err := <-retryDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestCursorLostAcknowledgmentRereadsRealKV(t *testing.T) {
	cluster, err := testcluster.Start(t.TempDir(), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	var all [2]jetstream.JetStream
	for i := range all {
		all[i], err = jetstream.New(cluster.Clients[i])
		if err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, 3*time.Second)
		err = provision.Ensure(attempt, all[0], 3)
		stop()
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	writer, err := all[0].KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	reader, err := all[1].KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []struct {
		kind      string
		operation string
	}{
		{kind: "lost-create-ack", operation: "create"},
		{kind: "lost-update-ack", operation: "update"},
	} {
		t.Run(scenario.operation, func(t *testing.T) {
			var revision uint64
			if scenario.operation == "update" {
				var err error
				revision, err = saveCursor(ctx, writer, scenario.kind, 4, 0)
				if err != nil {
					t.Fatal(err)
				}
			}
			hidden := hiddenCursorAckKV{KeyValue: writer, key: "scan." + scenario.kind, operation: scenario.operation}
			if _, err := saveCursor(ctx, hidden, scenario.kind, 5, revision); !errors.Is(err, nats.ErrTimeout) || !retryableReconcileError(err) {
				t.Fatalf("committed cursor acknowledgment hidden: %v", err)
			}
			var saved, newRevision uint64
			for ctx.Err() == nil {
				var readErr error
				saved, newRevision, readErr = loadCursor(ctx, reader, scenario.kind)
				if readErr == nil && saved == 5 && newRevision > revision {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			if saved != 5 || newRevision <= revision {
				t.Fatalf("replacement cursor=%d revision=%d previous=%d ctx=%v", saved, newRevision, revision, ctx.Err())
			}
			if _, err := saveCursor(ctx, reader, scenario.kind, 6, newRevision); err != nil {
				t.Fatalf("replacement cursor advance: %v", err)
			}
			if _, err := saveCursor(ctx, writer, scenario.kind, 7, revision); !errors.Is(err, ErrCursorStale) {
				t.Fatalf("old cursor revision must be stale: %v", err)
			}
		})
	}
}

func TestTombstoneLoopReclaimsExpiredState(t *testing.T) {
	cluster, err := testcluster.Start(t.TempDir(), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for ctx.Err() == nil {
		attempt, done := context.WithTimeout(ctx, 3*time.Second)
		err = provision.Ensure(attempt, js, 3)
		done()
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	state, err := js.KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	marker, _ := json.Marshal(retention.Tombstone{Tombstone: true, InvSeq: 1, PurgedAt: now.Add(-time.Hour), ExpiresAt: now.Add(-time.Minute)})
	if _, err := state.Put(ctx, "test.expired", marker); err != nil {
		t.Fatal(err)
	}
	if err := RunTombstoneLoop(ctx, js, "invalid-budget", time.Second, 1); err == nil {
		t.Fatal("accepted a one-sequence tombstone loop budget")
	}
	loopCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- RunTombstoneLoop(loopCtx, js, "tombstone-test", 50*time.Millisecond, 4) }()
	for ctx.Err() == nil {
		_, err := state.Get(ctx, "test.expired")
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		select {
		case loopErr := <-done:
			t.Fatalf("tombstone loop exited before reclaiming: %v", loopErr)
		case <-time.After(20 * time.Millisecond):
		}
	}
	if ctx.Err() != nil {
		t.Fatal("tombstone loop did not reclaim expired state")
	}
	for ctx.Err() == nil {
		_, err := state.Get(ctx, "scan.tombstone")
		if err == nil {
			break
		}
		if !errors.Is(err, jetstream.ErrKeyNotFound) {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("tombstone cursor was not persisted")
	}
	stop()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
