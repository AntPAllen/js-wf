package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/reconcile"
	"js-wf/retention"

	"github.com/nats-io/nats.go/jetstream"
)

// WF_TOMBSTONE_SWEEP_SCALE=1 proves bounded scheduled cleanup of 100,000
// expired tombstones. WF_TOMBSTONE_SWEEP_COUNT lowers the diagnostic size.
func TestHundredThousandTombstonesSweptByLeaderLoop(t *testing.T) {
	if os.Getenv("WF_TOMBSTONE_SWEEP_SCALE") == "" {
		t.Skip("set WF_TOMBSTONE_SWEEP_SCALE=1 for the 100,000-tombstone proof")
	}
	count := 100000
	if raw := os.Getenv("WF_TOMBSTONE_SWEEP_COUNT"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 100 || parsed > count {
			t.Fatalf("invalid WF_TOMBSTONE_SWEEP_COUNT %q", raw)
		}
		count = parsed
	}
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	state := make([]jetstream.KeyValue, len(all))
	for index, js := range all {
		var err error
		state[index], err = js.KeyValue(ctx, "WF_STATE")
		if err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now().UTC()
	marker, _ := json.Marshal(retention.Tombstone{Tombstone: true, InvSeq: 1, PurgedAt: now.Add(-time.Hour), ExpiresAt: now.Add(-time.Minute)})
	jobs := make(chan int, 256)
	var workers sync.WaitGroup
	var written atomic.Int64
	var firstErr error
	var failOnce sync.Once
	start := time.Now()
	for workerID := 0; workerID < 96; workerID++ {
		workers.Add(1)
		go func(workerID int) {
			defer workers.Done()
			kv := state[workerID%len(state)]
			for index := range jobs {
				key := fmt.Sprintf("tombscale.%06d", index)
				if _, err := kv.Put(ctx, key, marker); err != nil {
					failOnce.Do(func() { firstErr = fmt.Errorf("put %s: %w", key, err); cancel() })
					return
				}
				if done := written.Add(1); done%10000 == 0 {
					t.Logf("stored %d/%d expired tombstones in %s", done, count, time.Since(start))
				}
			}
		}(workerID)
	}
produce:
	for index := 0; index < count; index++ {
		select {
		case jobs <- index:
		case <-ctx.Done():
			break produce
		}
	}
	close(jobs)
	workers.Wait()
	if firstErr != nil || written.Load() != int64(count) {
		t.Fatalf("tombstones stored=%d/%d error=%v context=%v", written.Load(), count, firstErr, ctx.Err())
	}
	countLive := func(kv jetstream.KeyValue) (int, error) {
		keys, err := kv.Keys(ctx)
		if err != nil && !errors.Is(err, jetstream.ErrNoKeysFound) {
			return 0, err
		}
		remaining := 0
		for _, key := range keys {
			if strings.HasPrefix(key, "tombscale.") {
				remaining++
			}
		}
		return remaining, nil
	}
	for ctx.Err() == nil {
		visible, err := countLive(state[1])
		if err != nil {
			t.Fatal(err)
		}
		if visible == count {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("tombstone fixture did not become visible")
	}
	loopCtx, stopLoop := context.WithCancel(ctx)
	loopDone := make(chan error, 1)
	go func() {
		loopDone <- reconcile.RunTombstoneLoop(loopCtx, all[0], "tombstone-scale", 10*time.Millisecond, 256)
	}()
	loopFinished := false
	defer func() {
		stopLoop()
		if !loopFinished {
			if err := <-loopDone; err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("tombstone loop: %v", err)
			}
		}
	}()
	lastRemaining := count
	for ctx.Err() == nil {
		remaining, err := countLive(state[1])
		if err != nil {
			t.Fatal(err)
		}
		if remaining == 0 {
			break
		}
		lastRemaining = remaining
		select {
		case loopErr := <-loopDone:
			loopFinished = true
			t.Fatalf("tombstone loop stopped with %d remaining: %v", remaining, loopErr)
		case <-time.After(time.Second):
		}
	}
	if ctx.Err() != nil {
		t.Fatalf("tombstone sweep stopped with %d remaining: %v", lastRemaining, ctx.Err())
	}
	if remaining, err := countLive(state[2]); err != nil || remaining != 0 {
		t.Fatalf("surviving node retains %d tombstones: %v", remaining, err)
	}
	// KV key absence is logical deletion: History=1 retains a DEL revision.
	// Keep the production loop running until a later page removes the physical
	// marker subjects. A subject-filtered stream census observes retained records.
	stream, err := all[0].Stream(ctx, "KV_WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	var physical uint64
	for ctx.Err() == nil {
		info, err := stream.Info(ctx, jetstream.WithSubjectFilter("$KV.WF_STATE.tombscale.>"))
		if err != nil {
			t.Fatal(err)
		}
		physical = 0
		for subject, messages := range info.State.Subjects {
			if !strings.HasPrefix(subject, "$KV.WF_STATE.tombscale.") {
				t.Fatalf("unexpected filtered subject %q", subject)
			}
			physical += messages
		}
		if physical == 0 {
			break
		}
		select {
		case loopErr := <-loopDone:
			loopFinished = true
			t.Fatalf("tombstone loop stopped with %d physical markers: %v", physical, loopErr)
		case <-time.After(time.Second):
		}
	}
	if ctx.Err() != nil {
		t.Fatalf("physical tombstone drain stopped with %d markers: %v", physical, ctx.Err())
	}
	t.Logf("physical tombstone subjects drained: count=%d retained_marker_messages=%d", count, physical)
	if _, err := state[2].Get(ctx, "scan.tombstone"); err != nil {
		t.Fatalf("missing persisted sweep cursor: %v", err)
	}
	t.Logf("swept %d expired tombstones in %s", count, time.Since(start))
}
