package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/retention"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go/jetstream"
)

func TestContinuationRetirementGCAndGenerationReuse(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	const typ = "checkpoint-retire"
	const retired = "reused"
	const survivor = "survivor"
	payload := strings.Repeat("x", wf.MaxInlineResult)
	sharedJSON, _ := json.Marshal(payload)
	digest := sha256.Sum256(sharedJSON)
	sharedObject := "step-result-" + hex.EncodeToString(digest[:])
	var initialCalls, effects atomic.Int64
	handlers := map[string]worker.Handler{typ: func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
		initialCalls.Add(1)
		if err := c.SetState("value", input); err != nil {
			return nil, err
		}
		if _, err := wf.Run(c, "shared", 0, func(context.Context) (string, error) { effects.Add(1); return payload, nil }); err != nil {
			return nil, err
		}
		return nil, wf.Continue(c, "finish_v1", input)
	}}
	stages := map[string]worker.ContinuationHandler{"finish_v1": func(c *wf.Context, input, locals json.RawMessage) (json.RawMessage, error) {
		var value int
		found, err := c.GetState("value", &value)
		if err != nil {
			return nil, err
		}
		if !found || string(input) != string(locals) {
			return nil, errors.New("wrong restored locals")
		}
		result, _ := json.Marshal(value)
		return result, nil
	}}
	w, err := worker.New(ctx, all[1], "checkpoint-retirement", handlers, worker.WithContinuations(typ, stages))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	c := client.New(all[0])
	first, err := c.Start(ctx, typ, retired, []byte(`1`))
	if err != nil {
		t.Fatal(err)
	}
	other, err := c.Start(ctx, typ, survivor, []byte(`1`))
	if err != nil {
		t.Fatal(err)
	}
	run := func(workerInstance *worker.Worker) (context.CancelFunc, []chan error) {
		runCtx, stop := context.WithCancel(ctx)
		partitions := map[uint32]bool{identity.Partition(typ, retired, provision.Partitions): true, identity.Partition(typ, survivor, provision.Partitions): true}
		var done []chan error
		for partition := range partitions {
			ch := make(chan error, 1)
			done = append(done, ch)
			go func() { ch <- workerInstance.RunPartition(runCtx, partition) }()
		}
		return stop, done
	}
	stop, done := run(w)
	for _, id := range []string{retired, survivor} {
		value, err := c.Await(ctx, typ, id)
		if err != nil || string(value) != "1" {
			t.Fatalf("id=%s result=%s err=%v", id, value, err)
		}
	}
	stop()
	for _, ch := range done {
		if err := <-ch; err != nil {
			t.Fatal(err)
		}
	}
	if initialCalls.Load() != 2 || effects.Load() != 2 {
		t.Fatalf("calls=%d effects=%d", initialCalls.Load(), effects.Load())
	}
	store := journal.New(all[2])
	old, err := store.ReadCheckpoint(ctx, typ, retired, first.InvSeq)
	if err != nil || old == nil {
		t.Fatalf("old checkpoint=%+v err=%v", old, err)
	}
	kept, err := store.ReadCheckpoint(ctx, typ, survivor, other.InvSeq)
	if err != nil || kept == nil {
		t.Fatalf("survivor checkpoint=%+v err=%v", kept, err)
	}
	state, err := all[0].KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	snapshotKey := "snap." + identity.Key(typ, retired)
	saved, err := state.Get(ctx, snapshotKey)
	if err != nil {
		t.Fatal(err)
	}
	savedManifest := append([]byte(nil), saved.Value()...)
	objects, err := all[0].ObjectStore(ctx, "WF_BLOB")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := retention.SweepBlobsQuiescent(ctx, all[0], 0, time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{old.Snapshot.Runtime.Object, old.Snapshot.Object, kept.Snapshot.Runtime.Object, kept.Snapshot.Object, sharedObject} {
		if _, err := objects.GetInfo(ctx, name); err != nil {
			t.Fatalf("reachable object %s: %v", name, err)
		}
	}
	if err := retention.Purge(ctx, all[0], typ, retired, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Await(ctx, typ, retired); !errors.Is(err, client.ErrPurged) {
		t.Fatalf("retired result=%v", err)
	}
	if _, err := state.Get(ctx, snapshotKey); !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatalf("retired manifest retained: %v", err)
	}
	if view, err := store.ReadCheckpoint(ctx, typ, retired, first.InvSeq); err != nil || view != nil {
		t.Fatalf("retired checkpoint=%+v err=%v", view, err)
	}
	swept, err := retention.SweepBlobsQuiescent(ctx, all[0], 0, time.Now().Add(time.Minute))
	if err != nil || swept.Deleted < 2 {
		t.Fatalf("retired sweep=%+v err=%v", swept, err)
	}
	for _, name := range []string{old.Snapshot.Runtime.Object, old.Snapshot.Object} {
		if _, err := objects.GetInfo(ctx, name); !errors.Is(err, jetstream.ErrObjectNotFound) {
			t.Fatalf("retired object survived %s: %v", name, err)
		}
	}
	for _, name := range []string{kept.Snapshot.Runtime.Object, kept.Snapshot.Object, sharedObject} {
		if _, err := objects.GetInfo(ctx, name); err != nil {
			t.Fatalf("shared/survivor object lost %s: %v", name, err)
		}
	}
	sharedBytes, err := objects.GetBytes(ctx, sharedObject)
	if err != nil || !bytes.Equal(sharedBytes, sharedJSON) {
		t.Fatalf("shared content after retirement lost: %v", err)
	}
	second, err := c.Start(ctx, typ, retired, []byte(`2`))
	if err != nil || second.InvSeq <= first.InvSeq {
		t.Fatalf("reused handle=%+v err=%v", second, err)
	}
	// Inject old metadata after reuse. Generation rejection must happen before
	// any old object access; those objects have already been collected.
	revision, err := state.Create(ctx, snapshotKey, savedManifest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReadCheckpoint(ctx, typ, retired, second.InvSeq); !errors.Is(err, journal.ErrCheckpointGeneration) {
		t.Fatalf("retired frame accepted: %v", err)
	}
	rejected := make(chan struct{}, 1)
	rejectWorker, err := worker.New(ctx, all[2], "checkpoint-stale-generation", handlers, worker.WithContinuations(typ, stages), worker.WithDispatchObserver(func(event worker.DispatchEvent) {
		if event.Stage == "execution_retry" && strings.Contains(event.Error, journal.ErrCheckpointGeneration.Error()) {
			select {
			case rejected <- struct{}{}:
			default:
			}
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	rejectCtx, stopReject := context.WithCancel(ctx)
	rejectDone := make(chan error, 1)
	go func() {
		rejectDone <- rejectWorker.RunPartition(rejectCtx, identity.Partition(typ, retired, provision.Partitions))
	}()
	select {
	case <-rejected:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	stopReject()
	if err := <-rejectDone; err != nil {
		t.Fatal(err)
	}
	if err := rejectWorker.Close(); err != nil {
		t.Fatal(err)
	}
	if initialCalls.Load() != 2 || effects.Load() != 2 {
		t.Fatal("stale checkpoint executed user code")
	}
	if err := state.Delete(ctx, snapshotKey, jetstream.LastRevision(revision)); err != nil {
		t.Fatal(err)
	}
	stop, done = run(w)
	value, err := c.Await(ctx, typ, retired)
	stop()
	for _, ch := range done {
		if err := <-ch; err != nil {
			t.Fatal(err)
		}
	}
	if err != nil || string(value) != "2" || initialCalls.Load() != 3 || effects.Load() != 3 {
		t.Fatalf("reused result=%s calls=%d effects=%d err=%v", value, initialCalls.Load(), effects.Load(), err)
	}
	fresh, err := store.ReadCheckpoint(ctx, typ, retired, second.InvSeq)
	if err != nil || fresh == nil || fresh.Snapshot.Runtime.InvSeq != second.InvSeq || fresh.Snapshot.Runtime.Object == old.Snapshot.Runtime.Object {
		t.Fatalf("fresh checkpoint=%+v err=%v", fresh, err)
	}
	for _, peer := range all {
		value, err := client.New(peer).Await(ctx, typ, retired)
		if err != nil || string(value) != "2" {
			t.Fatalf("peer reused result=%s err=%v", value, err)
		}
	}
	if value, err := c.Await(ctx, typ, survivor); err != nil || string(value) != "1" {
		t.Fatalf("survivor result=%s err=%v", value, err)
	}
	report, err := integrity.Check(ctx, all[2])
	if err != nil || report.Invocations != 2 || report.Terminal != 2 {
		t.Fatalf("reuse integrity=%+v err=%v", report, err)
	}
	if _, err := retention.SweepBlobsQuiescent(ctx, all[0], 0, time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{fresh.Snapshot.Runtime.Object, kept.Snapshot.Runtime.Object, sharedObject} {
		if _, err := objects.GetInfo(ctx, name); err != nil {
			t.Fatalf("reused reference lost %s: %v", name, err)
		}
	}
	sharedBytes, err = objects.GetBytes(ctx, sharedObject)
	if err != nil || !bytes.Equal(sharedBytes, sharedJSON) {
		t.Fatalf("shared content after reuse lost: %v", err)
	}
	t.Logf("continuation retirement/reuse: old_generation=%d fresh_generation=%d reclaimed=%d calls=%d effects=%d terminals=%d shared_blob_retained=true", first.InvSeq, second.InvSeq, swept.Deleted, initialCalls.Load(), effects.Load(), report.Terminal)
}
