package sim

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/retention"
	"js-wf/wf"
)

func runSeededContinuationRetirement(seed int64, replay *Trace) (trace Trace, runErr error) {
	schedule := NewScheduler(seed)
	if replay != nil {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	if err := schedule.SetWorkload("continuation_retirement"); err != nil {
		return trace, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"clean", "marker_ack_lost", "signals_ack_lost", "journal_drop", "snapshot_delete_drop", "tombstone_ack_lost", "purge_event_ack_lost", "invocation_ack_lost"})
	if err != nil {
		return trace, err
	}
	gcMode, err := schedule.Choose([]string{"clean", "frame_corrupt", "frame_read_lost", "delete_drop", "delete_ack_lost"})
	if err != nil {
		return trace, err
	}
	ctx := context.Background()
	const typ = "retire"
	port := NewPurgeTransport(schedule)
	store := journal.NewWithSnapshotPort(port, port, port)
	old := time.Unix(0, 0).UTC().Add(-time.Hour)
	now := time.Unix(0, 0).UTC().Add(time.Hour)
	shared := []byte(`23`)
	sum := sha256.Sum256(shared)
	sharedName := "step-result-" + hex.EncodeToString(sum[:])
	port.Blobs.PutObject(sharedName, shared, old)
	port.Blobs.PutObject("input-orphan", []byte(`orphan`), old)
	port.Blobs.PutObject("user-unmanaged", []byte(`user`), old)
	type invocation struct {
		id         string
		generation uint64
		records    []journal.Record
		tail       uint64
		snapshot   journal.Snapshot
		frame      []byte
		epoch      uint64
	}
	add := func(v *invocation, kind journal.Kind, payload json.RawMessage) error {
		entry := journal.Entry{Index: uint64(len(v.records)), Epoch: v.epoch, Kind: kind, Payload: payload, WorkerID: fmt.Sprintf("sdk-%d", v.epoch)}
		seq, err := store.Append(ctx, typ, v.id, entry, v.tail)
		if err != nil {
			return err
		}
		v.tail = seq
		v.records = append(v.records, journal.Record{Entry: entry, Sequence: seq})
		return nil
	}
	build := func(id string) (*invocation, error) {
		generation, err := port.Blobs.PublishSubject("WF_INV", identity.InvocationSubject(typ, id), nil, []byte(`23`))
		if err != nil {
			return nil, err
		}
		v := &invocation{id: id, generation: generation, epoch: 1}
		if err := add(v, journal.Started, nil); err != nil {
			return nil, err
		}
		outcome, _ := json.Marshal(wf.Outcome{ResultRef: sharedName, ResultHash: hex.EncodeToString(sum[:])})
		sigSeq, err := port.Blobs.PublishSubject("WF_SIG", "wf.sig."+typ+"."+id+".held", nil, outcome)
		if err != nil {
			return nil, err
		}
		consumed, _ := json.Marshal(map[string]any{"sig_seq": sigSeq, "name": "held", "payload": outcome})
		if err := add(v, journal.SignalConsumed, consumed); err != nil {
			return nil, err
		}
		c := wf.NewContext(ctx, nil, func(_ context.Context, kind wf.Kind, payload json.RawMessage) error {
			return add(v, journal.Kind(kind), payload)
		}, wf.Signal{Sequence: sigSeq, Name: "held", Payload: outcome})
		c.SetChildSupport(typ, id, generation, nil)
		c.SetResultStore(func(ctx context.Context, raw []byte) (string, error) {
			hash := sha256.Sum256(raw)
			name := "step-result-" + hex.EncodeToString(hash[:])
			return name, port.PutObject(ctx, name, raw)
		}, port.GetObject)
		c.SetContinuationSupport(func(stage string) bool { return stage == "finish_v1" }, func(completed uint64, recorded bool) (wf.ContinuationAnchor, error) {
			return wf.ContinuationAnchor{Index: uint64(len(v.records) + 1), Epoch: v.epoch, SignalCursor: sigSeq}, nil
		})
		if err := c.SetState("value", 23); err != nil {
			return nil, err
		}
		got, err := wf.AwaitPromise(c, wf.Promise{SignalName: "held"})
		if err != nil || !bytes.Equal(got, shared) {
			return nil, fmt.Errorf("promise=%s err=%v", got, err)
		}
		if err := wf.Continue(c, "finish_v1", 23); !errors.Is(err, wf.ErrContinuation) {
			return nil, fmt.Errorf("continue=%v", err)
		}
		point, ok := c.Continuation()
		if !ok {
			return nil, fmt.Errorf("no committed SDK frame")
		}
		runtime := journal.RuntimeCheckpoint{InvSeq: generation, Stage: point.Stage, Index: point.Index, Epoch: point.Epoch, Sequence: v.records[point.Index].Sequence, StepPosition: point.StepPosition, Object: point.Object, SHA256: point.SHA256}
		v.snapshot, err = store.WriteCheckpointSnapshot(ctx, typ, id, runtime)
		if err != nil {
			return nil, err
		}
		if err := store.PurgeSnapshot(ctx, typ, id, v.snapshot); err != nil {
			return nil, err
		}
		v.frame, err = port.GetObject(ctx, point.Object)
		if err != nil {
			return nil, err
		}
		if err := add(v, journal.Suspended, json.RawMessage(`{"waiting_on":"continuation:finish_v1"}`)); err != nil {
			return nil, err
		}
		return v, nil
	}
	retired, err := build("retired")
	if err != nil {
		return trace, err
	}
	survivor, err := build("survivor")
	if err != nil {
		return trace, err
	}
	complete := func(v *invocation) error {
		v.epoch++
		r := v.snapshot.Runtime
		c, info, err := wf.NewCheckpointContext(ctx, nil, func(_ context.Context, kind wf.Kind, payload json.RawMessage) error {
			return add(v, journal.Kind(kind), payload)
		}, v.frame, wf.CheckpointLocation{Type: typ, ID: v.id, InvSeq: v.generation, Index: r.Index, Epoch: r.Epoch, Hash: r.SHA256})
		if err != nil {
			return err
		}
		if info.Stage != "finish_v1" || string(info.Data) != "23" {
			return fmt.Errorf("wrong locals")
		}
		c.SetResultStore(nil, port.GetObject)
		got, err := wf.AwaitPromise(c, wf.Promise{SignalName: "held"})
		if err != nil || !bytes.Equal(got, shared) {
			return fmt.Errorf("restored promise=%s err=%v", got, err)
		}
		var value int
		found, err := c.GetState("value", &value)
		if err != nil || !found || value != 23 {
			return fmt.Errorf("restored state=%d found=%v err=%v", value, found, err)
		}
		terminal, _ := json.Marshal(wf.Outcome{InvSeq: v.generation, Result: shared})
		if err := add(v, journal.Completed, terminal); err != nil {
			return err
		}
		_, err = port.Blobs.State().Create(ctx, identity.Key(typ, v.id), terminal)
		if err != nil {
			return err
		}
		_, err = integrity.CheckSnapshot(integrity.Snapshot{Invocations: []string{identity.InvocationSubject(typ, v.id)}, Journals: map[string][]journal.Record{identity.JournalSubject(typ, v.id): v.records}, TerminalState: map[string][]byte{identity.Key(typ, v.id): terminal}})
		return err
	}
	if err := retention.PurgeWithPort(ctx, port, typ, survivor.id, time.Hour); !errors.Is(err, retention.ErrNotTerminal) {
		return trace, fmt.Errorf("unfinished survivor retirement=%v", err)
	}
	frameName := survivor.snapshot.Runtime.Object
	switch gcMode {
	case "frame_corrupt":
		port.Blobs.PutObject(frameName, []byte(`corrupt`), old)
	case "frame_read_lost":
		if err := port.Blobs.QueueReadFault(frameName); err != nil {
			return trace, err
		}
	case "delete_drop", "delete_ack_lost":
		fault := DropBeforeCommit
		if gcMode == "delete_ack_lost" {
			fault = LoseAckAfterCommit
		}
		if err := port.Blobs.QueueDeleteFault(fault); err != nil {
			return trace, err
		}
	}
	first, firstErr := retention.SweepBlobsQuiescentWithPort(ctx, port.Blobs, 0, now)
	if gcMode == "clean" {
		if firstErr != nil {
			return trace, firstErr
		}
	} else if firstErr == nil {
		return trace, fmt.Errorf("GC fault %s hidden", gcMode)
	}
	if gcMode == "frame_corrupt" || gcMode == "frame_read_lost" {
		if first.Deleted != 0 || !port.Blobs.HasObject("input-orphan") {
			return trace, fmt.Errorf("mark failure permitted deletion")
		}
		if gcMode == "frame_corrupt" {
			port.Blobs.PutObject(frameName, survivor.frame, old)
		}
	}
	initial, err := retention.SweepBlobsQuiescentWithPort(ctx, port.Blobs, 0, now)
	if err != nil || initial.Referenced != 5 || port.Blobs.HasObject("input-orphan") || !port.Blobs.HasObject(sharedName) {
		return trace, fmt.Errorf("initial sweep=%+v err=%v", initial, err)
	}
	if err := complete(retired); err != nil {
		return trace, err
	}
	switch mode {
	case "marker_ack_lost":
		err = port.Blobs.State().QueueFault(KVFault{Operation: "put", Kind: KVLoseAckAfterCommit})
	case "snapshot_delete_drop":
		err = port.Blobs.State().QueueFault(KVFault{Operation: "delete", Kind: KVDropBeforeCommit})
	case "tombstone_ack_lost":
		err = port.Blobs.State().QueueFault(KVFault{Operation: "update", Kind: KVLoseAckAfterCommit})
	case "signals_ack_lost", "journal_drop", "purge_event_ack_lost", "invocation_ack_lost":
		operation := map[string]string{"signals_ack_lost": "purge_WF_SIG", "journal_drop": "purge_WF_JRN", "purge_event_ack_lost": "publish_purge", "invocation_ack_lost": "purge_WF_INV"}[mode]
		fault := LoseAckAfterCommit
		if mode == "journal_drop" {
			fault = DropBeforeCommit
		}
		err = port.QueueFault(PurgeFault{Operation: operation, Kind: fault})
	}
	if err != nil {
		return trace, err
	}
	purgeErr := retention.PurgeWithPort(ctx, port, typ, retired.id, time.Hour)
	if mode == "clean" || mode == "tombstone_ack_lost" {
		if purgeErr != nil {
			return trace, purgeErr
		}
	} else if purgeErr == nil {
		return trace, fmt.Errorf("purge fault %s hidden", mode)
	}
	// Quiescent GC can run between uncertain retirement and its retry. It may
	// collect the retired prefix after durable removal, but must preserve the
	// unfinished survivor's frame, archive and frame-held promise result.
	intermediate, err := retention.SweepBlobsQuiescentWithPort(ctx, port.Blobs, 0, now)
	if err != nil || !port.Blobs.HasObject(sharedName) || !port.Blobs.HasObject(survivor.snapshot.Object) || !port.Blobs.HasObject(frameName) {
		return trace, fmt.Errorf("interrupted retirement sweep=%+v err=%v", intermediate, err)
	}
	if err := retention.PurgeWithPort(ctx, port, typ, retired.id, time.Hour); err != nil {
		return trace, err
	}
	for _, key := range []string{"snap." + identity.Key(typ, retired.id), "purging." + identity.Key(typ, retired.id)} {
		if _, err := port.Blobs.State().Get(ctx, key); !errors.Is(err, jetstream.ErrKeyNotFound) {
			return trace, fmt.Errorf("retained %s: %v", key, err)
		}
	}
	middle, err := retention.SweepBlobsQuiescentWithPort(ctx, port.Blobs, 0, now)
	if err != nil || middle.Referenced != 3 || middle.Deleted+intermediate.Deleted != 2 || !port.Blobs.HasObject(sharedName) || port.Blobs.HasObject(retired.snapshot.Runtime.Object) || port.Blobs.HasObject(retired.snapshot.Object) {
		return trace, fmt.Errorf("one survivor sweep=%+v err=%v", middle, err)
	}
	if err := complete(survivor); err != nil {
		return trace, err
	}
	if err := retention.PurgeWithPort(ctx, port, typ, survivor.id, time.Hour); err != nil {
		return trace, err
	}
	final, err := retention.SweepBlobsQuiescentWithPort(ctx, port.Blobs, 0, now)
	if err != nil || final.Referenced != 0 || final.Deleted != 3 || port.Blobs.HasObject(sharedName) || !port.Blobs.HasObject("user-unmanaged") || port.PurgeEventCount() != 2 {
		return trace, fmt.Errorf("final sweep=%+v events=%d err=%v", final, port.PurgeEventCount(), err)
	}
	for _, v := range []*invocation{retired, survivor} {
		state, err := port.Blobs.State().Get(ctx, identity.Key(typ, v.id))
		if err != nil {
			return trace, err
		}
		tombstone, tomb, err := retention.Decode(state.Value)
		if err != nil || !tomb || tombstone.InvSeq != v.generation {
			return trace, fmt.Errorf("wrong tombstone %s: %+v tomb=%v err=%v", v.id, tombstone, tomb, err)
		}
		if _, err := port.Invocation(ctx, identity.InvocationSubject(typ, v.id)); !errors.Is(err, jetstream.ErrMsgNotFound) {
			return trace, fmt.Errorf("retained invocation %s: %v", v.id, err)
		}
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_continuation_retirement", Subject: typ, Outcome: mode + ":" + gcMode, Sequence: uint64(final.Deleted), AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededContinuationRetirementReplay(t *testing.T) {
	if os.Getenv("SIM_CONTINUATION_RETIREMENT_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededContinuationRetirement(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_CONTINUATION_RETIREMENT_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	observed := map[string]int{}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		generated, err := runSeededContinuationRetirement(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "continuation-retirement-failure.json")
			}
			_ = generated.Save(path)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		observed[generated.Decisions[0].Chosen+":"+generated.Decisions[1].Chosen]++
		if seed <= 10 {
			replayed, err := runSeededContinuationRetirement(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d replay: %v", seed, err)
			}
		}
	}
	if len(observed) != 40 {
		t.Fatalf("covered %d/40 retirement/GC combinations", len(observed))
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("continuation-retirement-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededContinuationRetirementReplay$")
		cmd.Env = append(os.Environ(), "SIM_CONTINUATION_RETIREMENT_HELPER=1", "SIM_CONTINUATION_RETIREMENT_OUT="+files[i], "FAULT_SEED=42")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child %d: %v: %s", i, err, output)
		}
	}
	first, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(files[1])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("purge/blob trace changed across processes")
	}
}
