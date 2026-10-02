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

	"js-wf/internal/checkpoint"
	"js-wf/journal"
	"js-wf/wf"
)

func runSeededContinuationSDK(seed int64, replay *Trace) (trace Trace, runErr error) {
	schedule := NewScheduler(seed)
	if replay != nil {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	if err := schedule.SetWorkload("continuation_sdk"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	modes := []string{"clean", "request_drop", "request_ack_lost", "completion_drop", "completion_ack_lost", "frame_drop", "frame_ack_lost", "frame_get_failure"}
	mode, err := schedule.Choose(modes)
	if err != nil {
		return trace, err
	}
	ctx := context.Background()
	const typ, id = "test", "continue"
	live := NewJournalTransport(schedule)
	objects := NewSnapshotReadTransport(schedule)
	objects.BindJournal(live)
	store := journal.NewWithSnapshotPort(live, live, objects)
	var records []journal.Record
	var tail uint64
	epoch := uint64(51)
	faultFired := false
	add := func(kind journal.Kind, payload json.RawMessage) error {
		if !faultFired && (mode == "request_drop" || mode == "request_ack_lost" || mode == "completion_drop" || mode == "completion_ack_lost") {
			var request struct {
				Kind string `json:"kind"`
			}
			_ = json.Unmarshal(payload, &request)
			inject := kind == journal.StepRequested && request.Kind == "checkpoint" && (mode == "request_drop" || mode == "request_ack_lost") || kind == journal.StepCompleted && (mode == "completion_drop" || mode == "completion_ack_lost") && len(records) >= 6
			if inject {
				fault := Fault{Kind: DropBeforeCommit}
				if mode == "request_ack_lost" || mode == "completion_ack_lost" {
					fault.Kind = LoseAckAfterCommit
				}
				if err := live.QueueFault(fault); err != nil {
					return err
				}
				faultFired = true
			}
		}
		entry := journal.Entry{Index: uint64(len(records)), Epoch: epoch, Kind: kind, Payload: payload, WorkerID: fmt.Sprintf("owner-%d", epoch)}
		seq, err := store.Append(ctx, typ, id, entry, tail)
		if err != nil {
			return err
		}
		records = append(records, journal.Record{Entry: entry, Sequence: seq})
		tail = seq
		return nil
	}
	if err := add(journal.Started, nil); err != nil {
		return trace, err
	}
	puts := 0
	effects := 0
	configure := func(entries []wf.Entry) *wf.Context {
		c := wf.NewContext(ctx, entries, func(_ context.Context, kind wf.Kind, payload json.RawMessage) error {
			return add(journal.Kind(kind), payload)
		})
		c.SetChildSupport(typ, id, 17, nil)
		c.SetContinuationSupport(func(stage string) bool { return stage == "next_v1" }, func(completed uint64, recorded bool) (wf.ContinuationAnchor, error) {
			if completed != 0 {
				if completed >= uint64(len(records)) {
					return wf.ContinuationAnchor{}, journal.ErrGap
				}
				return wf.ContinuationAnchor{Index: completed, Epoch: records[completed].Epoch}, nil
			}
			index := uint64(len(records) + 1)
			if recorded {
				index--
			}
			return wf.ContinuationAnchor{Index: index, Epoch: epoch}, nil
		})
		c.SetResultStore(func(ctx context.Context, raw []byte) (string, error) {
			puts++
			hash := sha256.Sum256(raw)
			name := "step-result-" + hex.EncodeToString(hash[:])
			return name, objects.PutObject(ctx, name, raw)
		}, objects.GetObject)
		return c
	}
	prefix := func(c *wf.Context) error {
		if err := c.SetState("total", 23); err != nil {
			return err
		}
		_, err := wf.RunOnce(c, "external", 23, func(context.Context, string) (int, error) { effects++; return 46, nil })
		return err
	}
	c := configure(nil)
	if err := prefix(c); err != nil {
		return trace, err
	}
	switch mode {
	case "frame_drop", "frame_ack_lost":
		fault := SnapshotFault{"put_object", DropBeforeCommit}
		if mode == "frame_ack_lost" {
			fault.Kind = LoseAckAfterCommit
		}
		if err := objects.QueueWriteFault(fault); err != nil {
			return trace, err
		}
	case "frame_get_failure":
		objects.QueueObjectTransient()
	}
	first := wf.Continue(c, "next_v1", 23)
	if mode == "clean" {
		if !errors.Is(first, wf.ErrContinuation) {
			return trace, first
		}
	} else {
		if first == nil || errors.Is(first, wf.ErrContinuation) {
			return trace, fmt.Errorf("mode %s missed fault: %v", mode, first)
		}
		if _, ok := c.Continuation(); ok {
			return trace, fmt.Errorf("unknown outcome advertised checkpoint")
		}
		if c.CheckComplete() == nil {
			return trace, fmt.Errorf("failed checkpoint could complete")
		}
	}
	// Redelivery must reconstruct actual committed state, including an append
	// whose reply was lost. The new owner uses a higher epoch only for new writes.
	records, tail, err = store.Read(ctx, typ, id)
	if err != nil {
		return trace, err
	}
	epoch = 99
	steps := make([]wf.Entry, 0, len(records)-1)
	for _, record := range records[1:] {
		steps = append(steps, wf.Entry{Index: record.Index, Kind: wf.Kind(record.Kind), Payload: record.Payload})
	}
	repair := configure(steps)
	if err := prefix(repair); err != nil {
		return trace, err
	}
	if err := wf.Continue(repair, "next_v1", 23); !errors.Is(err, wf.ErrContinuation) {
		return trace, err
	}
	point, ok := repair.Continuation()
	if !ok || len(records) != 7 || effects != 1 {
		return trace, fmt.Errorf("repair point=%+v records=%d effects=%d", point, len(records), effects)
	}
	if mode == "clean" || mode == "completion_ack_lost" {
		if point.Epoch != 51 {
			return trace, fmt.Errorf("recorded epoch lost")
		}
	} else if point.Epoch != 99 {
		return trace, fmt.Errorf("new completion epoch lost")
	}
	beforePuts := puts
	records, tail, err = store.Read(ctx, typ, id)
	if err != nil {
		return trace, err
	}
	steps = nil
	for _, record := range records[1:] {
		steps = append(steps, wf.Entry{Index: record.Index, Kind: wf.Kind(record.Kind), Payload: record.Payload})
	}
	again := configure(steps)
	if err := prefix(again); err != nil {
		return trace, err
	}
	if err := wf.Continue(again, "next_v1", 23); !errors.Is(err, wf.ErrContinuation) || puts != beforePuts || effects != 1 {
		return trace, fmt.Errorf("immutable replay: %v", err)
	}
	runtime := journal.RuntimeCheckpoint{InvSeq: 17, Stage: point.Stage, Sequence: tail, Index: point.Index, Epoch: point.Epoch, StepPosition: point.StepPosition, Object: point.Object, SHA256: point.SHA256}
	snap, err := store.WriteCheckpointSnapshot(ctx, typ, id, runtime)
	if err != nil {
		return trace, err
	}
	if err := store.PurgeSnapshot(ctx, typ, id, snap); err != nil {
		return trace, err
	}
	guard := &checkpointArchiveGuard{SnapshotReadTransport: objects}
	reader := journal.NewWithSnapshotReadPort(live, live, guard)
	view, err := reader.ReadCheckpoint(ctx, typ, id, 17)
	if err != nil || view == nil {
		return trace, fmt.Errorf("resume read: %v", err)
	}
	resumed, _, err := wf.NewCheckpointContext(ctx, nil, nil, view.Frame, wf.CheckpointLocation{Type: typ, ID: id, InvSeq: 17, Index: runtime.Index, Epoch: runtime.Epoch, Hash: runtime.SHA256})
	if err != nil {
		return trace, err
	}
	// Inspect the immutable frame's saved state without executing an SDK step
	// on a context deliberately configured with no appender.
	frame, err := checkpoint.Decode(view.Frame, runtime.SHA256, checkpoint.Identity{Type: typ, ID: id, InvSeq: 17}, checkpoint.Anchor{Index: runtime.Index, Epoch: runtime.Epoch})
	if err != nil || resumed == nil || string(frame.State["total"]) != "23" || guard.denied != 0 {
		return trace, fmt.Errorf("restore: %v", err)
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_continuation_sdk", Outcome: mode, Sequence: point.Index, AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededContinuationSDKReplay(t *testing.T) {
	if os.Getenv("SIM_CONTINUATION_SDK_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededContinuationSDK(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_CONTINUATION_SDK_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	observed := map[string]int{}
	for seed := range seededSchedules(t) {
		trace, err := runSeededContinuationSDK(seed, nil)
		if err != nil {
			root := os.Getenv("FAULT_TRACE_OUT")
			if root == "" {
				dir, mkdirErr := os.MkdirTemp("", "js-wf-checkpoint-failure-")
				if mkdirErr != nil {
					t.Fatal(mkdirErr)
				}
				root = filepath.Join(dir, "trace.json")
			}
			_ = trace.Save(root)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, root, err)
		}
		observed[trace.Decisions[0].Chosen]++
		if seed <= 10 {
			again, err := runSeededContinuationSDK(seed, &trace)
			if err != nil || !reflect.DeepEqual(trace, again) {
				t.Fatalf("seed %d replay: %v", seed, err)
			}
		}
	}
	for _, mode := range []string{"clean", "request_drop", "request_ack_lost", "completion_drop", "completion_ack_lost", "frame_drop", "frame_ack_lost", "frame_get_failure"} {
		if observed[mode] == 0 {
			t.Fatalf("mode %s not covered", mode)
		}
	}
	var paths [2]string
	for i := range paths {
		paths[i] = filepath.Join(t.TempDir(), "checkpoint.json")
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededContinuationSDKReplay$")
		cmd.Env = append(os.Environ(), "SIM_CONTINUATION_SDK_HELPER=1", "FAULT_SEED=42", "SIM_CONTINUATION_SDK_OUT="+paths[i])
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child %d: %v %s", i, err, output)
		}
	}
	a, err := os.ReadFile(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(paths[1])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatal("checkpoint replay changes across processes")
	}
}
