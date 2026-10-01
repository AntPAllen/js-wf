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
	"strings"
	"testing"

	"js-wf/identity"
	"js-wf/internal/checkpoint"
	"js-wf/journal"
)

type checkpointArchiveGuard struct {
	*SnapshotReadTransport
	denied int
}

func (p *checkpointArchiveGuard) GetObject(ctx context.Context, name string) ([]byte, error) {
	if strings.HasPrefix(name, "snapshot-") {
		p.denied++
		return nil, fmt.Errorf("archive read forbidden")
	}
	return p.SnapshotReadTransport.GetObject(ctx, name)
}

type checkpointAdvanceRead struct {
	journal.ReadPort
	advance func() error
}

func (p *checkpointAdvanceRead) Next(ctx context.Context, subject string, from uint64) (journal.AppendTail, error) {
	if p.advance != nil {
		action := p.advance
		p.advance = nil
		if err := action(); err != nil {
			return journal.AppendTail{}, err
		}
	}
	return p.ReadPort.Next(ctx, subject, from)
}

func runSeededCheckpointRead(seed int64, replay *Trace) (trace Trace, runErr error) {
	schedule := NewScheduler(seed)
	if replay != nil {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	if err := schedule.SetWorkload("checkpoint_read"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	modes := []string{"clean", "generation", "anchor_delete", "suffix_gap", "epoch_regression", "terminal_tail", "frame_corrupt", "transient", "manifest_advance", "long_suffix"}
	mode, err := schedule.Choose(modes)
	if err != nil {
		return trace, err
	}
	ctx := context.Background()
	const typ, id = "test", "checkpoint-read"
	live := NewJournalTransport(schedule)
	port := NewSnapshotReadTransport(schedule)
	port.BindJournal(live)
	writer := journal.NewWithSnapshotPort(live, live, port)
	var records []journal.Record
	var tail uint64
	add := func(kind journal.Kind, payload []byte) error {
		entry := journal.Entry{Index: uint64(len(records)), Epoch: 51, Kind: kind, Payload: payload, WorkerID: "checkpoint-owner"}
		seq, err := writer.Append(ctx, typ, id, entry, tail)
		if err != nil {
			return err
		}
		records = append(records, journal.Record{Entry: entry, Sequence: seq})
		tail = seq
		return nil
	}
	for _, kind := range []journal.Kind{journal.Started, journal.StepRequested, journal.StepCompleted} {
		if err := add(kind, nil); err != nil {
			return trace, err
		}
	}
	pair := func(stage string) (journal.RuntimeCheckpoint, error) {
		index := uint64(len(records) + 1)
		frame := checkpoint.Frame{Version: 1, Identity: checkpoint.Identity{Type: typ, ID: id, InvSeq: 17}, Stage: stage, Data: json.RawMessage(`23`), Anchor: checkpoint.Anchor{Index: index, Epoch: 51}, StepPosition: index, State: map[string]json.RawMessage{"value": json.RawMessage(`23`)}}
		raw, hash, err := checkpoint.Encode(frame)
		if err != nil {
			return journal.RuntimeCheckpoint{}, err
		}
		object := "step-result-" + hash
		if err := port.PutObject(ctx, object, raw); err != nil {
			return journal.RuntimeCheckpoint{}, err
		}
		input := sha256.Sum256(frame.Data)
		request, _ := json.Marshal(map[string]string{"kind": "checkpoint", "name": stage, "input_hash": hex.EncodeToString(input[:])})
		done, _ := json.Marshal(map[string]string{"result_ref": object, "result_hash": hash})
		if err := add(journal.StepRequested, request); err != nil {
			return journal.RuntimeCheckpoint{}, err
		}
		if err := add(journal.StepCompleted, done); err != nil {
			return journal.RuntimeCheckpoint{}, err
		}
		return journal.RuntimeCheckpoint{InvSeq: 17, Stage: stage, Sequence: tail, Index: index, Epoch: 51, StepPosition: index, Object: object, SHA256: hash}, nil
	}
	runtime, err := pair("next_v1")
	if err != nil {
		return trace, err
	}
	snap, err := writer.WriteCheckpointSnapshot(ctx, typ, id, runtime)
	if err != nil {
		return trace, err
	}
	if err := writer.PurgeSnapshot(ctx, typ, id, snap); err != nil {
		return trace, err
	}
	count := 1
	if mode == "long_suffix" {
		count = 150
	}
	for i := 0; i < count; i++ {
		if err := add(journal.StepRequested, []byte(`{"kind":"run"}`)); err != nil {
			return trace, err
		}
		if err := add(journal.StepCompleted, []byte(`{"result":23}`)); err != nil {
			return trace, err
		}
	}
	guard := &checkpointArchiveGuard{SnapshotReadTransport: port}
	readPort := &checkpointAdvanceRead{ReadPort: live}
	reader := journal.NewWithSnapshotReadPort(live, readPort, guard)
	if mode == "long_suffix" {
		reader = journal.NewWithSnapshotReadPortAndBatch(live, readPort, live, guard)
	}
	expected := runtime
	invSeq := uint64(17)
	var wantErr error
	switch mode {
	case "generation":
		invSeq++
		wantErr = journal.ErrCheckpointGeneration
	case "anchor_delete":
		live.DeleteMessage(identity.JournalSubject(typ, id), runtime.Sequence)
		wantErr = journal.ErrGap
	case "suffix_gap":
		live.DeleteMessage(identity.JournalSubject(typ, id), runtime.Sequence+1)
		wantErr = journal.ErrGap
	case "frame_corrupt":
		if err := port.CorruptObject(runtime.Object); err != nil {
			return trace, err
		}
		wantErr = journal.ErrGap
	case "transient":
		if err := port.QueueManifestReadFault("unavailable"); err != nil {
			return trace, err
		}
		if err := live.QueueNextFault(NextReadFault{Sequence: runtime.Sequence + 1, Kind: "no_responders"}); err != nil {
			return trace, err
		}
	case "epoch_regression", "terminal_tail":
		live.mu.Lock()
		messages := live.messages[identity.JournalSubject(typ, id)]
		if mode == "epoch_regression" {
			var entry journal.Entry
			_ = json.Unmarshal(messages[1].Data, &entry)
			entry.Epoch = 50
			messages[1].Data, _ = json.Marshal(entry)
		} else {
			var entry journal.Entry
			_ = json.Unmarshal(messages[1].Data, &entry)
			entry.Kind = journal.Completed
			messages[1].Data, _ = json.Marshal(entry)
		}
		live.messages[identity.JournalSubject(typ, id)] = messages
		live.mu.Unlock()
		schedule.RecordTransport(TransportEvent{Operation: "inject_checkpoint_suffix_corruption", Subject: identity.JournalSubject(typ, id), Outcome: mode, AtMillis: schedule.NowMillis()})
		wantErr = journal.ErrGap
	case "manifest_advance":
		expected, err = pair("done_v1")
		if err != nil {
			return trace, err
		}
		readPort.advance = func() error {
			newer, err := writer.WriteCheckpointSnapshot(ctx, typ, id, expected)
			if err != nil {
				return err
			}
			return writer.PurgeSnapshot(ctx, typ, id, newer)
		}
	}
	view, err := reader.ReadCheckpoint(ctx, typ, id, invSeq)
	if wantErr != nil {
		if !errors.Is(err, wantErr) || view != nil {
			return trace, fmt.Errorf("mode %s view=%+v err=%v", mode, view, err)
		}
	} else if err != nil || view == nil || *view.Snapshot.Runtime != expected || view.Tail != tail || !reflect.DeepEqual(view.Records, records[expected.Index+1:]) {
		return trace, fmt.Errorf("mode %s view=%+v err=%v", mode, view, err)
	}
	if guard.denied != 0 {
		return trace, fmt.Errorf("resume read archival prefix")
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_checkpoint_read", Subject: identity.JournalSubject(typ, id), Outcome: mode, AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededCheckpointReadReplay(t *testing.T) {
	if os.Getenv("SIM_CHECKPOINT_READ_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededCheckpointRead(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_CHECKPOINT_READ_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	observed := map[string]int{}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		trace, err := runSeededCheckpointRead(seed, nil)
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
			again, err := runSeededCheckpointRead(seed, &trace)
			if err != nil || !reflect.DeepEqual(trace, again) {
				t.Fatalf("seed %d replay: %v", seed, err)
			}
		}
	}
	for _, mode := range []string{"clean", "generation", "anchor_delete", "suffix_gap", "epoch_regression", "terminal_tail", "frame_corrupt", "transient", "manifest_advance", "long_suffix"} {
		if observed[mode] == 0 {
			t.Fatalf("mode %s not covered", mode)
		}
	}
	var paths [2]string
	for i := range paths {
		paths[i] = filepath.Join(t.TempDir(), "checkpoint.json")
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededCheckpointReadReplay$")
		cmd.Env = append(os.Environ(), "SIM_CHECKPOINT_READ_HELPER=1", "FAULT_SEED=42", "SIM_CHECKPOINT_READ_OUT="+paths[i])
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
