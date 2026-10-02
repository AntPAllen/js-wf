package sim

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	workerpkg "js-wf/worker"
)

func retainedModelSnapshot(starts *StartTransport, journals *JournalTransport, state *KVTransport) (integrity.Snapshot, error) {
	snapshot := integrity.Snapshot{Journals: map[string][]journal.Record{}, TerminalState: map[string][]byte{}}
	starts.mu.Lock()
	for _, message := range starts.invocations {
		snapshot.Invocations = append(snapshot.Invocations, message.Subject)
	}
	starts.mu.Unlock()
	journals.mu.Lock()
	for subject, messages := range journals.messages {
		for _, message := range messages {
			var entry journal.Entry
			if err := json.Unmarshal(message.Data, &entry); err != nil {
				journals.mu.Unlock()
				return snapshot, err
			}
			snapshot.Journals[subject] = append(snapshot.Journals[subject], journal.Record{Entry: entry, Sequence: message.Sequence})
		}
	}
	journals.mu.Unlock()
	state.mu.Lock()
	for key, item := range state.items {
		snapshot.TerminalState[key] = append([]byte(nil), item.value...)
	}
	state.mu.Unlock()
	return snapshot, nil
}

func runSeededRetainedInvariantChecks(seed int64, replay *Trace) (trace Trace, runErr error) {
	var schedule *Scheduler
	if replay == nil {
		schedule = NewScheduler(seed)
	} else {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	if err := schedule.SetWorkload("retained_invariants_5"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	ctx := context.Background()
	starts := NewStartTransport(schedule)
	journals := NewJournalTransport(schedule)
	state := NewKVTransport(schedule, 0)
	c := client.NewWithStartPort(starts)
	j := journal.NewWithPorts(journals, journals)
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("invariant-%02d", i)
		handle, err := c.Start(ctx, "test", id, []byte(`null`))
		if err != nil || handle.InvSeq == 0 {
			return trace, fmt.Errorf("seed %d start %s: %+v %w", seed, id, handle, err)
		}
		if _, err := c.Start(ctx, "test", id, []byte(`null`)); !errors.Is(err, client.ErrAlreadyStarted) {
			return trace, fmt.Errorf("seed %d duplicate start %s: %v", seed, id, err)
		}
		worker, err := schedule.Choose([]string{"worker-a", "worker-b"})
		if err != nil {
			return trace, err
		}
		appendEntry := func(index uint64, kind journal.Kind, payload []byte, expected uint64) (uint64, error) {
			epoch := uint64(1)
			id := worker
			if index == 0 {
				epoch, id = 0, ""
			}
			return j.Append(ctx, "test", fmt.Sprintf("invariant-%02d", i), journal.Entry{Epoch: epoch, Index: index, Kind: kind, Payload: payload, WorkerID: id}, expected)
		}
		seq, err := appendEntry(0, journal.Started, nil, 0)
		if err != nil {
			return trace, err
		}
		seq, err = appendEntry(1, journal.StepRequested, []byte(`{"kind":"effect","name":"step"}`), seq)
		if err != nil {
			return trace, err
		}
		fault, err := schedule.Choose([]string{"ok", "drop_before_commit", "lose_ack_after_commit"})
		if err != nil {
			return trace, err
		}
		if fault != "ok" {
			if err := journals.QueueFault(Fault{Kind: AppendFault(fault)}); err != nil {
				return trace, err
			}
		}
		completed, appendErr := appendEntry(2, journal.StepCompleted, []byte(`{"value":1}`), seq)
		if fault == "ok" {
			if appendErr != nil {
				return trace, appendErr
			}
		} else if !errors.Is(appendErr, journal.ErrUnknown) {
			return trace, fmt.Errorf("seed %d %s completion fault=%s: %v", seed, id, fault, appendErr)
		}
		if fault == "drop_before_commit" {
			completed, err = appendEntry(2, journal.StepCompleted, []byte(`{"value":1}`), seq)
			if err != nil {
				return trace, err
			}
		}
		if fault == "lose_ack_after_commit" {
			messages := journals.Messages(identity.JournalSubject("test", id))
			if len(messages) != 3 || messages[2].Sequence <= seq {
				return trace, fmt.Errorf("seed %d %s hidden completion not retained: %+v", seed, id, messages)
			}
			completed = messages[2].Sequence
		}
		terminal := []byte(fmt.Sprintf(`{"inv_seq":%d,"result":"done"}`, handle.InvSeq))
		if _, err := appendEntry(3, journal.Completed, terminal, completed); err != nil {
			return trace, err
		}
		kvFault, err := schedule.Choose([]string{"ok", "lose_ack_after_commit"})
		if err != nil {
			return trace, err
		}
		if kvFault != "ok" {
			if err := state.QueueFault(KVFault{Operation: "create", Kind: KVLoseAckAfterCommit}); err != nil {
				return trace, err
			}
		}
		stateErr := workerpkg.PersistOutcomeWithPort(ctx, state, "test", id, handle.InvSeq, terminal)
		if kvFault == "ok" && stateErr != nil || kvFault != "ok" && !errors.Is(stateErr, ErrTransportLost) {
			return trace, fmt.Errorf("seed %d %s terminal KV fault=%s: %v", seed, id, kvFault, stateErr)
		}
		if err := workerpkg.PersistOutcomeWithPort(ctx, state, "test", id, handle.InvSeq, terminal); err != nil {
			return trace, fmt.Errorf("seed %d %s terminal KV retry: %w", seed, id, err)
		}
		snapshot, err := retainedModelSnapshot(starts, journals, state)
		if err != nil {
			return trace, err
		}
		readRecords, readTail, err := j.Read(ctx, "test", id)
		wantRecords := snapshot.Journals[identity.JournalSubject("test", id)]
		if err != nil || !reflect.DeepEqual(readRecords, wantRecords) || readTail != wantRecords[len(wantRecords)-1].Sequence {
			return trace, fmt.Errorf("seed %d read %s: records=%d/%d tail=%d err=%v", seed, id, len(readRecords), len(wantRecords), readTail, err)
		}
		report, err := integrity.CheckSnapshot(snapshot)
		want := integrity.Report{Invocations: i + 1, Journals: i + 1, Entries: 4 * (i + 1), Terminal: i + 1}
		if err != nil || report != want {
			return trace, fmt.Errorf("seed %d after %s checker=%+v want=%+v err=%v", seed, id, report, want, err)
		}
		schedule.RecordTransport(TransportEvent{Operation: "check_invariants", Sequence: uint64(report.Terminal), Outcome: "ok", AtMillis: schedule.NowMillis()})
	}
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededRetainedInvariantChecks(t *testing.T) {
	if os.Getenv("SIM_RETAINED_INVARIANTS_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededRetainedInvariantChecks(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_RETAINED_INVARIANTS_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed := range seededSchedules(t) {
		generated, err := runSeededRetainedInvariantChecks(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-retained-invariants-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-retained-invariants.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runSeededRetainedInvariantChecks(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("retained-invariants-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededRetainedInvariantChecks$")
		cmd.Env = append(os.Environ(), "SIM_RETAINED_INVARIANTS_HELPER=1", "SIM_RETAINED_INVARIANTS_OUT="+files[i], "FAULT_SEED=42")
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
		t.Fatal("retained invariant trace changed across processes")
	}
}
