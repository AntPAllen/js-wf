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
	"js-wf/wf"
	"js-wf/worker"
)

func runSeededWorkflowReplay(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("workflow_replay_5"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	ctx := context.Background()
	starts := NewStartTransport(schedule)
	journals := NewJournalTransport(schedule)
	state := NewKVTransport(schedule, 0)
	client := client.NewWithStartPort(starts)
	store := journal.NewWithPorts(journals, journals)
	for caseIndex := 0; caseIndex < 5; caseIndex++ {
		id := fmt.Sprintf("replay-%02d", caseIndex)
		handle, err := client.Start(ctx, "test", id, []byte(`null`))
		if err != nil || handle.InvSeq == 0 {
			return trace, fmt.Errorf("seed %d start %s: handle=%+v err=%v", seed, id, handle, err)
		}
		first, err := store.Append(ctx, "test", id, journal.Entry{Index: 0, Kind: journal.Started}, 0)
		if err != nil {
			return trace, err
		}
		choice, err := schedule.Choose([]string{"1", "2", "3", "4"})
		if err != nil {
			return trace, err
		}
		base, _ := strconv.Atoi(choice)
		var effects int
		handler := func(c *wf.Context, changedName, changedInput bool) (int, error) {
			total := 0
			for step := 0; step < 3; step++ {
				name := fmt.Sprintf("step-%d", step)
				input := base + step
				if step == 1 && changedName {
					name = "renamed-step"
				}
				if step == 1 && changedInput {
					input++
				}
				value, err := wf.Run(c, name, input, func(context.Context) (int, error) {
					effects++
					return input * 2, nil
				})
				if err != nil {
					return 0, err
				}
				total += value
			}
			return total, nil
		}
		tail, index := first, uint64(1)
		appendStep := func(ctx context.Context, kind wf.Kind, payload json.RawMessage) error {
			next, err := store.Append(ctx, "test", id, journal.Entry{Epoch: 1, Index: index, Kind: journal.Kind(kind), Payload: payload, WorkerID: "worker-a"}, tail)
			if err == nil {
				tail, index = next, index+1
			}
			return err
		}
		result, err := handler(wf.NewContext(ctx, nil, appendStep), false, false)
		if err != nil || effects != 3 || index != 7 {
			return trace, fmt.Errorf("seed %d handler %s result=%d effects=%d next_index=%d err=%v", seed, id, result, effects, index, err)
		}
		terminal := []byte(fmt.Sprintf(`{"inv_seq":%d,"result":%d}`, handle.InvSeq, result))
		if _, err := store.Append(ctx, "test", id, journal.Entry{Epoch: 1, Index: index, Kind: journal.Completed, Payload: terminal, WorkerID: "worker-a"}, tail); err != nil {
			return trace, err
		}
		if err := worker.PersistOutcomeWithPort(ctx, state, "test", id, handle.InvSeq, terminal); err != nil {
			return trace, err
		}
		snapshot, err := retainedModelSnapshot(starts, journals, state)
		if err != nil {
			return trace, err
		}
		readRecords, readTail, err := store.Read(ctx, "test", id)
		wantRecords := snapshot.Journals[identity.JournalSubject("test", id)]
		if err != nil || !reflect.DeepEqual(readRecords, wantRecords) || readTail != wantRecords[len(wantRecords)-1].Sequence {
			return trace, fmt.Errorf("seed %d read %s: records=%d/%d tail=%d err=%v", seed, id, len(readRecords), len(wantRecords), readTail, err)
		}
		report, err := integrity.CheckSnapshot(snapshot)
		want := integrity.Report{Invocations: caseIndex + 1, Journals: caseIndex + 1, Entries: 8 * (caseIndex + 1), Terminal: caseIndex + 1}
		if err != nil || report != want {
			return trace, fmt.Errorf("seed %d checker after %s report=%+v want=%+v err=%v", seed, id, report, want, err)
		}
		schedule.RecordTransport(TransportEvent{Operation: "check_invariants", Subject: identity.JournalSubject("test", id), Sequence: uint64(report.Terminal), Outcome: "ok", AtMillis: schedule.NowMillis()})
		journalBytes, err := json.Marshal(snapshot.Journals[identity.JournalSubject("test", id)])
		if err != nil {
			return trace, err
		}
		replayed, err := wf.Replay(journalBytes, func(c *wf.Context) (int, error) { return handler(c, false, false) })
		if err != nil || replayed != result || effects != 3 {
			return trace, fmt.Errorf("seed %d replay %s result=%d want=%d effects=%d err=%v", seed, id, replayed, result, effects, err)
		}
		for _, mutation := range []struct {
			name  string
			input bool
		}{
			{"step_name", false},
			{"step_input", true},
		} {
			_, err := wf.Replay(journalBytes, func(c *wf.Context) (int, error) {
				return handler(c, !mutation.input, mutation.input)
			})
			if !errors.Is(err, wf.ErrNonDeterministic) || effects != 3 {
				return trace, fmt.Errorf("seed %d mutation %s %s escaped guard: effects=%d err=%v", seed, id, mutation.name, effects, err)
			}
		}
		schedule.RecordTransport(TransportEvent{Operation: "check_determinism", Subject: identity.JournalSubject("test", id), Outcome: "ok", AtMillis: schedule.NowMillis()})
	}
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededWorkflowReplay(t *testing.T) {
	if os.Getenv("SIM_WORKFLOW_REPLAY_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededWorkflowReplay(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_WORKFLOW_REPLAY_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed := int64(1); seed <= 1000; seed++ {
		generated, err := runSeededWorkflowReplay(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-workflow-replay-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-workflow-replay.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runSeededWorkflowReplay(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("workflow-replay-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededWorkflowReplay$")
		cmd.Env = append(os.Environ(), "SIM_WORKFLOW_REPLAY_HELPER=1", "SIM_WORKFLOW_REPLAY_OUT="+files[i], "FAULT_SEED=42")
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
		t.Fatal("workflow replay trace changed across processes")
	}
}
