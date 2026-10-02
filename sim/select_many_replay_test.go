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
	"time"

	"js-wf/journal"
	"js-wf/wf"
)

func runSeededSelectMany(seed int64, replay *Trace) (trace Trace, runErr error) {
	var schedule *Scheduler
	if replay == nil {
		schedule = NewScheduler(seed)
	} else {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	if err := schedule.SetWorkload("select_many"); err != nil {
		return trace, err
	}
	defer func() { trace = schedule.Trace() }()
	priority, err := schedule.Choose([]string{"signal", "timer", "promise"})
	if err != nil {
		return trace, err
	}
	readiness, err := schedule.Choose([]string{"all", "last_only", "none"})
	if err != nil {
		return trace, err
	}
	fault, err := schedule.Choose([]string{"clean", "drop_completion", "lose_completion_ack"})
	if err != nil {
		return trace, err
	}
	ctx := context.Background()
	model := NewJournalTransport(schedule)
	port := &faultingExecutionJournal{JournalTransport: model}
	store := journal.NewWithPorts(port, model)
	tail, err := store.Append(ctx, "test", "select", journal.Entry{Kind: journal.Started}, 0)
	if err != nil {
		return trace, err
	}
	nextIndex := uint64(1)
	appendStep := func(ctx context.Context, kind wf.Kind, payload json.RawMessage) error {
		next, err := store.Append(ctx, "test", "select", journal.Entry{Kind: journal.Kind(kind), Epoch: 1, Index: nextIndex, Payload: payload}, tail)
		if err == nil {
			tail = next
			nextIndex++
		}
		return err
	}
	base := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	outcome, _ := json.Marshal(wf.Outcome{Result: []byte(`"promise"`)})
	allSignals := []wf.Signal{{Sequence: 3, Name: "go", Payload: []byte(`"signal"`)}, {Sequence: 7, Name: "child_0", Payload: outcome}}
	kinds := []string{priority}
	for _, kind := range []string{"signal", "timer", "promise"} {
		if kind != priority {
			kinds = append(kinds, kind)
		}
	}
	build := func(entries []wf.Entry, ready string) (*wf.Context, []wf.Awaitable, error) {
		var signals []wf.Signal
		wakeup := base
		if ready == "all" {
			signals = allSignals
			wakeup = base.Add(2 * time.Second)
		} else if ready == "last_only" {
			switch kinds[2] {
			case "timer":
				wakeup = base.Add(2 * time.Second)
			case "signal":
				signals = allSignals[:1]
			case "promise":
				signals = allSignals[1:]
			}
		}
		c := wf.NewContext(ctx, entries, appendStep, signals...)
		c.SetTimerSupport(wakeup, func(context.Context) (time.Time, error) {
			return base.Add(time.Duration(schedule.NowMillis()) * time.Millisecond), nil
		}, func(context.Context, uint64, time.Time) error { return nil })
		timer, err := c.Timer("deadline", time.Second)
		if err != nil {
			return nil, nil, err
		}
		cases := make([]wf.Awaitable, 3)
		for i, kind := range kinds {
			switch kind {
			case "signal":
				cases[i] = wf.SignalAwaitable("go")
			case "promise":
				cases[i] = wf.Promise{SignalName: "child_0"}
			case "timer":
				cases[i] = timer
			}
		}
		return c, cases, nil
	}
	// Create the timer before advancing the delivery clock or exposing events.
	_, _, err = build(nil, "none")
	if err != nil {
		return trace, err
	}
	initial, _, err := store.Read(ctx, "test", "select")
	if err != nil {
		return trace, err
	}
	var initialEntries []wf.Entry
	for _, record := range initial[1:] {
		initialEntries = append(initialEntries, wf.Entry{Index: record.Index, Kind: wf.Kind(record.Kind), Payload: record.Payload})
	}
	if readiness == "all" || readiness == "last_only" && kinds[2] == "timer" {
		if err := model.Wait(ctx, 2*time.Second); err != nil {
			return trace, err
		}
	}
	c, cases, err := build(initialEntries, readiness)
	if err != nil {
		return trace, err
	}
	port.subject = "wf.jrn.test.select"
	if fault == "drop_completion" {
		port.fault = DropBeforeCommit
	} else if fault == "lose_completion_ack" {
		port.fault = LoseAckAfterCommit
	}
	firstIndex, _, firstErr := wf.Select(c, cases...)
	if firstErr != nil && !errors.Is(firstErr, wf.ErrSuspended) && !errors.Is(firstErr, journal.ErrUnknown) {
		return trace, firstErr
	}
	if readiness == "none" && !errors.Is(firstErr, wf.ErrSuspended) {
		return trace, fmt.Errorf("unready select completed")
	}
	// Rebuild after suspension/unknown commit. All cases are now ready, but a
	// committed completion must preserve its former winner, even if it was last.
	if remaining := int64(2000) - schedule.NowMillis(); remaining > 0 {
		if err := model.Wait(ctx, time.Duration(remaining)*time.Millisecond); err != nil {
			return trace, err
		}
	}
	for attempt := 0; attempt < 3; attempt++ {
		records, last, err := store.Read(ctx, "test", "select")
		if err != nil {
			return trace, err
		}
		tail = last
		nextIndex = uint64(len(records))
		var entries []wf.Entry
		for _, r := range records[1:] {
			entries = append(entries, wf.Entry{Index: r.Index, Kind: wf.Kind(r.Kind), Payload: r.Payload})
		}
		expected := 0
		if len(records) == 5 {
			var done struct {
				CaseIndex *int `json:"case_index"`
			}
			if err := json.Unmarshal(records[4].Payload, &done); err != nil || done.CaseIndex == nil {
				return trace, fmt.Errorf("invalid recorded winner")
			}
			expected = *done.CaseIndex
		}
		c, cases, err = build(entries, "all")
		if err != nil {
			return trace, err
		}
		selected, result, err := wf.Select(c, cases...)
		if errors.Is(err, journal.ErrUnknown) {
			continue
		}
		if err != nil {
			return trace, err
		}
		if selected != expected {
			return trace, fmt.Errorf("winner changed from %d to %d (initial %d)", expected, selected, firstIndex)
		}
		want := []byte(fmt.Sprintf("%q", kinds[selected]))
		if kinds[selected] == "timer" {
			want = nil
		}
		if !bytes.Equal(result, want) {
			return trace, fmt.Errorf("selected result=%s want=%s", result, want)
		}
		if kinds[selected] == "promise" {
			again, err := wf.AwaitPromise(c, wf.Promise{SignalName: "child_0"})
			if err != nil || !bytes.Equal(again, want) {
				return trace, fmt.Errorf("selected promise reuse: %v", err)
			}
		}
		if err := c.CheckComplete(); err != nil {
			return trace, err
		}
		records, _, err = store.Read(ctx, "test", "select")
		if err != nil || len(records) != 5 {
			return trace, fmt.Errorf("select retained records=%d err=%v", len(records), err)
		}
		schedule.RecordTransport(TransportEvent{Operation: "check_select_many", Outcome: kinds[selected], AtMillis: schedule.NowMillis()})
		if err := schedule.Finish(); err != nil {
			return trace, err
		}
		return schedule.Trace(), nil
	}
	return trace, fmt.Errorf("select did not resolve within recovery bound")
}

func TestSeededSelectManyReplay(t *testing.T) {
	if os.Getenv("SIM_SELECT_MANY_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededSelectMany(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_SELECT_MANY_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed := range seededSchedules(t) {
		generated, err := runSeededSelectMany(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-select-many-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-select-many.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runSeededSelectMany(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d select many replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("select-many-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededSelectManyReplay$")
		cmd.Env = append(os.Environ(), "SIM_SELECT_MANY_HELPER=1", "SIM_SELECT_MANY_OUT="+files[i], "FAULT_SEED=42")
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
		t.Fatal("select many trace changed across processes")
	}
}
