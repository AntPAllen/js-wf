package sim

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	"js-wf/identity"
	"js-wf/journal"

	"github.com/nats-io/nats.go"
)

func runStalledJournalCursor(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("journal_stalled_cursor"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"timeout", "no_messages", "partial_timeout", "persistent_timeout"})
	if err != nil {
		return trace, err
	}
	ctx := context.Background()
	model := NewJournalTransport(schedule)
	store := journal.NewWithBatchReadPort(model, model, model)
	const typ, id = "batch", "stalled"
	subject := identity.JournalSubject(typ, id)
	var expected []journal.Record
	var tail uint64
	for index := 0; index < 80; index++ {
		kind := journal.StepRequested
		if index == 0 {
			kind = journal.Started
		}
		entry := journal.Entry{Epoch: 1, Index: uint64(index), Kind: kind, WorkerID: "reader"}
		tail, err = store.Append(ctx, typ, id, entry, tail)
		if err != nil {
			return trace, err
		}
		expected = append(expected, journal.Record{Entry: entry, Sequence: tail})
	}
	kind := "timeout"
	if mode == "no_messages" {
		kind = mode
	}
	count := 2
	if mode == "partial_timeout" {
		count = 3
	}
	if mode == "persistent_timeout" {
		count = 6
	}
	for i := 0; i < count; i++ {
		after := 0
		if mode == "partial_timeout" && i == 0 {
			after = 5
		}
		if err := model.QueueBatchFault(BatchReadFault{Kind: kind, After: after}); err != nil {
			return trace, err
		}
	}
	records, gotTail, readErr := store.Read(ctx, typ, id)
	wantOpens, wantWaits := 2, 2
	if mode == "persistent_timeout" {
		wantOpens, wantWaits = 5, 5
		if !errors.Is(readErr, nats.ErrTimeout) || len(records) != 0 || gotTail != 0 {
			return trace, fmt.Errorf("persistent stall: records=%d tail=%d err=%v", len(records), gotTail, readErr)
		}
	} else if readErr != nil || gotTail != tail || !reflect.DeepEqual(records, expected) {
		return trace, fmt.Errorf("mode %s: records=%d tail=%d want=%d err=%v", mode, len(records), gotTail, tail, readErr)
	}
	var opens, closes, waits int
	for _, event := range schedule.Trace().Transport {
		switch event.Operation {
		case "open_journal_batch":
			opens++
			if opens == 2 {
				wantSeq := expected[64].Sequence
				if mode == "partial_timeout" {
					wantSeq = expected[69].Sequence
				}
				if event.Subject != subject || event.Sequence != wantSeq {
					return trace, fmt.Errorf("replacement skips verified tail: sequence=%d want=%d", event.Sequence, wantSeq)
				}
			}
		case "close_journal_batch":
			closes++
		case "wait":
			waits++
		}
	}
	if opens != wantOpens || closes != opens || waits != wantWaits || schedule.NowMillis() != int64(wantWaits*50) {
		return trace, fmt.Errorf("mode %s: opens=%d closes=%d waits=%d time=%d", mode, opens, closes, waits, schedule.NowMillis())
	}
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededStalledJournalCursorReplay(t *testing.T) {
	if path := os.Getenv("SIM_JOURNAL_STALL_OUT"); path != "" {
		trace, err := runStalledJournalCursor(42, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(path); err != nil {
			t.Fatal(err)
		}
		return
	}
	modes := map[string]bool{}
	for seed := range seededSchedules(t) {
		generated, err := runStalledJournalCursor(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "journal-stalled-cursor-failure.json")
			}
			_ = generated.Save(path)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		modes[generated.Decisions[0].Chosen] = true
		if seed <= 10 {
			replayed, err := runStalledJournalCursor(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d replay: %v", seed, err)
			}
		}
	}
	if len(modes) != 4 {
		t.Fatalf("missing stalled cursor modes: %v", modes)
	}
	var previous []byte
	for i := 0; i < 2; i++ {
		path := filepath.Join(t.TempDir(), "journal-stalled-cursor.json")
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededStalledJournalCursorReplay$")
		cmd.Env = append(os.Environ(), "SIM_JOURNAL_STALL_OUT="+path)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child %d: %v: %s", i, err, output)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if i == 1 && !bytes.Equal(previous, data) {
			t.Fatal("stalled cursor trace changed across processes")
		}
		previous = data
	}
}
