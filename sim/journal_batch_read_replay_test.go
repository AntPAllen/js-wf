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
	"strconv"
	"testing"

	"js-wf/identity"
	"js-wf/journal"

	"github.com/nats-io/nats.go"
)

func runSeededJournalBatchRead(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("journal_batch_read_80"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"clean", "no_responders", "partial_no_responders", "partial_consumer_deleted", "replace_consumer", "persistent_no_responders", "deleted_middle"})
	if err != nil {
		return trace, err
	}
	ctx := context.Background()
	model := NewJournalTransport(schedule)
	store := journal.NewWithBatchReadPort(model, model, model)
	id := fmt.Sprintf("batch-%d", seed)
	const typ = "batch"
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
			return trace, fmt.Errorf("seed %d append %d: %w", seed, index, err)
		}
		expected = append(expected, journal.Record{Entry: entry, Sequence: tail})
		if index%4 == 0 {
			other := identity.JournalSubject(typ, "other")
			if _, err := model.Publish(ctx, other, []byte(`{"other":true}`), modelSubjectTail(model.Messages(other))); err != nil {
				return trace, err
			}
		}
	}
	switch mode {
	case "no_responders", "partial_no_responders", "partial_consumer_deleted", "replace_consumer", "persistent_no_responders":
		kind := "no_responders"
		after := 0
		if mode == "partial_no_responders" || mode == "partial_consumer_deleted" {
			after = 5
		}
		if mode == "partial_consumer_deleted" {
			kind = "consumer_deleted"
		}
		count := 1
		if mode == "replace_consumer" {
			count = 2
		}
		if mode == "persistent_no_responders" {
			count = 6
		}
		for i := 0; i < count; i++ {
			if err := model.QueueBatchFault(BatchReadFault{Kind: kind, After: after}); err != nil {
				return trace, err
			}
		}
	case "deleted_middle":
		if !model.DeleteMessage(subject, expected[70].Sequence) {
			return trace, fmt.Errorf("seed %d could not delete journal entry", seed)
		}
	}
	actual, gotTail, readErr := store.Read(ctx, typ, id)
	if mode == "persistent_no_responders" {
		if !errors.Is(readErr, nats.ErrNoResponders) || len(actual) != 0 || gotTail != 0 || schedule.NowMillis() != 250 {
			return trace, fmt.Errorf("seed %d persistent fault: entries=%d tail=%d time=%d err=%v", seed, len(actual), gotTail, schedule.NowMillis(), readErr)
		}
	} else if mode == "deleted_middle" {
		if !errors.Is(readErr, journal.ErrGap) || len(actual) != 0 || gotTail != 0 || schedule.NowMillis() != 2000 {
			return trace, fmt.Errorf("seed %d deleted entry: entries=%d tail=%d time=%d err=%v", seed, len(actual), gotTail, schedule.NowMillis(), readErr)
		}
	} else if readErr != nil || gotTail != tail || !reflect.DeepEqual(actual, expected) {
		return trace, fmt.Errorf("seed %d mode %s: entries=%d tail=%d want=%d err=%v", seed, mode, len(actual), gotTail, tail, readErr)
	}
	var opens, closes, waits int
	for _, event := range schedule.Trace().Transport {
		switch event.Operation {
		case "open_journal_batch":
			opens++
		case "close_journal_batch":
			closes++
		case "wait":
			waits++
		}
	}
	wantOpens, wantWaits := 1, 0
	switch mode {
	case "no_responders", "partial_no_responders", "partial_consumer_deleted":
		wantWaits = 1
	case "replace_consumer":
		wantOpens, wantWaits = 2, 2
	case "persistent_no_responders":
		wantOpens, wantWaits = 5, 5
	case "deleted_middle":
		wantOpens, wantWaits = 81, 80
	}
	if opens != wantOpens || closes != opens || waits != wantWaits {
		return trace, fmt.Errorf("seed %d mode %s: opens=%d closes=%d waits=%d", seed, mode, opens, closes, waits)
	}
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func modelSubjectTail(messages []Message) uint64 {
	if len(messages) == 0 {
		return 0
	}
	return messages[len(messages)-1].Sequence
}

func TestSeededJournalBatchReadReplay(t *testing.T) {
	if os.Getenv("SIM_JOURNAL_BATCH_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededJournalBatchRead(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_JOURNAL_BATCH_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		generated, err := runSeededJournalBatchRead(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "journal-batch-read-failure.json")
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runSeededJournalBatchRead(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d batch read replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("batch-read-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededJournalBatchReadReplay$")
		cmd.Env = append(os.Environ(), "SIM_JOURNAL_BATCH_HELPER=1", "SIM_JOURNAL_BATCH_OUT="+files[i], "FAULT_SEED=42")
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
		t.Fatal("journal batch read trace changed across processes")
	}
}
