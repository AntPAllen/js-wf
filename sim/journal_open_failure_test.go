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

// modeledBatchOpenFailure supplies bounded creation outcomes without using wall
// time. The real adapter bounds each attempt separately from the logical read.
type modeledBatchOpenFailure struct {
	*JournalTransport
	schedule  *Scheduler
	failure   error
	remaining int
	attempts  int
	starts    []uint64
}

func (p *modeledBatchOpenFailure) Open(ctx context.Context, subject string, sequence uint64) (journal.BatchReadCursor, error) {
	p.attempts++
	p.starts = append(p.starts, sequence)
	if p.remaining > 0 {
		p.remaining--
		if err := p.schedule.AdvanceMillis(3000); err != nil {
			return nil, err
		}
		p.JournalTransport.event(TransportEvent{Operation: "open_journal_batch_failure", Subject: subject, Sequence: sequence, Outcome: p.failure.Error()})
		return nil, p.failure
	}
	return p.JournalTransport.Open(ctx, subject, sequence)
}
func runJournalOpenFailure(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("journal_open_failure"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"deadline", "no_responders", "persistent_deadline", "semantic_error"})
	if err != nil {
		return trace, err
	}
	ctx := context.Background()
	model := NewJournalTransport(schedule)
	port := &modeledBatchOpenFailure{JournalTransport: model, schedule: schedule, failure: context.DeadlineExceeded, remaining: 1}
	if mode == "no_responders" {
		port.failure = nats.ErrNoResponders
	}
	if mode == "persistent_deadline" {
		port.remaining = 3
	}
	store := journal.NewWithBatchReadPort(model, model, port)
	const typ, id = "batch", "open-failure"
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
	// Use a semantic error outside ErrGap: Read legitimately retries ErrGap for
	// concurrent compaction, whereas consumer creation must not retry a config error.
	if mode == "semantic_error" {
		port.failure = errors.New("consumer config rejected")
	}
	records, gotTail, readErr := store.Read(ctx, typ, id)
	wantAttempts := 2
	var wantElapsed int64 = 3050
	if mode == "persistent_deadline" {
		wantAttempts, wantElapsed = 3, 9100
		if !errors.Is(readErr, context.DeadlineExceeded) || len(records) != 0 || gotTail != 0 {
			return trace, fmt.Errorf("persistent open: records=%d tail=%d err=%v", len(records), gotTail, readErr)
		}
	} else if mode == "semantic_error" {
		wantAttempts, wantElapsed = 1, 3000
		if !errors.Is(readErr, port.failure) || len(records) != 0 || gotTail != 0 {
			return trace, fmt.Errorf("semantic open: %v", readErr)
		}
	} else if readErr != nil || gotTail != tail || !reflect.DeepEqual(records, expected) {
		return trace, fmt.Errorf("recovered open: records=%d tail=%d err=%v", len(records), gotTail, readErr)
	}
	if port.attempts != wantAttempts || schedule.NowMillis() != wantElapsed {
		return trace, fmt.Errorf("open bound: attempts=%d want=%d time=%d want=%d", port.attempts, wantAttempts, schedule.NowMillis(), wantElapsed)
	}
	for _, start := range port.starts {
		if start != expected[64].Sequence {
			return trace, fmt.Errorf("open skips verified prefix: start=%d subject=%s", start, subject)
		}
	}
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}
func TestSeededJournalOpenFailureReplay(t *testing.T) {
	if path := os.Getenv("SIM_JOURNAL_OPEN_OUT"); path != "" {
		trace, err := runJournalOpenFailure(42, nil)
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
		generated, err := runJournalOpenFailure(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "journal-open-failure-failure.json")
			}
			_ = generated.Save(path)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		modes[generated.Decisions[0].Chosen] = true
		if seed <= 10 {
			replayed, err := runJournalOpenFailure(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d replay: %v", seed, err)
			}
		}
	}
	if len(modes) != 4 {
		t.Fatalf("missing consumer open modes: %v", modes)
	}
	var previous []byte
	for i := 0; i < 2; i++ {
		path := filepath.Join(t.TempDir(), "journal-open-failure.json")
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededJournalOpenFailureReplay$")
		cmd.Env = append(os.Environ(), "SIM_JOURNAL_OPEN_OUT="+path)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child %d: %v: %s", i, err, output)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if i == 1 && !bytes.Equal(previous, data) {
			t.Fatal("consumer open trace changed across processes")
		}
		previous = data
	}
}
