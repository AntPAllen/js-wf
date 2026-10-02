package sim

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"

	"js-wf/journal"
)

func runMixedJournalEncoding(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("journal_mixed_encoding"); err != nil {
		return trace, err
	}
	defer func() { trace = schedule.Trace() }()
	pattern, err := schedule.Choose([]string{"json_first", "protobuf_first"})
	if err != nil {
		return trace, err
	}
	fault, err := schedule.Choose([]string{string(DropBeforeCommit), string(LoseAckAfterCommit), string(RejectUnchanged)})
	if err != nil {
		return trace, err
	}
	port := NewJournalTransport(schedule)
	legacy, err := journal.NewWithEncodedPorts(port, port, journal.JSON)
	if err != nil {
		return trace, err
	}
	protobuf, err := journal.NewWithEncodedPorts(port, port, journal.ProtobufV1)
	if err != nil {
		return trace, err
	}
	ctx := context.Background()
	var tail uint64
	for i, kind := range []journal.Kind{journal.Started, journal.StepRequested, journal.StepCompleted, journal.Completed} {
		store := legacy
		if (i%2 == 1) == (pattern == "json_first") {
			store = protobuf
		}
		entry := journal.Entry{Epoch: 1, Index: uint64(i), Kind: kind, Payload: []byte(`null`)}
		if i > 0 {
			entry.Epoch = 2
		}
		if i == 1 {
			if err := port.QueueFault(Fault{Kind: AppendFault(fault)}); err != nil {
				return trace, err
			}
		}
		next, err := store.Append(ctx, "test", "mixed", entry, tail)
		if errors.Is(err, journal.ErrUnknown) {
			records, observed, readErr := legacy.Read(ctx, "test", "mixed")
			if readErr != nil {
				return trace, readErr
			}
			if len(records) == i+1 {
				next = observed
				err = nil
			} else {
				next, err = store.Append(ctx, "test", "mixed", entry, tail)
			}
		}
		if err != nil {
			return trace, err
		}
		tail = next
		if i == 1 {
			if _, err := legacy.Append(ctx, "test", "mixed", journal.Entry{Epoch: 1, Index: 2, Kind: journal.StepCompleted}, tail); !errors.Is(err, journal.ErrStale) {
				return trace, fmt.Errorf("stale owner accepted: %v", err)
			}
		}
	}
	for _, reader := range []*journal.Store{legacy, protobuf} {
		records, observed, err := reader.Read(ctx, "test", "mixed")
		if err != nil || len(records) != 4 || observed != tail {
			return trace, fmt.Errorf("mixed read: count%d tail%d err%v", len(records), observed, err)
		}
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_mixed_encoding", Subject: "wf.jrn.test.mixed", Outcome: pattern + "/" + fault, AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededMixedJournalEncodingReplay(t *testing.T) {
	if path := os.Getenv("SIM_JOURNAL_ENCODING_OUT"); path != "" {
		trace, err := runMixedJournalEncoding(42, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(path); err != nil {
			t.Fatal(err)
		}
		return
	}
	coverage := map[string]bool{}
	for seed := range seededSchedules(t) {
		generated, err := runMixedJournalEncoding(seed, nil)
		if err != nil {
			t.Fatalf("seed%d: %v", seed, err)
		}
		coverage[generated.Decisions[0].Chosen+"/"+generated.Decisions[1].Chosen] = true
		if seed <= 10 {
			replayed, err := runMixedJournalEncoding(0, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("replay seed%d: %v", seed, err)
			}
		}
	}
	if len(coverage) != 6 {
		t.Fatal("missing encoding/fault combinations", coverage)
	}
}
