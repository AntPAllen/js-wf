package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"js-wf/journal"
)

// Run with WF_JOURNAL_BOUNDARY=1. This fills one real three-replica journal
// through its final allowed index, then checks the rejection and full read.
func TestJournalMaxEntriesBoundary(t *testing.T) {
	if os.Getenv("WF_JOURNAL_BOUNDARY") == "" {
		t.Skip("set WF_JOURNAL_BOUNDARY=1 for the 100,000-entry journal boundary proof")
	}
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	store := journal.New(all[0])
	var seq uint64
	start := time.Now()
	for index := 0; index < journal.MaxEntries; index++ {
		kind := journal.Started
		if index > 0 {
			kind = journal.StepRequested
			if index%2 == 0 {
				kind = journal.StepCompleted
			}
		}
		next, err := store.Append(ctx, "boundary", "max", journal.Entry{Epoch: 1, Index: uint64(index), Kind: kind, WorkerID: "boundary"}, seq)
		if err != nil {
			t.Fatalf("append index %d after %s: %v", index, time.Since(start), err)
		}
		if next <= seq {
			t.Fatalf("non-increasing stream sequence at index %d: %d -> %d", index, seq, next)
		}
		seq = next
		if (index+1)%25000 == 0 {
			t.Logf("appended %d entries in %s", index+1, time.Since(start))
		}
	}
	writeTime := time.Since(start)
	if next, err := store.Append(ctx, "boundary", "max", journal.Entry{Epoch: 1, Index: journal.MaxEntries, Kind: journal.StepCompleted, WorkerID: "boundary"}, seq); next != 0 || !errors.Is(err, journal.ErrTooLong) {
		t.Fatalf("entry past boundary: seq=%d err=%v", next, err)
	}
	stream, err := all[1].Stream(ctx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx)
	if err != nil || info.State.Msgs != journal.MaxEntries || info.State.NumSubjects != 1 || info.State.FirstSeq != 1 || info.State.LastSeq != seq {
		t.Fatalf("retained journal after rejection: info=%+v err=%v", info, err)
	}
	last, err := stream.GetLastMsgForSubject(ctx, "wf.jrn.boundary.max")
	if err != nil {
		t.Fatal(err)
	}
	var lastEntry journal.Entry
	if err := json.Unmarshal(last.Data, &lastEntry); err != nil || last.Sequence != seq || lastEntry.Index != journal.MaxEntries-1 {
		t.Fatalf("last retained entry: seq=%d entry=%+v err=%v", last.Sequence, lastEntry, err)
	}
	readStart := time.Now()
	records, tail, err := journal.New(all[2]).Read(ctx, "boundary", "max")
	if err != nil || len(records) != journal.MaxEntries || tail != seq {
		t.Fatalf("full journal read: records=%d tail=%d want=%d err=%v", len(records), tail, seq, err)
	}
	for index, record := range records {
		if record.Index != uint64(index) || record.Epoch != 1 || record.WorkerID != "boundary" || record.Sequence != uint64(index+1) {
			t.Fatalf("entry %d after boundary read: %+v", index, record)
		}
	}
	for node, js := range all {
		view, err := js.Stream(ctx, "WF_JRN")
		if err != nil {
			t.Fatal(err)
		}
		state, err := view.Info(ctx)
		if err != nil || state.State.Msgs != journal.MaxEntries || state.State.FirstSeq != 1 || state.State.LastSeq != seq {
			t.Fatalf("node %d boundary state: info=%+v err=%v", node, state, err)
		}
	}
	t.Logf("journal entries=%d append_time=%s read_time=%s", journal.MaxEntries, writeTime, time.Since(readStart))
}
