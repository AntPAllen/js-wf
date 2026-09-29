package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"js-wf/journal"
)

func TestJournalBatchedReadPreservesSubjectOrderAndDetectsGap(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	mainStore := journal.New(all[0])
	otherStore := journal.New(all[1])
	var mainTail, otherTail uint64
	var deletedSeq uint64
	for index := 0; index < 300; index++ {
		kind := journal.Started
		if index > 0 {
			kind = journal.StepRequested
			if index%2 == 0 {
				kind = journal.StepCompleted
			}
		}
		var err error
		mainTail, err = mainStore.Append(ctx, "batch", "main", journal.Entry{Epoch: 1, Index: uint64(index), Kind: kind, WorkerID: "reader"}, mainTail)
		if err != nil {
			t.Fatalf("main append %d: %v", index, err)
		}
		if index == 150 {
			deletedSeq = mainTail
		}
		if index%4 == 0 {
			otherIndex := uint64(index / 4)
			otherKind := journal.StepRequested
			if otherIndex == 0 {
				otherKind = journal.Started
			}
			otherTail, err = otherStore.Append(ctx, "batch", "other", journal.Entry{Epoch: 1, Index: otherIndex, Kind: otherKind, WorkerID: "reader"}, otherTail)
			if err != nil {
				t.Fatalf("other append %d: %v", otherIndex, err)
			}
		}
	}
	readCtx, stopRead := context.WithTimeout(ctx, 15*time.Second)
	readStarted := time.Now()
	records, tail, err := journal.New(all[2]).Read(readCtx, "batch", "main")
	stopRead()
	if err != nil || len(records) != 300 || tail != mainTail {
		t.Fatalf("batched read: records=%d tail=%d want=%d err=%v", len(records), tail, mainTail, err)
	}
	for index, record := range records {
		if record.Index != uint64(index) || record.Epoch != 1 || record.WorkerID != "reader" ||
			index > 0 && record.Sequence <= records[index-1].Sequence {
			t.Fatalf("batched read record %d: %+v", index, record)
		}
	}
	t.Logf("batched read 300 entries across 75 unrelated stream messages in %s", time.Since(readStarted))
	stream, err := all[0].Stream(ctx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.DeleteMsg(ctx, deletedSeq); err != nil {
		t.Fatal(err)
	}
	if _, _, err := journal.New(all[2]).Read(ctx, "batch", "main"); !errors.Is(err, journal.ErrGap) {
		t.Fatalf("batched read accepted deleted middle entry: %v", err)
	}
}
