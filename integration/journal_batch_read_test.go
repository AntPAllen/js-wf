package integration_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"js-wf/journal"
	"js-wf/sim"
)

func TestJournalBatchedReadPreservesSubjectOrderAndDetectsGap(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	mainStore := journal.New(all[0])
	otherStore := journal.New(all[1])
	model := sim.NewJournalTransport(sim.NewScheduler(1))
	modelStore := journal.NewWithBatchReadPort(model, model, model)
	var mainTail, otherTail uint64
	var modelMainTail, modelOtherTail uint64
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
		entry := journal.Entry{Epoch: 1, Index: uint64(index), Kind: kind, WorkerID: "reader"}
		mainTail, err = mainStore.Append(ctx, "batch", "main", entry, mainTail)
		if err != nil {
			t.Fatalf("main append %d: %v", index, err)
		}
		modelMainTail, err = modelStore.Append(ctx, "batch", "main", entry, modelMainTail)
		if err != nil || modelMainTail != mainTail {
			t.Fatalf("model main append %d: seq=%d real=%d err=%v", index, modelMainTail, mainTail, err)
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
			otherEntry := journal.Entry{Epoch: 1, Index: otherIndex, Kind: otherKind, WorkerID: "reader"}
			otherTail, err = otherStore.Append(ctx, "batch", "other", otherEntry, otherTail)
			if err != nil {
				t.Fatalf("other append %d: %v", otherIndex, err)
			}
			modelOtherTail, err = modelStore.Append(ctx, "batch", "other", otherEntry, modelOtherTail)
			if err != nil || modelOtherTail != otherTail {
				t.Fatalf("model other append %d: seq=%d real=%d err=%v", otherIndex, modelOtherTail, otherTail, err)
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
	modelRecords, modelTail, modelErr := modelStore.Read(ctx, "batch", "main")
	if modelErr != nil || modelTail != tail || len(modelRecords) != len(records) {
		t.Fatalf("model batched read: records=%d tail=%d real=%d err=%v", len(modelRecords), modelTail, tail, modelErr)
	}
	for index, record := range records {
		if record.Index != uint64(index) || record.Epoch != 1 || record.WorkerID != "reader" ||
			index > 0 && record.Sequence <= records[index-1].Sequence {
			t.Fatalf("batched read record %d: %+v", index, record)
		}
		if !reflect.DeepEqual(modelRecords[index], record) {
			t.Fatalf("batch read model mismatch at %d: model=%+v real=%+v", index, modelRecords[index], record)
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
