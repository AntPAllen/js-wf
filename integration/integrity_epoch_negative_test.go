package integration_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"

	"github.com/nats-io/nats.go"
)

func TestIntegrityRejectsTwoWorkersInOneEpoch(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := all[0].PublishMsg(ctx, &nats.Msg{Subject: identity.InvocationSubject("test", "shared-epoch"), Data: []byte(`null`)}); err != nil {
		t.Fatal(err)
	}
	j := journal.New(all[0])
	seq, err := j.Append(ctx, "test", "shared-epoch", journal.Entry{Index: 0, Kind: journal.Started}, 0)
	if err != nil {
		t.Fatal(err)
	}
	seq, err = j.Append(ctx, "test", "shared-epoch", journal.Entry{Epoch: 1, Index: 1, Kind: journal.StepRequested, WorkerID: "worker-a"}, seq)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.Append(ctx, "test", "shared-epoch", journal.Entry{Epoch: 1, Index: 2, Kind: journal.StepCompleted, WorkerID: "worker-b"}, seq); err != nil {
		t.Fatal(err)
	}
	if _, err := integrity.Check(ctx, all[1]); err == nil || !strings.Contains(err.Error(), "epoch 1 used by workers") {
		t.Fatalf("shared epoch survived real-state checker: %v", err)
	}
}

func TestIntegrityRejectsInvalidEntryKindsAndUnresolvedSuccess(t *testing.T) {
	for _, test := range []struct {
		name     string
		middle   journal.Kind
		terminal journal.Kind
		want     string
	}{
		{"unknown-kind", "Unknown", journal.Completed, "unknown entry kind"},
		{"unresolved-success", journal.Attempt, journal.Completed, "successful terminal with unresolved request"},
		{"failed-pending-step", journal.Attempt, journal.Failed, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			all, _ := setup(t)
			ctx, stop := context.WithTimeout(context.Background(), 15*time.Second)
			defer stop()
			if _, err := all[0].Publish(ctx, identity.InvocationSubject("test", test.name), []byte(`null`)); err != nil {
				t.Fatal(err)
			}
			store := journal.New(all[0])
			var tail uint64
			entries := []journal.Entry{
				{Kind: journal.Started},
				{Kind: journal.StepRequested},
				{Kind: test.middle, Payload: json.RawMessage(`{"count":1,"error":"panic"}`)},
				{Kind: test.terminal, Payload: json.RawMessage(`"result"`)},
			}
			for i, entry := range entries {
				entry.Index, entry.Epoch, entry.WorkerID = uint64(i), 1, "worker-a"
				var err error
				tail, err = store.Append(ctx, "test", test.name, entry, tail)
				if err != nil {
					t.Fatal(err)
				}
			}
			state, err := all[0].KeyValue(ctx, "WF_STATE")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := state.Put(ctx, identity.Key("test", test.name), []byte(`"result"`)); err != nil {
				t.Fatal(err)
			}
			report, err := integrity.Check(ctx, all[1])
			if test.want == "" {
				if err != nil || report != (integrity.Report{Invocations: 1, Journals: 1, Entries: 4, Terminal: 1}) {
					t.Fatalf("valid failed terminal: %+v %v", report, err)
				}
			} else if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("mutation survived real-state checker: %v; want %q", err, test.want)
			}
		})
	}
}
