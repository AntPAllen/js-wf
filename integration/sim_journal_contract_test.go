package integration_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"js-wf/identity"
	"js-wf/journal"
	"js-wf/sim"
)

// Keep the modeled journal sequence and CAS baseline tied to a real stream.
// Lost acknowledgments and unchanged-tail wrong-sequence replies have separate
// fault fixtures because a healthy server does not produce them on demand.
func TestSimJournalAppendContractAgainstRealCluster(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	real := journal.New(all[0])
	model := sim.NewJournalTransport(sim.NewScheduler(1))
	simulated := journal.NewWithAppendPort(model)
	const typ = "sim-contract"
	appendBoth := func(id string, entry journal.Entry, expected uint64) uint64 {
		t.Helper()
		gotReal, realErr := real.Append(ctx, typ, id, entry, expected)
		gotModel, modelErr := simulated.Append(ctx, typ, id, entry, expected)
		if gotReal != gotModel || realErr != nil || modelErr != nil {
			t.Fatalf("append %s index=%d: real=(%d,%v) model=(%d,%v)", id, entry.Index, gotReal, realErr, gotModel, modelErr)
		}
		return gotReal
	}
	first := appendBoth("one", journal.Entry{Kind: journal.Started, Index: 0, Epoch: 1}, 0)
	appendBoth("two", journal.Entry{Kind: journal.Started, Index: 0, Epoch: 1}, 0)
	appendBoth("one", journal.Entry{Kind: journal.StepRequested, Index: 1, Epoch: 2}, first)
	stale := journal.Entry{Kind: journal.StepRequested, Index: 1, Epoch: 1}
	if _, realErr := real.Append(ctx, typ, "one", stale, first); !errors.Is(realErr, journal.ErrStale) {
		t.Fatalf("real stale append: %v", realErr)
	}
	if _, modelErr := simulated.Append(ctx, typ, "one", stale, first); !errors.Is(modelErr, journal.ErrStale) {
		t.Fatalf("modeled stale append: %v", modelErr)
	}
	stream, err := all[1].Stream(ctx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"one", "two"} {
		subject := identity.JournalSubject(typ, id)
		modeled := model.Messages(subject)
		records, _, err := real.Read(ctx, typ, id)
		if err != nil || len(records) != len(modeled) {
			t.Fatalf("read %s: records=%d modeled=%d err=%v", id, len(records), len(modeled), err)
		}
		for _, message := range modeled {
			realMessage, err := stream.GetMsg(ctx, message.Sequence)
			if err != nil || realMessage.Subject != subject || !bytes.Equal(realMessage.Data, message.Data) {
				t.Fatalf("retained message %d for %s differs: err=%v", message.Sequence, id, err)
			}
		}
	}
}
