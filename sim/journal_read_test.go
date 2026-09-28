package sim

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"js-wf/identity"
	"js-wf/journal"
)

func TestModeledJournalReadKeepsGlobalHolesAndRejectsIndexGap(t *testing.T) {
	ctx := context.Background()
	schedule := NewScheduler(42)
	transport := NewJournalTransport(schedule)
	store := journal.NewWithPorts(transport, transport)
	first, err := store.Append(ctx, "test", "valid", journal.Entry{Kind: journal.Started, Index: 0}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, "test", "other", journal.Entry{Kind: journal.Started, Index: 0}, 0); err != nil {
		t.Fatal(err)
	}
	last, err := store.Append(ctx, "test", "valid", journal.Entry{Kind: journal.StepRequested, Index: 1, Payload: []byte(`{"kind":"effect","name":"step"}`)}, first)
	if err != nil {
		t.Fatal(err)
	}
	records, tail, err := store.Read(ctx, "test", "valid")
	if err != nil || len(records) != 2 || records[0].Sequence != first || records[1].Sequence != last || last != first+2 || tail != last {
		t.Fatalf("global sequence hole: records=%+v tail=%d err=%v", records, tail, err)
	}
	badSubject := identity.JournalSubject("test", "bad")
	for _, entry := range []journal.Entry{{Kind: journal.Started, Index: 0}, {Kind: journal.StepRequested, Index: 2}} {
		data, err := json.Marshal(entry)
		if err != nil {
			t.Fatal(err)
		}
		transport.mu.Lock()
		transport.commit(badSubject, data)
		transport.mu.Unlock()
	}
	if _, _, err := store.Read(ctx, "test", "bad"); !errors.Is(err, journal.ErrGap) || schedule.NowMillis() != 2000 {
		t.Fatalf("index gap err=%v virtual_ms=%d", err, schedule.NowMillis())
	}
}
