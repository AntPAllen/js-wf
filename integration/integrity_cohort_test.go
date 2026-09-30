package integration_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
)

func TestRetainedCohortExcludesLaterWorkAndDetectsLaterCorruption(t *testing.T) {
	all, _ := setup(t)
	js := all[0]
	ctx, stop := context.WithTimeout(context.Background(), 30*time.Second)
	defer stop()
	c := client.New(js)
	handle, err := c.Start(ctx, "cohort", "finished", []byte(`null`))
	if err != nil {
		t.Fatal(err)
	}
	store := journal.New(js)
	tail, err := store.Append(ctx, "cohort", "finished", journal.Entry{Kind: journal.Started, Epoch: 1}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, "cohort", "finished", journal.Entry{Kind: journal.Completed, Epoch: 1, Index: 1, Payload: json.RawMessage(`42`)}, tail); err != nil {
		t.Fatal(err)
	}
	state, err := js.KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.Put(ctx, identity.Key("cohort", "finished"), []byte(`42`)); err != nil {
		t.Fatal(err)
	}
	// Later retained work must not expand or invalidate the checkpoint cohort.
	if _, err := c.Start(ctx, "cohort", "later", []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, "cohort", "later", journal.Entry{Kind: journal.Started, Epoch: 1}, 0); err != nil {
		t.Fatal(err)
	}
	// Deliberately orphan a later journal. A cohort audit must leave it to the
	// mandatory global audit, which must still reject it.
	data, _ := json.Marshal(journal.Entry{Kind: journal.Started, Epoch: 1})
	if _, err := js.Publish(ctx, identity.JournalSubject("cohort", "orphan"), data); err != nil {
		t.Fatal(err)
	}
	report, err := integrity.CheckThroughInvocationSequence(ctx, js, handle.InvSeq)
	if err != nil || report != (integrity.Report{Invocations: 1, Journals: 1, Entries: 2, Terminal: 1}) {
		t.Fatalf("cohort=%+v err=%v", report, err)
	}
	if _, err := integrity.Check(ctx, js); err == nil || !strings.Contains(err.Error(), "invocation") {
		t.Fatalf("global audit accepted orphan: %v", err)
	}
	// Corruption written after capturing the cutoff is still inside the cohort.
	data, _ = json.Marshal(journal.Entry{Kind: journal.Started, Epoch: 1, Index: 2})
	if _, err := js.Publish(ctx, identity.JournalSubject("cohort", "finished"), data); err != nil {
		t.Fatal(err)
	}
	if _, err := integrity.CheckThroughInvocationSequence(ctx, js, handle.InvSeq); err == nil {
		t.Fatal("cohort audit ignored post-cutoff journal corruption")
	}
}
