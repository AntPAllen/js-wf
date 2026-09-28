package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"js-wf/journal"

	"github.com/nats-io/nats.go/jetstream"
)

// This deliberately applies a small byte quota after provisioning. Normal
// provisioning leaves WF_JRN unlimited; with a quota, DiscardNew must reject
// appends rather than evicting another live invocation's journal prefix.
func TestJournalDiscardNewPreservesLivePrefixAtByteLimit(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := all[0].Stream(ctx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	config := info.Config
	if config.Discard != jetstream.DiscardNew || config.Replicas != 3 || config.Storage != jetstream.FileStorage {
		t.Fatalf("unexpected journal config before pressure: %+v", config)
	}
	config.MaxBytes = 32 * 1024
	if _, err := all[0].UpdateStream(ctx, config); err != nil {
		t.Fatal(err)
	}
	store := journal.New(all[1])
	first, err := store.Append(ctx, "pressure", "first", journal.Entry{Epoch: 1, Index: 0, Kind: journal.Started}, 0)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Append(ctx, "pressure", "second", journal.Entry{Epoch: 1, Index: 0, Kind: journal.Started}, 0)
	if err != nil {
		t.Fatal(err)
	}
	seq := first
	accepted := 0
	payload, _ := json.Marshal(strings.Repeat("p", 1024))
	var rejection error
	for index := 1; index <= 1000; index++ {
		kind := journal.StepRequested
		if index%2 == 0 {
			kind = journal.StepCompleted
		}
		next, appendErr := store.Append(ctx, "pressure", "first", journal.Entry{Epoch: 1, Index: uint64(index), Kind: kind, Payload: payload, WorkerID: "pressure"}, seq)
		if appendErr != nil {
			rejection = appendErr
			break
		}
		seq = next
		accepted++
	}
	if rejection == nil || accepted == 0 || errors.Is(rejection, journal.ErrUnknown) || errors.Is(rejection, journal.ErrStale) {
		t.Fatalf("quota did not explicitly reject new append: accepted=%d err=%v", accepted, rejection)
	}
	var api *jetstream.APIError
	if !errors.As(rejection, &api) {
		t.Fatalf("quota rejection has no API error: %v", rejection)
	}
	for node, js := range all {
		view, err := js.Stream(ctx, "WF_JRN")
		if err != nil {
			t.Fatal(err)
		}
		state, err := view.Info(ctx)
		if err != nil || state.Config.Discard != jetstream.DiscardNew || state.Config.MaxBytes != 32*1024 || state.State.FirstSeq != first || state.State.Msgs != uint64(accepted+2) || state.State.NumSubjects != 2 {
			t.Fatalf("node %d journal state after quota: info=%+v err=%v", node, state, err)
		}
		if _, err := view.GetMsg(ctx, first); err != nil {
			t.Fatalf("node %d first journal entry evicted: %v", node, err)
		}
		if _, err := view.GetMsg(ctx, second); err != nil {
			t.Fatalf("node %d other invocation evicted: %v", node, err)
		}
	}
	records, tail, err := journal.New(all[2]).Read(ctx, "pressure", "first")
	if err != nil || len(records) != accepted+1 || tail != seq {
		t.Fatalf("live journal after quota: records=%d tail=%d want_tail=%d err=%v", len(records), tail, seq, err)
	}
	other, otherTail, err := journal.New(all[2]).Read(ctx, "pressure", "second")
	if err != nil || len(other) != 1 || otherTail != second || other[0].Kind != journal.Started {
		t.Fatalf("other live journal after quota: records=%v tail=%d err=%v", other, otherTail, err)
	}
	t.Logf("WF_JRN rejected append after %d retained first-journal steps: %v", accepted, rejection)
}
