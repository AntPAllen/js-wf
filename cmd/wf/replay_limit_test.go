package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/testcluster"
	"js-wf/wf"

	"github.com/nats-io/nats.go/jetstream"
)

func TestReplayJournalLimitChecksAttemptedStepWithoutEffect(t *testing.T) {
	pluginPath := buildReplayPlugin(t)
	input := []byte(`{"n":7}`)
	inputHash := sha256.Sum256(input)
	stepHash := sha256.Sum256([]byte(`7`))
	request, err := json.Marshal(struct {
		Kind      string `json:"kind"`
		Name      string `json:"name"`
		InputHash string `json:"input_hash"`
	}{"run", "double", hex.EncodeToString(stepHash[:])})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := json.Marshal(wf.Outcome{InvSeq: 5, Error: journal.ErrTooLong.Error(), LimitRequest: request})
	if err != nil {
		t.Fatal(err)
	}
	bundle := replayBundle{
		Type: "test", ID: "limit", InvSeq: 5, Input: input,
		InputHash: hex.EncodeToString(inputHash[:]),
		Journal: []journal.Record{
			{Entry: journal.Entry{Index: 0, Epoch: 1, Kind: journal.Started}, Sequence: 1},
			{Entry: journal.Entry{Index: 1, Epoch: 1, Kind: journal.Failed, Payload: terminal}, Sequence: 2},
		},
	}
	marker := filepath.Join(t.TempDir(), "effect-ran")
	t.Setenv("WF_REPLAY_EFFECT_MARKER", marker)
	report, err := runReplayBundle(bundle, pluginPath, "Workflow")
	if err != nil || report.Status != "failed" || report.Error != journal.ErrTooLong.Error() {
		t.Fatalf("journal-limit replay report=%+v err=%v", report, err)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("offline replay executed effect: %v", err)
	}
	if _, err := runReplayBundle(bundle, pluginPath, "ChangedWorkflow"); err == nil || !strings.Contains(err.Error(), "differs") {
		t.Fatalf("renamed limit step accepted: %v", err)
	}
	bad := bundle
	bad.Journal = append([]journal.Record(nil), bundle.Journal...)
	corrupt, err := json.Marshal(wf.Outcome{InvSeq: 5, Error: journal.ErrTooLong.Error(), LimitRequest: json.RawMessage(`{"kind":"run","name":"double","input_hash":"wrong"}`)})
	if err != nil {
		t.Fatal(err)
	}
	bad.Journal[1].Payload = corrupt
	if _, err := runReplayBundle(bad, pluginPath, "Workflow"); err == nil || !strings.Contains(err.Error(), "differs") {
		t.Fatalf("changed limit request accepted: %v", err)
	}
}

func TestReplayNonStepJournalLimit(t *testing.T) {
	pluginPath := buildReplayPlugin(t)
	input := []byte(`null`)
	inputHash := sha256.Sum256(input)
	for _, tc := range []struct {
		name, symbol, changed string
		entry                 wf.LimitEntry
	}{
		{"suspension", "WaitSignalWorkflow", "ChangedWait", wf.LimitEntry{Kind: string(journal.Suspended), Payload: json.RawMessage(`{"waiting_on":"signal:go"}`)}},
		{"panic", "PanicWorkflow", "ImmediateFailure", wf.LimitEntry{Kind: string(journal.Attempt), Payload: json.RawMessage(`{"count":1,"error":"workflow panic: boom"}`)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			terminal, err := json.Marshal(wf.Outcome{InvSeq: 5, Error: journal.ErrTooLong.Error(), LimitEntry: &tc.entry})
			if err != nil {
				t.Fatal(err)
			}
			bundle := replayBundle{Type: "test", ID: tc.name, InvSeq: 5, Input: input, InputHash: hex.EncodeToString(inputHash[:]), Journal: []journal.Record{
				{Entry: journal.Entry{Index: 0, Epoch: 1, Kind: journal.Started}, Sequence: 1},
				{Entry: journal.Entry{Index: 1, Epoch: 1, Kind: journal.Failed, Payload: terminal}, Sequence: 2},
			}}
			if tc.name == "suspension" {
				bundle.Journal = []journal.Record{
					{Entry: journal.Entry{Index: 0, Epoch: 1, Kind: journal.Started}, Sequence: 1},
					{Entry: journal.Entry{Index: 1, Epoch: 1, Kind: journal.StepRequested, Payload: json.RawMessage(`{"kind":"signal","name":"go","input_hash":""}`)}, Sequence: 2},
					{Entry: journal.Entry{Index: 2, Epoch: 1, Kind: journal.Failed, Payload: terminal}, Sequence: 3},
				}
			}
			report, err := runReplayBundle(bundle, pluginPath, tc.symbol)
			if err != nil || report.Status != "failed" || report.Error != journal.ErrTooLong.Error() {
				t.Fatalf("report=%+v err=%v", report, err)
			}
			if _, err := runReplayBundle(bundle, pluginPath, tc.changed); err == nil || !strings.Contains(err.Error(), "differs") {
				t.Fatalf("changed handler accepted: %v", err)
			}
			legacy := bundle
			legacy.Journal = append([]journal.Record(nil), bundle.Journal...)
			legacy.Journal[len(legacy.Journal)-1].Payload, _ = json.Marshal(wf.Outcome{InvSeq: 5, Error: journal.ErrTooLong.Error()})
			if _, err := runReplayBundle(legacy, pluginPath, tc.symbol); err == nil || !strings.Contains(err.Error(), "metadata") {
				t.Fatalf("legacy failure incorrectly verified: %v", err)
			}
		})
	}
}

func TestReplayRejectedSignalDrainUsesRetainedSource(t *testing.T) {
	pluginPath := buildReplayPlugin(t)
	cluster, err := testcluster.Start(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := provision.Ensure(ctx, js, 1); err != nil {
		t.Fatal(err)
	}
	const typ, id = "test", "signal-limit-replay"
	c := client.New(js)
	handle, err := c.Start(ctx, typ, id, []byte(`null`))
	if err != nil {
		t.Fatal(err)
	}
	signalSeq, err := c.Signal(ctx, typ, id, "go", []byte(`true`), "limit-go")
	if err != nil {
		t.Fatal(err)
	}
	before, err := fetchReplayBundle(ctx, js, typ, id)
	if err != nil || len(before.Journal) != 0 {
		t.Fatalf("export before first journal entry: records=%d err=%v", len(before.Journal), err)
	}
	hash := sha256.Sum256([]byte(`true`))
	attempted, _ := json.Marshal(struct {
		Sequence uint64 `json:"sig_seq"`
		Name     string `json:"name"`
		Payload  []byte `json:"payload"`
		Hash     string `json:"hash"`
	}{signalSeq, "go", []byte(`true`), hex.EncodeToString(hash[:])})
	failure, _ := json.Marshal(wf.Outcome{InvSeq: handle.InvSeq, Error: journal.ErrTooLong.Error(), LimitEntry: &wf.LimitEntry{Kind: string(journal.SignalConsumed), Payload: attempted}})
	entries := []journal.Entry{
		{Epoch: 1, Index: 0, Kind: journal.Started},
		{Epoch: 1, Index: 1, Kind: journal.StepRequested, Payload: json.RawMessage(`{"kind":"signal","name":"go","input_hash":""}`)},
		{Epoch: 1, Index: 2, Kind: journal.Suspended, Payload: json.RawMessage(`{"waiting_on":"signal:go"}`)},
		{Epoch: 1, Index: 3, Kind: journal.Failed, Payload: failure},
	}
	store := journal.New(js)
	var tail uint64
	for _, entry := range entries {
		tail, err = store.Append(ctx, typ, id, entry, tail)
		if err != nil {
			t.Fatal(err)
		}
	}
	bundle, err := fetchReplayBundle(ctx, js, typ, id)
	if err != nil || bundle.PendingSignal == nil || bundle.PendingSignal.Sequence != signalSeq {
		t.Fatalf("export rejected signal: bundle=%+v err=%v", bundle.PendingSignal, err)
	}
	data, err := json.Marshal(bundle)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "signal-limit.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	bundle, err = loadReplayBundle(path)
	if err != nil {
		t.Fatal(err)
	}
	report, err := runReplayBundle(bundle, pluginPath, "WaitSignalWorkflow")
	if err != nil || report.Status != "failed" || report.Error != journal.ErrTooLong.Error() {
		t.Fatalf("rejected signal replay=%+v err=%v", report, err)
	}
	// A pending signal can also fill a short journal before its first handler
	// run. There is then no prior suspension for offline replay to reproduce.
	fresh := bundle
	fresh.Journal = []journal.Record{bundle.Journal[0], bundle.Journal[len(bundle.Journal)-1]}
	fresh.Journal[1].Index = 1
	if _, err := runReplayBundle(fresh, pluginPath, "WaitSignalWorkflow"); err != nil {
		t.Fatalf("rejected signal before first handler run: %v", err)
	}
	changed := bundle
	changed.PendingSignal = nil
	if _, err := runReplayBundle(changed, pluginPath, "WaitSignalWorkflow"); err == nil || !strings.Contains(err.Error(), "source is missing") {
		t.Fatalf("missing signal source accepted: %v", err)
	}
	changed = bundle
	source := *bundle.PendingSignal
	source.Data = []byte(`false`)
	changed.PendingSignal = &source
	if _, err := runReplayBundle(changed, pluginPath, "WaitSignalWorkflow"); err == nil || !strings.Contains(err.Error(), "differs") {
		t.Fatalf("changed signal payload accepted: %v", err)
	}
	changed = bundle
	source = *bundle.PendingSignal
	source.Header = make(map[string][]string, len(bundle.PendingSignal.Header))
	for key, values := range bundle.PendingSignal.Header {
		source.Header[key] = append([]string(nil), values...)
	}
	source.Header.Set("Wf-Inv-Seq", "999")
	changed.PendingSignal = &source
	if _, err := runReplayBundle(changed, pluginPath, "WaitSignalWorkflow"); err == nil || !strings.Contains(err.Error(), "generation") {
		t.Fatalf("changed signal generation accepted: %v", err)
	}
	blob := bundle
	blob.Journal = append([]journal.Record(nil), bundle.Journal...)
	blob.Objects = make(map[string][]byte, len(bundle.Objects)+1)
	for name, value := range bundle.Objects {
		blob.Objects[name] = value
	}
	large := bytes.Repeat([]byte("x"), client.MaxInlineSignal+1)
	largeHash := sha256.Sum256(large)
	ref := "signal-" + hex.EncodeToString(largeHash[:])
	blob.Objects[ref] = large
	source = *bundle.PendingSignal
	source.Header = make(map[string][]string, len(bundle.PendingSignal.Header)+1)
	for key, values := range bundle.PendingSignal.Header {
		source.Header[key] = append([]string(nil), values...)
	}
	source.Header.Set("Wf-Signal-Ref", ref)
	source.Header.Set("Wf-Input-SHA256", hex.EncodeToString(largeHash[:]))
	source.Data = nil
	blob.PendingSignal = &source
	largeAttempt, _ := json.Marshal(struct {
		Sequence uint64 `json:"sig_seq"`
		Name     string `json:"name"`
		Ref      string `json:"ref"`
		Hash     string `json:"hash"`
	}{signalSeq, "go", ref, hex.EncodeToString(largeHash[:])})
	blobFailure, _ := json.Marshal(wf.Outcome{InvSeq: handle.InvSeq, Error: journal.ErrTooLong.Error(), LimitEntry: &wf.LimitEntry{Kind: string(journal.SignalConsumed), Payload: largeAttempt}})
	blob.Journal[len(blob.Journal)-1].Payload = blobFailure
	if _, err := runReplayBundle(blob, pluginPath, "WaitSignalWorkflow"); err != nil {
		t.Fatalf("rejected large signal replay: %v", err)
	}
	blob.Objects[ref] = []byte(`corrupt`)
	if _, err := runReplayBundle(blob, pluginPath, "WaitSignalWorkflow"); err == nil || !strings.Contains(err.Error(), "hash mismatch") {
		t.Fatalf("corrupt rejected signal object accepted: %v", err)
	}
}

func TestReplayLongPanicMatchesTruncatedAttempt(t *testing.T) {
	pluginPath := buildReplayPlugin(t)
	input := []byte(`null`)
	inputHash := sha256.Sum256(input)
	panicText := journal.AttemptError("workflow panic: " + strings.Repeat("x", journal.MaxAttemptErrorBytes-len("workflow panic: ")-1) + "é trailing")
	for _, atLimit := range []bool{false, true} {
		outcome := wf.Outcome{InvSeq: 5, Error: panicText}
		if atLimit {
			outcome.Error = journal.ErrTooLong.Error()
			attempt, _ := json.Marshal(journal.AttemptPayload{Count: 1, Error: panicText})
			outcome.LimitEntry = &wf.LimitEntry{Kind: string(journal.Attempt), Payload: attempt}
		}
		terminal, err := json.Marshal(outcome)
		if err != nil {
			t.Fatal(err)
		}
		records := []journal.Record{{Entry: journal.Entry{Index: 0, Epoch: 1, Kind: journal.Started}, Sequence: 1}}
		if !atLimit {
			attempt, _ := json.Marshal(journal.AttemptPayload{Count: 1, Error: panicText})
			records = append(records, journal.Record{Entry: journal.Entry{Index: 1, Epoch: 1, Kind: journal.Attempt, Payload: attempt}, Sequence: 2})
		}
		records = append(records, journal.Record{Entry: journal.Entry{Index: uint64(len(records)), Epoch: 1, Kind: journal.Failed, Payload: terminal}, Sequence: uint64(len(records) + 1)})
		bundle := replayBundle{Type: "test", ID: "long-panic", InvSeq: 5, Input: input, InputHash: hex.EncodeToString(inputHash[:]), Journal: records}
		report, err := runReplayBundle(bundle, pluginPath, "LongPanicWorkflow")
		if err != nil || report.Status != "failed" || report.Error != outcome.Error {
			t.Fatalf("atLimit=%t report=%+v err=%v", atLimit, report, err)
		}
	}
}
