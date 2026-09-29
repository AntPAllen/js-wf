package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"js-wf/journal"
	"js-wf/wf"
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
