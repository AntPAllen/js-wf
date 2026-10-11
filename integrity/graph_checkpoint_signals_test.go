package integrity

import (
	"context"
	"encoding/json"
	"testing"

	"js-wf/internal/blobpublication"
	"js-wf/internal/checkpoint"
	"js-wf/internal/retainedgraph"
	"js-wf/journal"
)

func sdkSignalHistoryFixture(t *testing.T) (*auditedGraphJournal, checkpoint.Frame) {
	t.Helper()
	s := &auditedGraphJournal{}
	for _, signal := range []auditedCheckpointSignal{{Sequence: 7, Name: "go", Payload: []byte(`42`)}, {Sequence: 9, Name: "go", Payload: []byte(`43`)}, {Sequence: 12, Name: "later", Ref: "graph-signal-" + digest([]byte(`44`)), Hash: digest([]byte(`44`))}} {
		observeSDKPromiseSignal(s, signal, []retainedgraph.Link{{Hash: signal.Hash, Reference: blobpublication.Reference{Object: "signal-body"}}})
	}
	s.journal.lastSignal = 12
	s.journal.request = json.RawMessage(`{"kind":"signal","name":"go"}`)
	if err := observeSDKSignalSelection(s, journal.Entry{Kind: journal.StepCompleted, Payload: json.RawMessage(`{"signal_seq":7}`)}); err != nil {
		t.Fatal(err)
	}
	return s, checkpoint.Frame{SignalCursor: 12, ConsumedSignals: []uint64{7}, PendingSignals: []checkpoint.Signal{{Sequence: 9, Name: "go", Payload: []byte(`43`)}, {Sequence: 12, Name: "later", Payload: []byte(`44`)}}}
}

func TestRawGraphCheckpointSDKSignalHistory(t *testing.T) {
	for _, kind := range []string{"signal", "call", "timer_signal_select", "select_many", "timer-branch", "many-timer-branch", "cached-promise", "different-name-order"} {
		t.Run(kind, func(t *testing.T) {
			s, frame := sdkSignalHistoryFixture(t)
			request, done := `{"kind":"signal","name":"go"}`, `{"signal_seq":9}`
			switch kind {
			case "different-name-order":
				request = `{"kind":"signal","name":"later"}`
				done = `{"signal_seq":12}`
			case "call":
				request = `{"kind":"call","name":"go"}`
			case "timer_signal_select":
				request = `{"kind":"timer_signal_select","name":"go"}`
				done = `{"selected":"signal","signal_seq":9}`
			case "select_many":
				request = `{"kind":"select_many","cases":[{"kind":"signal","name":"go"}]}`
				done = `{"case_index":0,"signal_seq":9}`
			case "timer-branch":
				request = `{"kind":"timer_signal_select","name":"go"}`
				done = `{"selected":"timer"}`
			case "many-timer-branch":
				request = `{"kind":"select_many","cases":[{"kind":"timer","name":"clock"}]}`
				done = `{"case_index":0}`
			case "cached-promise":
				graph, prior, saved, _ := promiseHistoryFixture(t, true, false)
				if err := observeSDKSignalSelection(prior, journal.Entry{Kind: journal.StepCompleted, Payload: json.RawMessage(`{"case_index":0,"signal_seq":7}`)}); err != nil {
					t.Fatal(err)
				}
				prior.journal.lastSignal = 7
				s, frame = prior, saved
				frame.SignalCursor, frame.ConsumedSignals = 7, []uint64{7}
				if err := auditSDKCheckpointPromises(context.Background(), graph, s, frame); err != nil {
					t.Fatal(err)
				}
				request = `{"kind":"select_many","cases":[{"kind":"promise","name":"child_0"}]}`
				done = `{"case_index":0}`
			}
			s.journal.request = json.RawMessage(request)
			if kind == "cached-promise" {
				if err := observeSDKPromiseSelection(s, journal.Entry{Kind: journal.StepCompleted, Payload: json.RawMessage(done)}); err != nil {
					t.Fatal(err)
				}
			}
			if err := observeSDKSignalSelection(s, journal.Entry{Kind: journal.StepCompleted, Payload: json.RawMessage(done)}); err != nil {
				t.Fatal(err)
			}
			if kind != "timer-branch" && kind != "many-timer-branch" && kind != "cached-promise" {
				frame.ConsumedSignals = []uint64{7, 9}
				frame.PendingSignals = frame.PendingSignals[1:]
			}
			if kind == "different-name-order" {
				frame.ConsumedSignals = []uint64{7, 12}
				frame.PendingSignals = []checkpoint.Signal{{Sequence: 9, Name: "go", Payload: []byte(`43`)}}
			}
			if err := auditSDKCheckpointSignals(s, frame); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, control := range []string{"cursor-advanced", "cursor-regressed", "consumed-omitted", "consumed-fabricated", "consumed-duplicate", "pending-omitted", "pending-extra", "pending-used", "pending-name", "pending-inline-bytes", "pending-owned-bytes", "pending-duplicate", "pending-not-arrived"} {
		t.Run(control, func(t *testing.T) {
			s, frame := sdkSignalHistoryFixture(t)
			switch control {
			case "cursor-advanced":
				frame.SignalCursor++
			case "cursor-regressed":
				frame.SignalCursor--
			case "consumed-omitted":
				frame.ConsumedSignals = nil
			case "consumed-fabricated":
				frame.ConsumedSignals = []uint64{9}
			case "consumed-duplicate":
				frame.ConsumedSignals = []uint64{7, 7}
			case "pending-omitted":
				frame.PendingSignals = frame.PendingSignals[1:]
			case "pending-extra":
				frame.PendingSignals = append(frame.PendingSignals, checkpoint.Signal{Sequence: 13, Name: "extra"})
			case "pending-used":
				frame.PendingSignals[0] = checkpoint.Signal{Sequence: 7, Name: "go", Payload: []byte(`42`)}
			case "pending-name":
				frame.PendingSignals[0].Name = "foreign"
			case "pending-inline-bytes":
				frame.PendingSignals[0].Payload = []byte(`99`)
			case "pending-owned-bytes":
				frame.PendingSignals[1].Payload = []byte(`99`)
			case "pending-duplicate":
				frame.PendingSignals[1] = frame.PendingSignals[0]
			case "pending-not-arrived":
				frame.PendingSignals[0].Sequence = 11
			}
			if err := auditSDKCheckpointSignals(s, frame); err == nil {
				t.Fatal("corrupt signal materialization accepted")
			}
		})
	}
	for _, control := range []string{"selection-missing", "selection-name", "selection-reused", "selection-skips-oldest", "timer-branch-invalid", "timer-with-signal", "case-index-missing", "case-index-invalid", "case-kind-invalid", "many-timer-with-signal"} {
		t.Run(control, func(t *testing.T) {
			s, _ := sdkSignalHistoryFixture(t)
			request, done := `{"kind":"signal","name":"go"}`, `{"signal_seq":8}`
			switch control {
			case "selection-name":
				request = `{"kind":"signal","name":"other"}`
				done = `{"signal_seq":9}`
			case "selection-reused":
				done = `{"signal_seq":7}`
			case "selection-skips-oldest":
				s.sdkUsedSignals = nil
				s.sdkArrivalPositions = nil
				done = `{"signal_seq":9}`
			case "timer-branch-invalid":
				request = `{"kind":"timer_signal_select","name":"go"}`
				done = `{"selected":"other","signal_seq":9}`
			case "timer-with-signal":
				request = `{"kind":"timer_signal_select","name":"go"}`
				done = `{"selected":"timer","signal_seq":9}`
			case "case-index-missing":
				request = `{"kind":"select_many","cases":[{"kind":"signal","name":"go"}]}`
				done = `{"signal_seq":9}`
			case "case-index-invalid":
				request = `{"kind":"select_many","cases":[{"kind":"signal","name":"go"}]}`
				done = `{"case_index":1,"signal_seq":9}`
			case "case-kind-invalid":
				request = `{"kind":"select_many","cases":[{"kind":"other","name":"go"}]}`
				done = `{"case_index":0,"signal_seq":9}`
			case "many-timer-with-signal":
				request = `{"kind":"select_many","cases":[{"kind":"timer","name":"go"}]}`
				done = `{"case_index":0,"signal_seq":9}`
			}
			s.journal.request = json.RawMessage(request)
			if err := observeSDKSignalSelection(s, journal.Entry{Kind: journal.StepCompleted, Payload: json.RawMessage(done)}); err == nil {
				t.Fatal("invalid signal selection accepted")
			}
		})
	}
}
