package integrity

import (
	"context"
	"encoding/json"
	"js-wf/journal"
	"testing"
	"time"
)

func TestRawGraphSelectionPriority(t *testing.T) {
	for _, encoding := range []journal.Encoding{journal.JSON, journal.ProtobufV1} {
		for _, mode := range []string{"first", "reordered", "earlier-absent", "skipped-ready", "invalid-unselected-kind", "invalid-unselected-name"} {
			t.Run(string(encoding)+"/"+mode, func(t *testing.T) {
				s, _ := rawJournalCheckpointFixture(t, encoding, false, func(e []journal.Entry) {
					switch mode {
					case "reordered":
						e[3].Payload = json.RawMessage(`{"kind":"select_many","cases":[{"kind":"signal","name":"second"},{"kind":"signal","name":"first"}]}`)
						e[4].Payload = json.RawMessage(`{"case_index":0,"signal_seq":9}`)
					case "earlier-absent":
						e[1].Payload = json.RawMessage(`{"sig_seq":7,"name":"other","payload":"NDI="}`)
						e[4].Payload = json.RawMessage(`{"case_index":1,"signal_seq":9}`)
					case "skipped-ready":
						e[4].Payload = json.RawMessage(`{"case_index":1,"signal_seq":9}`)
					case "invalid-unselected-kind":
						e[3].Payload = json.RawMessage(`{"kind":"select_many","cases":[{"kind":"signal","name":"first"},{"kind":"foreign","name":"second"}]}`)
					case "invalid-unselected-name":
						e[3].Payload = json.RawMessage(`{"kind":"select_many","cases":[{"kind":"signal","name":"first"},{"kind":"signal","name":"invalid.name"}]}`)
					}
				}, false, false, "selection-priority")
				if _, err := CheckGraphReferences(context.Background(), s.Graph); err != nil {
					t.Fatal("physical references", err)
				}
				_, err := CheckGraphJournals(context.Background(), s)
				valid := mode == "first" || mode == "reordered" || mode == "earlier-absent"
				if valid && err != nil {
					t.Fatal(err)
				}
				if !valid && err == nil {
					t.Fatal("invalid selection accepted")
				}
			})
		}
	}
}

func TestRawGraphSelectionReadiness(t *testing.T) {
	for _, mode := range []string{"signal-first", "signal-skipped", "signal-used", "promise-buffered-skipped", "promise-cached-skipped", "promise-ambiguous", "timer-zero-skipped", "timer-positive-unknown", "timer-cancelled", "timer-fired", "timer-signal-buffered", "timer-signal-used", "timer-signal-absent", "timer-before-signal", "invalid-child"} {
		t.Run(mode, func(t *testing.T) {
			s := &auditedGraphJournal{sdkArrivalQueue: map[string][]uint64{"ready": {7}}, sdkArrivalPositions: map[string]int{}}
			req := `{"kind":"select_many","cases":[{"kind":"signal","name":"ready"},{"kind":"signal","name":"other"}]}`
			done := `{"case_index":1,"signal_seq":9}`
			valid := false
			switch mode {
			case "signal-first":
				done = `{"case_index":0,"signal_seq":7}`
				valid = true
			case "signal-used":
				s.sdkArrivalPositions["ready"] = 1
				valid = true
			case "promise-buffered-skipped":
				req = `{"kind":"select_many","cases":[{"kind":"promise","name":"ready"},{"kind":"signal","name":"other"}]}`
			case "promise-cached-skipped", "promise-ambiguous":
				req = `{"kind":"select_many","cases":[{"kind":"promise","name":"child"},{"kind":"signal","name":"other"}]}`
				candidates := []auditedPromiseSignal{{event: auditedCheckpointSignal{Sequence: 1, Name: "child"}}}
				if mode == "promise-cached-skipped" {
					s.requiredPromises = map[string][]auditedPromiseSignal{"child": candidates}
				} else {
					s.promiseCandidates = map[string][]auditedPromiseSignal{"child": candidates}
					valid = true
				}
			case "timer-zero-skipped", "timer-positive-unknown", "timer-cancelled", "timer-fired", "timer-before-signal":
				timer := auditedSDKTimer{name: "clock"}
				if mode == "timer-positive-unknown" {
					timer.deadline = time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
					valid = true
				}
				if mode == "timer-cancelled" {
					timer.cancelled = true
					valid = true
				}
				if mode == "timer-fired" {
					timer.fired = true
					valid = true
				}
				s.sdkTimers = map[uint64]auditedSDKTimer{0: timer}
				req = `{"kind":"select_many","cases":[{"kind":"timer","name":"clock"},{"kind":"signal","name":"ready"}]}`
				if mode == "timer-before-signal" {
					done = `{"case_index":0}`
					valid = true
				}
			case "timer-signal-buffered", "timer-signal-used", "timer-signal-absent":
				req = `{"kind":"timer_signal_select","name":"ready"}`
				done = `{"selected":"timer"}`
				if mode == "timer-signal-used" {
					s.sdkArrivalPositions["ready"] = 1
					valid = true
				}
				if mode == "timer-signal-absent" {
					s.sdkArrivalQueue = nil
					valid = true
				}
			case "invalid-child":
				req = `{"kind":"select_many","cases":[{"kind":"signal","name":"ready"},{"kind":"promise","name":"child","child_type":"flow"}]}`
				done = `{"case_index":0,"signal_seq":7}`
			}
			s.journal.request = json.RawMessage(req)
			err := auditSDKSelectionPriority(s, journal.Entry{Kind: journal.StepCompleted, Payload: json.RawMessage(done)})
			if (err == nil) != valid {
				t.Fatalf("valid=%v err=%v", valid, err)
			}
			if s.sdkArrivalPositions["ready"] != 0 && mode != "signal-used" && mode != "timer-signal-used" {
				t.Fatal("readiness consumed signal")
			}
		})
	}
}
