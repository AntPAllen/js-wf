package history

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"js-wf/client"
)

func enqueueHistoryOperation(index int, status string, sequence uint64) client.Operation {
	base := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	return client.Operation{Op: "start", InvokeTS: base.Add(time.Duration(index) * time.Millisecond), ReturnTS: base.Add(time.Duration(index+1) * time.Millisecond),
		Args:   json.RawMessage(`{"type":"retry","id":"same","input_hash":"hash-a"}`),
		Result: json.RawMessage(fmt.Sprintf(`{"status":%q,"inv_seq":%d}`, status, sequence))}
}

func TestStartHistoryRepeatedEnqueueUnknown(t *testing.T) {
	for _, tc := range []struct {
		name     string
		statuses []string
		want     porcupine.CheckResult
	}{
		{"repeated_dispatch_failure", []string{"enqueue_unknown", "enqueue_unknown", "already_started"}, porcupine.Ok},
		{"known_creator_then_dispatch_failures", []string{"started", "enqueue_unknown", "enqueue_unknown", "already_started"}, porcupine.Ok},
		{"self_observed_record_then_dispatch_failure", []string{"already_started", "enqueue_unknown", "already_started"}, porcupine.Ok},
		{"creation_after_stored_observation", []string{"enqueue_unknown", "started"}, porcupine.Illegal},
		{"two_creators", []string{"started", "started"}, porcupine.Illegal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var operations []client.Operation
			for i, status := range tc.statuses {
				operations = append(operations, enqueueHistoryOperation(i*2, status, 7))
			}
			if got, err := CheckStarts(operations, time.Second); err != nil || got != tc.want {
				t.Fatalf("history=%s err=%v want=%s", got, err, tc.want)
			}
		})
	}
	for _, mutation := range []string{"wrong_sequence", "zero_sequence", "changed_input", "changed_parent"} {
		t.Run(mutation, func(t *testing.T) {
			operations := []client.Operation{enqueueHistoryOperation(0, "enqueue_unknown", 7), enqueueHistoryOperation(2, "enqueue_unknown", 7)}
			switch mutation {
			case "wrong_sequence":
				operations[1].Result = json.RawMessage(`{"status":"enqueue_unknown","inv_seq":8}`)
			case "zero_sequence":
				operations[1].Result = json.RawMessage(`{"status":"enqueue_unknown","inv_seq":0}`)
			case "changed_input":
				operations[1].Args = json.RawMessage(`{"type":"retry","id":"same","input_hash":"hash-b"}`)
			case "changed_parent":
				operations[1].Args = json.RawMessage(`{"type":"retry","id":"same","input_hash":"hash-a","parent_type":"parent","parent_id":"other","signal_name":"child"}`)
			}
			if got, err := CheckStarts(operations, time.Second); err != nil || got != porcupine.Illegal {
				t.Fatalf("invalid history=%s err=%v", got, err)
			}
		})
	}
}

func TestStartHistoryLargeRepeatedEnqueueUnknown(t *testing.T) {
	for _, mode := range []string{"all_observations", "one_creator", "two_creators", "wrong_sequence", "observation_before_creator"} {
		t.Run(mode, func(t *testing.T) {
			operations := make([]client.Operation, 500)
			for i := range operations {
				operations[i] = enqueueHistoryOperation(i, "enqueue_unknown", 7)
				operations[i].ReturnTS = operations[0].InvokeTS.Add(time.Second)
			}
			want := porcupine.Ok
			if mode != "all_observations" {
				operations[250].Result = json.RawMessage(`{"status":"started","inv_seq":7}`)
			}
			switch mode {
			case "two_creators":
				operations[251].Result = json.RawMessage(`{"status":"started","inv_seq":7}`)
				want = porcupine.Illegal
			case "wrong_sequence":
				operations[251].Result = json.RawMessage(`{"status":"enqueue_unknown","inv_seq":8}`)
				want = porcupine.Illegal
			case "observation_before_creator":
				operations[0].ReturnTS = operations[0].InvokeTS.Add(time.Millisecond)
				want = porcupine.Illegal
			}
			if got, err := CheckStarts(operations, time.Second); err != nil || got != want {
				t.Fatalf("large history=%s err=%v want=%s", got, err, want)
			}
		})
	}
}
