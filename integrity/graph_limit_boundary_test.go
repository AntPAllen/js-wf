package integrity

import (
	"context"
	"encoding/json"
	"js-wf/journal"
	"testing"
)

func TestRawGraphRejectedLimitBoundary(t *testing.T) {
	controls := []struct {
		name, metadata            string
		valid, completed, pending bool
	}{
		{name: "request", metadata: `"limit_request":{"kind":"run","name":"extra"}`, valid: true},
		{name: "attempt", metadata: `"limit_entry":{"kind":"Attempt","payload":{"count":1,"error":"panic"}}`, valid: true},
		{name: "signal", metadata: `"limit_entry":{"kind":"SignalConsumed","payload":{"sig_seq":1,"name":"event","payload":"NDI="}}`, valid: true},
		{name: "suspension", metadata: `"limit_entry":{"kind":"Suspended","payload":{"waiting_on":"timer:clock"}}`, valid: true, pending: true},
		{name: "overlap", metadata: `"limit_request":{"kind":"run","name":"extra"}`, pending: true},
		{name: "completed", metadata: `"limit_request":{"kind":"run"}`, completed: true},
		{name: "both", metadata: `"limit_request":{"kind":"run"},"limit_entry":{"kind":"Attempt","payload":{"count":1,"error":"panic"}}`},
		{name: "request-null", metadata: `"limit_request":null`},
		{name: "request-empty", metadata: `"limit_request":{}`},
		{name: "request-unknown", metadata: `"limit_request":{"kind":"run","unknown":1}`},
		{name: "request-duplicate", metadata: `"limit_request":{"kind":"run","kind":"signal"}`},
		{name: "entry-kind", metadata: `"limit_entry":{"kind":"StepCompleted","payload":{}}`},
		{name: "entry-null", metadata: `"limit_entry":{"kind":"Attempt","payload":null}`},
		{name: "attempt-gap", metadata: `"limit_entry":{"kind":"Attempt","payload":{"count":2,"error":"panic"}}`},
		{name: "attempt-empty-error", metadata: `"limit_entry":{"kind":"Attempt","payload":{"count":1}}`},
		{name: "attempt-alias", metadata: `"limit_entry":{"kind":"Attempt","payload":{"Count":1,"error":"panic"}}`},
		{name: "attempt-duplicate", metadata: `"limit_entry":{"kind":"Attempt","payload":{"count":2,"count":1,"error":"panic"}}`},
		{name: "signal-zero", metadata: `"limit_entry":{"kind":"SignalConsumed","payload":{"sig_seq":0,"name":"event"}}`},
		{name: "signal-name", metadata: `"limit_entry":{"kind":"SignalConsumed","payload":{"sig_seq":1}}`},
		{name: "signal-alias", metadata: `"limit_entry":{"kind":"SignalConsumed","payload":{"Sig_seq":1,"name":"event"}}`},
		{name: "suspension-unresolved", metadata: `"limit_entry":{"kind":"Suspended","payload":{"waiting_on":"timer:clock"}}`},
		{name: "suspension-empty", metadata: `"limit_entry":{"kind":"Suspended","payload":{}}`, pending: true},
		{name: "suspension-unknown", metadata: `"limit_entry":{"kind":"Suspended","payload":{"waiting_on":"timer:clock","unknown":1}}`, pending: true},
		{name: "wrong-error", metadata: `"limit_request":{"kind":"run"}`},
	}
	for _, encoding := range []journal.Encoding{journal.JSON, journal.ProtobufV1} {
		for _, c := range controls {
			t.Run(string(encoding)+"/"+c.name, func(t *testing.T) {
				s, _ := rawJournalFixture(t, encoding, false, func(entries []journal.Entry) {
					errorText := journal.ErrTooLong.Error()
					if c.name == "wrong-error" {
						errorText = "failure"
					}
					if c.completed {
						errorText = ""
					} else {
						entries[3].Kind = journal.Failed
					}
					entries[3].Payload = json.RawMessage(`{"inv_seq":10,"error":` + string(mustLimitJSON(t, errorText)) + `,` + c.metadata + `}`)
					if c.pending {
						entries[2].Kind = journal.Attempt
						entries[2].Payload = json.RawMessage(`{"count":1,"error":"prior panic"}`)
					}
				}, false, false)
				if _, err := CheckGraphReferences(context.Background(), s.Graph); err != nil {
					t.Fatal("physical references", err)
				}
				_, err := CheckGraphJournals(context.Background(), s)
				if c.valid && err != nil {
					t.Fatal(err)
				}
				if !c.valid && err == nil {
					t.Fatal("invalid rejected operation accepted")
				}
			})
		}
	}
}
func mustLimitJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestRawGraphRejectedLimitPrefix(t *testing.T) {
	// These are histories with prior nonzero attempt/signal state. A rejected
	// operation must neither reuse nor skip the prefix's next position.
	for _, c := range []struct {
		name, kind, payload string
		valid               bool
	}{
		{"attempt-next", "Attempt", `{"count":4,"error":"panic"}`, true},
		{"attempt-reuse", "Attempt", `{"count":3,"error":"panic"}`, false},
		{"attempt-skip", "Attempt", `{"count":5,"error":"panic"}`, false},
		{"signal-next", "SignalConsumed", `{"sig_seq":8,"name":"event"}`, true},
		{"signal-reuse", "SignalConsumed", `{"sig_seq":7,"name":"event"}`, false},
		{"signal-backwards", "SignalConsumed", `{"sig_seq":6,"name":"event"}`, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			prefix := journalAudit{lastAttempt: 3, lastSignal: 7}
			raw := json.RawMessage(`{"inv_seq":10,"error":"journal exceeds 100000 entries","limit_entry":{"kind":"` + c.kind + `","payload":` + c.payload + `}}`)
			err := auditGraphLimitBoundary(journal.Entry{Kind: journal.Failed, Payload: raw}, &prefix)
			if (err == nil) != c.valid {
				t.Fatalf("valid=%v error=%v", c.valid, err)
			}
			if prefix.lastAttempt != 3 || prefix.lastSignal != 7 {
				t.Fatal("rejected operation mutated prefix")
			}
		})
	}
}

func TestRawGraphRejectedLimitContinuation(t *testing.T) {
	hash := digest([]byte(`{"frame":42}`))
	for _, c := range []struct {
		name, waiting, kind, errorText string
		valid                          bool
	}{
		{"checkpoint", "continuation:next", "checkpoint", "", true},
		{"wrong-stage", "continuation:other", "checkpoint", "", false},
		{"not-checkpoint", "continuation:next", "run", "", false},
		{"failed-frame", "continuation:next", "checkpoint", "failed", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			prefix := journalAudit{
				request:    mustLimitJSON(t, map[string]any{"kind": c.kind, "name": "next"}),
				completion: mustLimitJSON(t, map[string]any{"result_ref": "step-result-" + hash, "result_hash": hash, "error": c.errorText}),
			}
			entry := journal.Entry{Kind: journal.Failed, Payload: mustLimitJSON(t, map[string]any{
				"inv_seq": 10, "error": journal.ErrTooLong.Error(),
				"limit_entry": map[string]any{"kind": "Suspended", "payload": map[string]any{"waiting_on": c.waiting}},
			})}
			err := auditGraphLimitBoundary(entry, &prefix)
			if (err == nil) != c.valid {
				t.Fatalf("valid=%v error=%v", c.valid, err)
			}
		})
	}
}
