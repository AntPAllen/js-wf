package integrity

import (
	"context"
	"encoding/json"
	"testing"

	"js-wf/journal"
)

func TestRawGraphSDKStepEnvelope(t *testing.T) {
	controls := []struct {
		name  string
		index int
		raw   string
		valid bool
	}{
		{"ordinary", 2, `{"result":42}`, true},
		{"opaque-field-names", 2, `{"result":{"unknown":1,"Result":2,"signal_seq":"user-value"}}`, true},
		{"opaque-duplicates", 2, `{"result":{"x":1,"x":2}}`, true},
		{"opaque-null", 2, `{"result":null}`, true},
		{"request-null", 1, `null`, false},
		{"request-array", 1, `[]`, false},
		{"request-unknown", 1, `{"kind":"run","name":"x","unknown":1}`, false},
		{"request-alias", 1, `{"Kind":"run","name":"x"}`, false},
		{"request-duplicate", 1, `{"kind":"run","kind":"signal","name":"x"}`, false},
		{"request-escaped-duplicate", 1, `{"kind":"run","\u006bind":"signal","name":"x"}`, false},
		{"request-time-type", 1, `{"kind":"run","name":"x","fire_at":12}`, false},
		{"request-step-negative", 1, `{"kind":"run","name":"x","timer_step":-1}`, false},
		{"case-unknown", 1, `{"kind":"select_many","cases":[{"kind":"signal","name":"x","unknown":1}]}`, false},
		{"case-alias", 1, `{"kind":"select_many","cases":[{"Kind":"signal","name":"x"}]}`, false},
		{"case-duplicate", 1, `{"kind":"select_many","cases":[{"kind":"signal","name":"x","name":"y"}]}`, false},
		{"case-shape", 1, `{"kind":"select_many","cases":{}}`, false},
		{"completion-null", 2, `null`, false},
		{"completion-array", 2, `[]`, false},
		{"completion-unknown", 2, `{"result":42,"unknown":1}`, false},
		{"completion-alias", 2, `{"Result":42}`, false},
		{"completion-duplicate", 2, `{"result":42,"result":43}`, false},
		{"completion-sequence-negative", 2, `{"signal_seq":-1}`, false},
		{"completion-case-fraction", 2, `{"case_index":1.5}`, false},
		{"completion-cancel-type", 2, `{"cancelled":"true"}`, false},
		{"metadata-ref-null", 2, `{"checkpoint_metadata_ref":null}`, false},
		{"metadata-ref-object", 2, `{"checkpoint_metadata_ref":{}}`, false},
		{"metadata-hash-null", 2, `{"checkpoint_metadata_hash":null}`, false},
		{"metadata-hash-number", 2, `{"checkpoint_metadata_hash":7}`, false},
	}
	for _, encoding := range []journal.Encoding{journal.JSON, journal.ProtobufV1} {
		for _, c := range controls {
			t.Run(string(encoding)+"/"+c.name, func(t *testing.T) {
				s, _ := rawJournalFixture(t, encoding, false, func(entries []journal.Entry) { entries[c.index].Payload = json.RawMessage(c.raw) }, false, false)
				if _, err := CheckGraphReferences(context.Background(), s.Graph); err != nil {
					t.Fatal("invalid physical fixture", err)
				}
				_, err := CheckGraphJournals(context.Background(), s)
				if c.valid && err != nil {
					t.Fatal("opaque result or valid envelope rejected", err)
				}
				if !c.valid && err == nil {
					t.Fatal("invalid runtime envelope accepted")
				}
			})
		}
	}
}
