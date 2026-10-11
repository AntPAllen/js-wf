package integrity

import (
	"context"
	"encoding/json"
	"js-wf/journal"
	"testing"
)

func TestRawGraphSDKStepNullMetadata(t *testing.T) {
	requestFields := []string{"kind", "name", "input_hash", "duration_nanos", "fire_at", "clock_domain", "timer_step", "timer_name", "child_type", "child_id", "cases"}
	caseFields := []string{"kind", "name", "timer_step", "fire_at", "clock_domain", "child_type", "child_id"}
	completionFields := []string{"case_index", "result_ref", "result_hash", "error", "error_kind", "signal_seq", "selected", "cancelled", "checkpoint_metadata_ref", "checkpoint_metadata_hash"}
	for _, enc := range []journal.Encoding{journal.JSON, journal.ProtobufV1} {
		for _, level := range []string{"request", "case", "completion"} {
			fields := requestFields
			if level == "case" {
				fields = caseFields
			}
			if level == "completion" {
				fields = completionFields
			}
			for _, field := range fields {
				t.Run(string(enc)+"/"+level+"/"+field, func(t *testing.T) {
					req := map[string]any{"kind": "run", "name": "x"}
					done := map[string]any{"result": 42}
					switch level {
					case "request":
						req[field] = nil
					case "case":
						c := map[string]any{"kind": "signal", "name": "gate"}
						c[field] = nil
						req["cases"] = []any{c}
					case "completion":
						done[field] = nil
					}
					r, _ := json.Marshal(req)
					d, _ := json.Marshal(done)
					snapshot, _ := rawJournalFixture(t, enc, false, func(entries []journal.Entry) { entries[1].Payload = r; entries[2].Payload = d }, false, false)
					if _, err := CheckGraphReferences(context.Background(), snapshot.Graph); err != nil {
						t.Fatal("physical references", err)
					}
					if report, err := CheckGraphJournals(context.Background(), snapshot); err == nil {
						t.Fatal("null step metadata accepted", report)
					}
				})
			}
		}
		t.Run(string(enc)+"/case/null-element", func(t *testing.T) {
			snapshot, _ := rawJournalFixture(t, enc, false, func(entries []journal.Entry) { entries[1].Payload = []byte(`{"kind":"run","name":"x","cases":[null]}`) }, false, false)
			if _, err := CheckGraphReferences(context.Background(), snapshot.Graph); err != nil {
				t.Fatal(err)
			}
			if _, err := CheckGraphJournals(context.Background(), snapshot); err == nil {
				t.Fatal("null case accepted")
			}
		})
		for _, c := range []struct {
			name          string
			request, done string
		}{{"legacy-kind-omitted", `{"name":"x"}`, `{"result":42}`}, {"legacy-kind-empty", `{"kind":"","name":"x"}`, `{"result":42}`}, {"opaque-null", `{"kind":"run","name":"x"}`, `{"result":null}`}, {"opaque-duplicates", `{"kind":"run","name":"x"}`, `{"result":{"x":1,"x":2,"error":null}}`}, {"explicit-zero", `{"kind":"run","name":"x","duration_nanos":0,"timer_step":0,"clock_domain":"","child_type":"","child_id":"","cases":[]}`, `{"result":42,"error":"","error_kind":"","signal_seq":0,"selected":"","cancelled":false}`}} {
			t.Run(string(enc)+"/valid/"+c.name, func(t *testing.T) {
				snapshot, _ := rawJournalFixture(t, enc, false, func(entries []journal.Entry) {
					entries[1].Payload = []byte(c.request)
					entries[2].Payload = []byte(c.done)
				}, false, false)
				if _, err := CheckGraphReferences(context.Background(), snapshot.Graph); err != nil {
					t.Fatal(err)
				}
				if _, err := CheckGraphJournals(context.Background(), snapshot); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
