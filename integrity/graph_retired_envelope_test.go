package integrity

import (
	"context"
	"encoding/json"
	"js-wf/internal/retainedgraph"
	"js-wf/journal"
	"strings"
	"testing"
)

func TestRawGraphRetiredProjectionEnvelope(t *testing.T) {
	hash := strings.Repeat("a", 64)
	controls := []struct {
		name, raw     string
		valid, failed bool
	}{
		{"inline", `{"inv_seq":10,"result":"NDI="}`, true, false},
		{"opaque", `{"inv_seq":10,"result":"eyJhIjoxLCJhIjoyfQ=="}`, true, false},
		{"empty", `{"inv_seq":10}`, true, false},
		{"whitespace", " \n{\"inv_seq\":10}\n", true, false},
		{"failed", `{"inv_seq":10,"error":"failure"}`, true, true},
		{"external", `{"inv_seq":10,"result_ref":"result","result_hash":"` + hash + `"}`, true, false},
		{"limit-request", `{"inv_seq":10,"error":"journal exceeds 100000 entries","limit_request":{"kind":"run","name":"extra"}}`, true, true},
		{"limit-entry", `{"inv_seq":10,"error":"journal exceeds 100000 entries","limit_entry":{"kind":"Attempt","payload":{"count":1,"error":"panic"}}}`, true, true},
		{"unknown", `{"inv_seq":10,"unknown":1}`, false, false},
		{"alias", `{"inv_seq":10,"Result":"NDI="}`, false, false},
		{"duplicate-generation", `{"inv_seq":9,"inv_seq":10}`, false, false},
		{"escaped-generation", `{"inv_seq":9,"\u0069nv_seq":10}`, false, false},
		{"duplicate-result", `{"inv_seq":10,"result":"Nw==","result":"NDI="}`, false, false},
		{"duplicate-error", `{"inv_seq":10,"error":"bad","error":""}`, false, false},
		{"unknown-limit-entry", `{"inv_seq":10,"error":"journal exceeds 100000 entries","limit_entry":{"kind":"Attempt","payload":{},"unknown":1}}`, false, true},
		{"alias-limit-entry", `{"inv_seq":10,"error":"journal exceeds 100000 entries","limit_entry":{"Kind":"Attempt","payload":{}}}`, false, true},
		{"duplicate-limit-entry", `{"inv_seq":10,"error":"journal exceeds 100000 entries","limit_entry":{"kind":"Attempt","kind":"Suspended","payload":{}}}`, false, true},
		{"null", `null`, false, false},
		{"array", `[]`, false, false},
		{"base64", `{"inv_seq":10,"result":"!"}`, false, false},
		{"generation-fraction", `{"inv_seq":10.5}`, false, false},
		{"generation-negative", `{"inv_seq":-10}`, false, false},
		{"failed-inline", `{"inv_seq":10,"error":"failure","result":"NDI="}`, false, true},
		{"failed-pointer", `{"inv_seq":10,"error":"failure","result_ref":"result","result_hash":"` + hash + `"}`, false, true},
		{"hash-only", `{"inv_seq":10,"result_hash":"` + hash + `"}`, false, false},
		{"pointer-only", `{"inv_seq":10,"result_ref":"result"}`, false, false},
		{"bad-hash", `{"inv_seq":10,"result_ref":"result","result_hash":"bad"}`, false, false},
		{"mixed-result", `{"inv_seq":10,"result":"NDI=","result_ref":"result","result_hash":"` + hash + `"}`, false, false},
		{"completed-limit", `{"inv_seq":10,"limit_request":{}}`, false, false},
		{"both-limits", `{"inv_seq":10,"error":"journal exceeds 100000 entries","limit_request":{},"limit_entry":{"kind":"Attempt","payload":{}}}`, false, true},
		{"wrong-limit-error", `{"inv_seq":10,"error":"other","limit_request":{}}`, false, true},
	}
	for _, enc := range []journal.Encoding{journal.JSON, journal.ProtobufV1} {
		for _, c := range controls {
			t.Run(string(enc)+"/"+c.name, func(t *testing.T) {
				s, projections := rawJournalFixture(t, enc, false, nil, false, false)
				projections["kind.id"] = []byte(c.raw)
				for name, root := range s.Graph.Roots {
					var cursor auditedGraphCursor
					if err := json.Unmarshal(root.Application, &cursor); err != nil {
						t.Fatal(err)
					}
					cursor.Retired = true
					if c.failed {
						cursor.Kind = journal.Failed
					}
					root.Graph = retainedgraph.Root{Schema: retainedgraph.Schema, Frontier: []retainedgraph.Tree{}}
					root.Application, _ = json.Marshal(cursor)
					s.Graph.Roots[name] = root
				}
				if _, err := CheckGraphReferences(context.Background(), s.Graph); err != nil {
					t.Fatal("physical references", err)
				}
				report, err := CheckGraphJournals(context.Background(), s)
				if c.valid {
					if err != nil || report.RetiredProjectionOnly != 1 || report.Retired != 1 || report.Journals != 0 || report.Terminal != 0 || report.Entries != 0 {
						t.Fatal(report, err)
					}
				} else if err == nil {
					t.Fatal("invalid retired projection accepted", report)
				}
			})
		}
	}
}
