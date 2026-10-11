package integrity

import (
	"context"
	"encoding/json"
	"js-wf/journal"
	"testing"
)

func TestRawGraphTerminalEnvelope(t *testing.T) {
	controls := []struct {
		name, raw     string
		valid, failed bool
	}{
		{"ordinary", `{"inv_seq":10,"result":"NDI="}`, true, false},
		{"opaque-result", `{"inv_seq":10,"result":"eyJVbmtub3duIjoxfQ=="}`, true, false},
		{"failed", `{"inv_seq":10,"error":"failure"}`, true, true},
		{"limit-request", `{"inv_seq":10,"error":"journal too long","limit_request":{"kind":"run","name":"extra"}}`, true, true},
		{"limit-entry", `{"inv_seq":10,"error":"journal too long","limit_entry":{"kind":"Suspended","payload":{"waiting_on":"timer:clock"}}}`, true, true},
		{"unknown", `{"inv_seq":10,"result":"NDI=","unknown":1}`, false, false},
		{"alias", `{"inv_seq":10,"Result":"NDI="}`, false, false},
		{"duplicate-invocation", `{"inv_seq":9,"inv_seq":10,"result":"NDI="}`, false, false},
		{"escaped-duplicate", `{"inv_seq":9,"\u0069nv_seq":10,"result":"NDI="}`, false, false},
		{"duplicate-result", `{"inv_seq":10,"result":"Nw==","result":"NDI="}`, false, false},
		{"duplicate-error", `{"inv_seq":10,"error":"failure","error":"","result":"NDI="}`, false, false},
		{"duplicate-limit-request", `{"inv_seq":10,"limit_request":{},"limit_request":null,"result":"NDI="}`, false, false},
		{"duplicate-limit-entry", `{"inv_seq":10,"limit_entry":{},"limit_entry":null,"result":"NDI="}`, false, false},
		{"limit-entry-unknown", `{"inv_seq":10,"error":"failure","limit_entry":{"kind":"Suspended","payload":{},"unknown":1}}`, false, true},
		{"limit-entry-alias", `{"inv_seq":10,"error":"failure","limit_entry":{"Kind":"Suspended","payload":{}}}`, false, true},
		{"limit-entry-duplicate", `{"inv_seq":10,"error":"failure","limit_entry":{"kind":"Attempt","kind":"Suspended","payload":{}}}`, false, true},
	}
	for _, encoding := range []journal.Encoding{journal.JSON, journal.ProtobufV1} {
		for _, c := range controls {
			t.Run(string(encoding)+"/"+c.name, func(t *testing.T) {
				s, _ := rawJournalFixture(t, encoding, false, func(e []journal.Entry) {
					e[3].Payload = json.RawMessage(c.raw)
					if c.failed {
						e[3].Kind = journal.Failed
					}
				}, false, false)
				if _, err := CheckGraphReferences(context.Background(), s.Graph); err != nil {
					t.Fatal("invalid physical references", err)
				}
				_, err := CheckGraphJournals(context.Background(), s)
				if c.valid && err != nil {
					t.Fatal(err)
				}
				if !c.valid && err == nil {
					t.Fatal("ambiguous terminal accepted")
				}
			})
		}
	}
}
