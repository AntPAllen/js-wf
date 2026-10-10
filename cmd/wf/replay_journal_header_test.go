package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"js-wf/journal"
)

func TestReplayJournalHeaderAmbiguity(t *testing.T) {
	const record = `{"epoch":1,"index":0,"kind":"Started","payload":{"user":1,"user":2},"worker_id":"one","sequence":1}`
	cases := []struct{ name, old, replacement string }{
		{"duplicate_epoch", `"epoch":1`, `"epoch":2,"epoch":1`},
		{"alias_epoch", `"epoch":1`, `"Epoch":2,"epoch":1`},
		{"duplicate_index", `"index":0`, `"index":99,"index":0`},
		{"alias_index", `"index":0`, `"Index":99,"index":0`},
		{"duplicate_kind", `"kind":"Started"`, `"kind":"Failed","kind":"Started"`},
		{"alias_kind", `"kind":"Started"`, `"Kind":"Failed","kind":"Started"`},
		{"duplicate_payload_header", `"payload":`, `"payload":null,"payload":`},
		{"alias_payload_header", `"payload":`, `"Payload":null,"payload":`},
		{"duplicate_worker", `"worker_id":"one"`, `"worker_id":"other","worker_id":"one"`},
		{"alias_worker", `"worker_id":"one"`, `"WORKER_ID":"other","worker_id":"one"`},
		{"duplicate_sequence", `"sequence":1`, `"sequence":99,"sequence":1`},
		{"alias_sequence", `"sequence":1`, `"Sequence":99,"sequence":1`},
		{"escaped_sequence", `"sequence":1`, `"\u0073equence":99,"sequence":1`},
	}
	load := func(t *testing.T, r string) (replayBundle, error) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "bundle.json")
		if err := os.WriteFile(path, []byte(`{"journal":[`+r+`]}`), 0600); err != nil {
			t.Fatal(err)
		}
		return loadReplayBundle(path)
	}
	t.Run("valid_opaque_payload", func(t *testing.T) {
		bundle, err := load(t, record)
		if err != nil {
			t.Fatal(err)
		}
		if len(bundle.Journal) != 1 || bundle.Journal[0].Epoch != 1 || string(bundle.Journal[0].Payload) != `{"user":1,"user":2}` {
			t.Fatalf("header or payload changed: %+v", bundle)
		}
	})
	t.Run("record_wire_shape", func(t *testing.T) {
		// Every actual Record field must be admitted by the flat header DTO.
		r := journal.Record{Entry: journal.Entry{Epoch: 3, Index: 4, Kind: journal.StepCompleted, WorkerID: "worker", Payload: json.RawMessage(`{"opaque":true}`)}, Sequence: 19}
		body, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		bundle, err := load(t, string(body))
		if err != nil {
			t.Fatal(err)
		}
		got, err := json.Marshal(bundle.Journal[0])
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != string(body) {
			t.Fatalf("wire fields changed: got %s want %s", got, body)
		}
	})
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mutated := strings.Replace(record, c.old, c.replacement, 1)
			if mutated == record {
				t.Fatal("mutation did not change control")
			}
			// The ordinary decoder accepts these and keeps the original last value.
			var ordinary journal.Record
			if err := json.Unmarshal([]byte(mutated), &ordinary); err != nil {
				t.Fatal(err)
			}
			if ordinary.Epoch != 1 || ordinary.Index != 0 || ordinary.Kind != journal.Started || ordinary.WorkerID != "one" || ordinary.Sequence != 1 {
				t.Fatal("mutation no longer demonstrates last-value acceptance")
			}
			if _, err := load(t, mutated); err == nil || !strings.Contains(err.Error(), "decode replay bundle") {
				t.Fatalf("ambiguous header admitted: %v", err)
			}
		})
	}
}
