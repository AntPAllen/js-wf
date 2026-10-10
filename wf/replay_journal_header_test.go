package wf

import (
	"encoding/json"
	"errors"
	"js-wf/journal"
	"strings"
	"testing"
)

func TestReplayJournalHeadersBeforeHandler(t *testing.T) {
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

	check := func(t *testing.T, first string, reject bool) {
		t.Helper()
		raw := []byte(`[` + first + `,{"epoch":1,"index":1,"kind":"Completed","payload":{"inv_seq":1,"result":"NDI="},"worker_id":"one","sequence":2}]`)
		calls := 0
		result, err := Replay(raw, func(*Context) (int, error) { calls++; return 42, nil }, ReplayOptions{Type: "parent", ID: "id", InvSeq: 1})
		if reject {
			if !errors.Is(err, ErrCorruptJournal) || calls != 0 {
				t.Fatalf("ambiguous journal reached handler: calls=%d err=%v", calls, err)
			}
		} else if err != nil || calls != 1 || result != 42 {
			t.Fatalf("valid replay result=%d calls=%d err=%v", result, calls, err)
		}
	}
	t.Run("valid_opaque_payload", func(t *testing.T) { check(t, record, false) })
	t.Run("unknown_header", func(t *testing.T) {
		check(t, strings.Replace(record, `"epoch":1`, `"foreign":true,"epoch":1`, 1), true)
	})
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mutated := strings.Replace(record, c.old, c.replacement, 1)
			if mutated == record {
				t.Fatal("mutation did not change control")
			}
			var ordinary journal.Record
			if err := json.Unmarshal([]byte(mutated), &ordinary); err != nil {
				t.Fatal(err)
			}
			if ordinary.Epoch != 1 || ordinary.Index != 0 || ordinary.Kind != journal.Started || ordinary.WorkerID != "one" || ordinary.Sequence != 1 {
				t.Fatal("ordinary decoder no longer preserves final header values")
			}
			check(t, mutated, true)
		})
	}
}
