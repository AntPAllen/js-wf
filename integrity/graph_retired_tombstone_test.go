package integrity

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"js-wf/internal/retainedgraph"
	"js-wf/journal"
)

func TestRawGraphRetiredPurgeTombstones(t *testing.T) {
	valid := `{"tombstone":true,"inv_seq":10,"purged_at":"2026-10-11T00:00:00Z","expires_at":"2026-10-11T01:00:00Z"}`
	cases := []struct {
		name, raw                string
		source, unfenced, accept bool
	}{
		{"valid", valid, false, false, true},
		{"whitespace", " \n" + valid + "\n", false, false, true},
		{"retained_source", valid, true, false, false},
		{"no_purge_fence", valid, false, true, false},
		{"false_marker", strings.Replace(valid, `true`, `false`, 1), false, false, false},
		{"null_marker", strings.Replace(valid, `true`, `null`, 1), false, false, false},
		{"missing_marker", strings.Replace(valid, `"tombstone":true,`, "", 1), false, false, false},
		{"wrong_generation", strings.Replace(valid, `"inv_seq":10`, `"inv_seq":11`, 1), false, false, false},
		{"zero_generation", strings.Replace(valid, `"inv_seq":10`, `"inv_seq":0`, 1), false, false, false},
		{"null_generation", strings.Replace(valid, `"inv_seq":10`, `"inv_seq":null`, 1), false, false, false},
		{"missing_generation", strings.Replace(valid, `"inv_seq":10,`, "", 1), false, false, false},
		{"zero_purge_time", strings.Replace(valid, `2026-10-11T00:00:00Z`, `0001-01-01T00:00:00Z`, 1), false, false, false},
		{"null_purge_time", strings.Replace(valid, `"2026-10-11T00:00:00Z"`, `null`, 1), false, false, false},
		{"missing_purge_time", strings.Replace(valid, `"purged_at":"2026-10-11T00:00:00Z",`, "", 1), false, false, false},
		{"null_expiry", strings.Replace(valid, `"2026-10-11T01:00:00Z"`, `null`, 1), false, false, false},
		{"missing_expiry", strings.Replace(valid, `,"expires_at":"2026-10-11T01:00:00Z"`, "", 1), false, false, false},
		{"equal_expiry", strings.Replace(valid, `2026-10-11T01:00:00Z`, `2026-10-11T00:00:00Z`, 1), false, false, false},
		{"earlier_expiry", strings.Replace(valid, `2026-10-11T01:00:00Z`, `2026-10-10T23:00:00Z`, 1), false, false, false},
		{"alias_marker", strings.Replace(valid, `"tombstone"`, `"Tombstone"`, 1), false, false, false},
		{"duplicate_marker", strings.Replace(valid, `"tombstone":true`, `"tombstone":false,"tombstone":true`, 1), false, false, false},
		{"duplicate_generation", strings.Replace(valid, `"inv_seq":10`, `"inv_seq":9,"inv_seq":10`, 1), false, false, false},
		{"duplicate_time", strings.Replace(valid, `"purged_at":`, `"purged_at":"2026-10-10T00:00:00Z","purged_at":`, 1), false, false, false},
		{"foreign_result", strings.Replace(valid, `"tombstone":true`, `"tombstone":true,"result":"NDI="`, 1), false, false, false},
		{"retained_outcome_compatibility", `{"inv_seq":10,"result":"NDI="}`, false, false, true},
	}
	for _, encoding := range []journal.Encoding{journal.JSON, journal.ProtobufV1} {
		for _, tc := range cases {
			t.Run(string(encoding)+"/"+tc.name, func(t *testing.T) {
				snapshot, projections := rawJournalFixture(t, encoding, false, nil, false, false)
				projections["kind.id"] = []byte(tc.raw)
				if !tc.source {
					for key := range snapshot.Invocations {
						delete(snapshot.Invocations, key)
					}
				}
				for destination, root := range snapshot.Graph.Roots {
					var cursor auditedGraphCursor
					if err := json.Unmarshal(root.Application, &cursor); err != nil {
						t.Fatal(err)
					}
					cursor.Retired = true
					cursor.Purging = !tc.unfenced
					root.Token = ""
					root.Graph = retainedgraph.Root{Schema: retainedgraph.Schema, Frontier: []retainedgraph.Tree{}}
					root.Application, _ = json.Marshal(cursor)
					snapshot.Graph.Roots[destination] = root
				}
				if _, err := CheckGraphReferences(context.Background(), snapshot.Graph); err != nil {
					t.Fatal(err)
				}
				report, err := CheckGraphJournals(context.Background(), snapshot)
				if tc.accept {
					tombstones, projections := 1, 0
					if tc.name == "retained_outcome_compatibility" {
						tombstones, projections = 0, 1
					}
					if err != nil || report.Retired != 1 || report.RetiredTombstones != tombstones || report.RetiredProjectionOnly != projections || report.Invocations != 0 || report.Journals != 0 {
						t.Fatal(report, err)
					}
				} else if err == nil {
					t.Fatal("invalid tombstone accepted", report)
				}
			})
		}
	}
}
