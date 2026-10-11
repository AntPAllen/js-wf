package integrity

import (
	"context"
	"encoding/json"
	"testing"

	"js-wf/internal/retainedgraph"
	"js-wf/journal"
)

func TestRawGraphRetiredSourceProjectionScope(t *testing.T) {
	for _, control := range []string{"valid", "purging-source", "source-generation", "projection-missing", "projection-generation", "projection-kind", "nonterminal", "live-forest"} {
		t.Run(control, func(t *testing.T) {
			s, projections := rawJournalFixture(t, journal.JSON, false, nil, false, false)
			for key, root := range s.Graph.Roots {
				var cursor auditedGraphCursor
				if err := json.Unmarshal(root.Application, &cursor); err != nil {
					t.Fatal(err)
				}
				cursor.Retired = true
				if control != "live-forest" {
					root.Graph = retainedgraph.Root{Schema: retainedgraph.Schema, Frontier: []retainedgraph.Tree{}}
				}
				switch control {
				case "purging-source":
					cursor.Purging = true
				case "source-generation":
					s.Invocations["kind.id"].Sequence++
				case "projection-missing":
					delete(projections, "kind.id")
				case "projection-generation":
					projections["kind.id"] = []byte(`{"inv_seq":11}`)
				case "projection-kind":
					projections["kind.id"] = []byte(`{"inv_seq":10,"error":"unexpected failure"}`)
				case "nonterminal":
					cursor.Kind = journal.Started
				}
				root.Application, _ = json.Marshal(cursor)
				s.Graph.Roots[key] = root
			}
			report, err := CheckGraphJournals(context.Background(), s)
			if control == "valid" {
				if err != nil || report.Invocations != 1 || report.Retired != 1 || report.RetiredProjectionOnly != 1 || report.Journals != 0 || report.Terminal != 0 || report.Entries != 0 {
					t.Fatal(report, err)
				}
			} else if err == nil {
				t.Fatal("invalid retired source accepted", report)
			}
		})
	}
}
