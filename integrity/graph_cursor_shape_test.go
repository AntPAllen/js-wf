package integrity

import (
	"context"
	"encoding/json"
	"fmt"
	"js-wf/internal/graphpublication"
	"js-wf/internal/retainedgraph"
	"js-wf/journal"
	"testing"
)

func TestRawGraphCursorState(t *testing.T) {
	for _, enc := range []journal.Encoding{journal.JSON, journal.ProtobufV1} {
		for _, archive := range []bool{false, true} {
			for _, retired := range []bool{false, true} {
				controls := []string{"valid", "root-version", "previous-generation", "signal-repair", "unknown-forest", "retired-token", "unknown-kind", "singleton-terminal"}
				for _, control := range controls {
					t.Run(fmt.Sprintf("%s/archive=%t/retired=%t/%s", enc, archive, retired, control), func(t *testing.T) {
						s, _ := rawJournalFixture(t, enc, archive, nil, false, false)
						for name, root := range s.Graph.Roots {
							var c auditedGraphCursor
							if err := json.Unmarshal(root.Application, &c); err != nil {
								t.Fatal(err)
							}
							if retired {
								c.Retired = true
								root.Token = ""
								empty := retainedgraph.Root{Schema: retainedgraph.Schema, Frontier: []retainedgraph.Tree{}}
								root.Graph = empty
								for i := range root.Streams {
									root.Streams[i].Graph = empty
								}
							}
							switch control {
							case "root-version":
								if archive {
									root.Schema = graphpublication.StreamsSchema
									c.Schema = "js-wf-graph-journal-cursor-v1"
								} else {
									root.Schema = graphpublication.StreamsSchema
								}
							case "previous-generation":
								if archive {
									c.PreviousInvocation = c.Invocation
								} else {
									c.PreviousInvocation = 1
								}
							case "signal-repair":
								c.SignalRepair = 1
							case "unknown-forest":
								root.Schema = graphpublication.StreamsSchema
								root.Streams = append(root.Streams, graphpublication.StreamGraph{Name: "unknown", Graph: retainedgraph.Root{Schema: retainedgraph.Schema, Frontier: []retainedgraph.Tree{}}})
							case "retired-token":
								root.Token = "owner"
							case "unknown-kind":
								c.Kind = "unknown"
							case "singleton-terminal":
								c.Count = 1
								if !archive && !retired {
									c.Kind = journal.Completed
								}
							}
							root.Application, _ = json.Marshal(c)
							s.Graph.Roots[name] = root
						}
						if _, err := CheckGraphReferences(context.Background(), s.Graph); err != nil {
							t.Fatal("physical references", err)
						}
						report, err := CheckGraphJournals(context.Background(), s)
						valid := control == "valid" || control == "retired-token" && !retired
						if valid && err != nil {
							t.Fatal(report, err)
						}
						if !valid && err == nil {
							t.Fatal("invalid cursor state accepted", report)
						}
					})
				}
			}
		}
	}
}

func TestRawGraphCursorVersionCapabilities(t *testing.T) {
	for _, version := range []string{"v1", "v2", "v4", "v5", "v6"} {
		for _, control := range []string{"valid", "signal-hint", "checkpoint", "archive", "pending-signal", "purging", "input-missing"} {
			t.Run(version+"/"+control, func(t *testing.T) {
				c := auditedGraphCursor{Schema: "js-wf-graph-runtime-cursor-" + version, Invocation: 10, Count: 0}
				root := graphpublication.Root{Schema: graphpublication.StreamsSchema, Head: 1}
				if version == "v1" {
					c.Schema = "js-wf-graph-journal-cursor-v1"
					root.Schema = graphpublication.ApplicationSchema
				} else {
					c.Start = &auditedGraphStart{}
					root.Streams = []graphpublication.StreamGraph{{Name: "input", Graph: retainedgraph.Root{Count: 1}}}
				}
				valid := control == "valid"
				switch control {
				case "signal-hint":
					c.SignalSource = 1
					valid = version == "v4" || version == "v5" || version == "v6"
				case "checkpoint":
					c.Checkpoint = json.RawMessage(`null`)
				case "archive":
					root.Streams = append(root.Streams, graphpublication.StreamGraph{Name: "archive"})
					valid = version == "v6"
				case "pending-signal":
					c.Invocation = 0
					c.SignalSource = 1
				case "purging":
					c.Purging = true
				case "input-missing":
					root.Streams = nil
					valid = version == "v1"
				}
				err := auditGraphCursorShape(c, root)
				if valid && err != nil {
					t.Fatal(err)
				}
				if !valid && err == nil {
					t.Fatal("invalid capability state accepted")
				}
			})
		}
	}
}
