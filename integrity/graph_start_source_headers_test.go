package integrity

import (
	"context"
	"encoding/json"
	"fmt"
	"js-wf/internal/retainedgraph"
	"js-wf/journal"
	"strings"
	"testing"
)

func TestRawGraphStartSourceHeaders(t *testing.T) {
	for _, encoding := range []journal.Encoding{journal.JSON, journal.ProtobufV1} {
		for _, retired := range []bool{false, true} {
			for _, parent := range []bool{false, true} {
				controls := []string{"valid", "transport", "input-ref-empty", "input-ref", "pointer"}
				keys := []string{"Wf-Graph-Start-Token", "Wf-Input-SHA256"}
				parentKeys := []string{"Wf-Parent-Type", "Wf-Parent-ID", "Wf-Parent-Inv-Seq", "Wf-Parent-Signal"}
				if parent {
					keys = append(keys, parentKeys...)
				} else {
					for _, key := range parentKeys {
						controls = append(controls, key+"/unexpected-empty", key+"/unexpected-alias")
					}
				}
				for _, key := range keys {
					for _, mode := range []string{"duplicate", "conflicting", "empty-list", "alias", "missing"} {
						controls = append(controls, key+"/"+mode)
					}
				}
				for _, control := range controls {
					t.Run(fmt.Sprintf("%s/retired=%t/parent=%t/%s", encoding, retired, parent, control), func(t *testing.T) {
						fixtureControl := ""
						if parent {
							fixtureControl = "start-parent-valid"
						}
						s, _ := rawJournalCheckpointFixture(t, encoding, true, nil, false, false, fixtureControl)
						source := s.Invocations["kind.id"]
						switch control {
						case "transport":
							source.Header.Set("Nats-Expected-Last-Subject-Sequence", "0")
							source.Header.Set("X-Trace", "opaque")
						case "input-ref-empty":
							source.Header.Set("Wf-Input-Ref", "")
						case "input-ref":
							source.Header.Set("Wf-Input-Ref", "external")
						case "pointer":
							source.Data = append(source.Data, ' ')
						default:
							parts := strings.Split(control, "/")
							if len(parts) == 2 {
								key, mode := parts[0], parts[1]
								value := source.Header[key]
								switch mode {
								case "duplicate":
									source.Header[key] = append(value, value[0])
								case "conflicting":
									source.Header[key] = append(value, "conflict")
								case "empty-list":
									source.Header[key] = nil
								case "alias":
									delete(source.Header, key)
									source.Header[strings.ToLower(key)] = value
								case "missing":
									delete(source.Header, key)
								case "unexpected-empty":
									source.Header[key] = []string{""}
								case "unexpected-alias":
									source.Header[strings.ToLower(key)] = []string{""}
								}
							}
						}
						if retired {
							for name, root := range s.Graph.Roots {
								var cursor auditedGraphCursor
								if err := json.Unmarshal(root.Application, &cursor); err != nil {
									t.Fatal(err)
								}
								cursor.Retired = true
								empty := retainedgraph.Root{Schema: retainedgraph.Schema, Frontier: []retainedgraph.Tree{}}
								root.Graph = empty
								for i := range root.Streams {
									root.Streams[i].Graph = empty
								}
								root.Application, _ = json.Marshal(cursor)
								s.Graph.Roots[name] = root
							}
						}
						if _, err := CheckGraphReferences(context.Background(), s.Graph); err != nil {
							t.Fatal("physical references", err)
						}
						report, err := CheckGraphJournals(context.Background(), s)
						valid := control == "valid" || control == "transport"
						if valid && err != nil {
							t.Fatal(report, err)
						}
						if valid && retired && (report.RetiredProjectionOnly != 1 || report.Journals != 0) {
							t.Fatal(report)
						}
						if !valid && err == nil {
							t.Fatal("invalid canonical source accepted", report)
						}
					})
				}
			}
		}
	}
}
