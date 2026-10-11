package integrity

import (
	"encoding/json"
	"fmt"
	"testing"

	"js-wf/internal/retainedgraph"
	"js-wf/journal"
)

func TestRawGraphCursorFieldPresence(t *testing.T) {
	required := []string{"schema", "invocation", "base", "count", "epoch", "kind", "retired"}
	optional := []string{"purging", "start", "previous_invocation", "signal_inputs", "signal_bindings", "signal_source", "signal_consumed", "signal_repair", "retained_from", "checkpoint"}
	controls := []string{"valid", "optional-zero"}
	for _, field := range required {
		controls = append(controls, "missing-"+field, "null-"+field)
	}
	for _, field := range optional {
		controls = append(controls, "null-"+field)
	}
	for _, enc := range []journal.Encoding{journal.JSON, journal.ProtobufV1} {
		for _, archive := range []bool{false, true} {
			for _, retired := range []bool{false, true} {
				for _, control := range controls {
					t.Run(fmt.Sprintf("%s/archive=%t/retired=%t/%s", enc, archive, retired, control), func(t *testing.T) {
						s, _ := rawJournalFixture(t, enc, archive, nil, false, false)
						for name, root := range s.Graph.Roots {
							var fields map[string]json.RawMessage
							if err := json.Unmarshal(root.Application, &fields); err != nil {
								t.Fatal(err)
							}
							if retired {
								fields["retired"] = json.RawMessage(`true`)
								root.Token = ""
								empty := retainedgraph.Root{Schema: retainedgraph.Schema, Frontier: []retainedgraph.Tree{}}
								root.Graph = empty
								for i := range root.Streams {
									root.Streams[i].Graph = empty
								}
							}
							switch control {
							case "valid":
							case "optional-zero":
								// Omitted optional counters and explicitly encoded zero mean the same thing.
								fields["signal_repair"] = json.RawMessage(`0`)
								fields["purging"] = json.RawMessage(`false`)
							default:
								if len(control) > 8 && control[:8] == "missing-" {
									delete(fields, control[8:])
								} else {
									fields[control[5:]] = json.RawMessage(`null`)
								}
							}
							root.Application, _ = json.Marshal(fields)
							s.Graph.Roots[name] = root
						}
						assertRawEnvelopeVerdict(t, s, control == "valid" || control == "optional-zero")
					})
				}
			}
		}
	}
}
