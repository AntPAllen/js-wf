package integrity

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"js-wf/internal/retainedgraph"
	"js-wf/journal"
)

func startPresenceControls() []string {
	controls := []string{"valid", "whitespace", "optional-zero"}
	for _, f := range []string{"schema", "token", "request", "input_sha256", "input_size", "request.type", "request.id"} {
		controls = append(controls, "missing-"+f, "null-"+f)
	}
	for _, f := range []string{"parent_type", "parent_id", "parent_invocation", "signal_name"} {
		controls = append(controls, "null-request."+f)
	}
	return controls
}

func mutateStartPresence(t *testing.T, raw []byte, control string) []byte {
	t.Helper()
	var start map[string]json.RawMessage
	if err := json.Unmarshal(raw, &start); err != nil {
		t.Fatal(err)
	}
	if control == "valid" {
		return raw
	}
	if control == "whitespace" {
		return append(append([]byte(" \n"), raw...), []byte("\n ")...)
	}
	var request map[string]json.RawMessage
	if err := json.Unmarshal(start["request"], &request); err != nil {
		t.Fatal(err)
	}
	if control == "optional-zero" {
		request["parent_invocation"] = json.RawMessage(`0`)
	} else {
		mode, field, _ := strings.Cut(control, "-")
		target := start
		if strings.HasPrefix(field, "request.") {
			field = strings.TrimPrefix(field, "request.")
			target = request
		}
		if mode == "missing" {
			delete(target, field)
		} else {
			target[field] = json.RawMessage(`null`)
		}
	}
	if _, ok := start["request"]; ok && string(start["request"]) != "null" {
		start["request"], _ = json.Marshal(request)
	}
	out, err := json.Marshal(start)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestRawGraphStartFieldPresence(t *testing.T) {
	for _, enc := range []journal.Encoding{journal.JSON, journal.ProtobufV1} {
		for _, scope := range []string{"cursor-live", "cursor-retired", "owned-input"} {
			for _, control := range startPresenceControls() {
				t.Run(fmt.Sprintf("%s/%s/%s", enc, scope, control), func(t *testing.T) {
					fixtureControl := ""
					if scope == "owned-input" {
						fixtureControl = "start-presence-" + control
					}
					s, _ := rawJournalCheckpointFixture(t, enc, true, nil, false, false, fixtureControl)
					if scope != "owned-input" {
						for name, root := range s.Graph.Roots {
							var fields map[string]json.RawMessage
							if err := json.Unmarshal(root.Application, &fields); err != nil {
								t.Fatal(err)
							}
							fields["start"] = mutateStartPresence(t, fields["start"], control)
							if scope == "cursor-retired" {
								fields["retired"] = json.RawMessage(`true`)
								root.Token = ""
								empty := retainedgraph.Root{Schema: retainedgraph.Schema, Frontier: []retainedgraph.Tree{}}
								root.Graph = empty
								for i := range root.Streams {
									root.Streams[i].Graph = empty
								}
							}
							root.Application, _ = json.Marshal(fields)
							s.Graph.Roots[name] = root
						}
					}
					assertRawEnvelopeVerdict(t, s, control == "valid" || control == "whitespace" || control == "optional-zero")
				})
			}
		}
	}
}
