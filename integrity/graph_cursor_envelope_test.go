package integrity

import (
	"context"
	"encoding/json"
	"fmt"
	"js-wf/journal"
	"strings"
	"testing"
)

func TestRawGraphCursorEnvelope(t *testing.T) {
	for _, encoding := range []journal.Encoding{journal.JSON, journal.ProtobufV1} {
		for _, archive := range []bool{false, true} {
			for _, control := range []string{"valid", "whitespace", "unknown", "alias", "duplicate-schema", "duplicate-invocation", "escaped-count", "count-fraction", "invocation-negative"} {
				t.Run(fmt.Sprintf("%s/archive=%t/%s", encoding, archive, control), func(t *testing.T) {
					s, _ := rawJournalFixture(t, encoding, archive, nil, false, false)
					for name, root := range s.Graph.Roots {
						raw := string(root.Application)
						switch control {
						case "whitespace":
							raw = " \n" + raw + "\n "
						case "unknown":
							raw = strings.TrimSuffix(raw, "}") + `,"unknown":1}`
						case "alias":
							raw = strings.Replace(raw, `"count":`, `"Count":`, 1)
						case "duplicate-schema":
							raw = strings.TrimSuffix(raw, "}") + `,"schema":"` + map[bool]string{false: "js-wf-graph-journal-cursor-v1", true: "js-wf-graph-runtime-cursor-v6"}[archive] + `"}`
						case "duplicate-invocation":
							raw = strings.TrimSuffix(raw, "}") + `,"invocation":10}`
						case "escaped-count":
							raw = strings.TrimSuffix(raw, "}") + `,"\u0063ount":4}`
						case "count-fraction":
							raw = strings.Replace(raw, `"count":4`, `"count":4.5`, 1)
						case "invocation-negative":
							raw = strings.Replace(raw, `"invocation":10`, `"invocation":-10`, 1)
						}
						root.Application = json.RawMessage(raw)
						s.Graph.Roots[name] = root
					}
					assertRawEnvelopeVerdict(t, s, control == "valid" || control == "whitespace")
				})
			}
		}
	}
}

func TestRawGraphStartEnvelope(t *testing.T) {
	for _, encoding := range []journal.Encoding{journal.JSON, journal.ProtobufV1} {
		for _, control := range []string{"valid", "cursor-unknown", "cursor-alias", "cursor-duplicate", "cursor-request-unknown", "cursor-request-alias", "cursor-request-duplicate", "start-wire-unknown", "start-wire-alias", "start-wire-duplicate", "start-wire-escaped", "start-wire-request-unknown", "start-wire-request-alias", "start-wire-request-duplicate", "start-parent-valid", "start-parent-type", "start-parent-id", "start-parent-generation", "start-parent-signal"} {
			t.Run(string(encoding)+"/"+control, func(t *testing.T) {
				s, _ := rawJournalCheckpointFixture(t, encoding, true, nil, false, false, control)
				if strings.HasPrefix(control, "cursor-") {
					for name, root := range s.Graph.Roots {
						raw := string(root.Application)
						switch control {
						case "cursor-unknown":
							raw = strings.Replace(raw, `"start":{`, `"start":{"unknown":1,`, 1)
						case "cursor-alias":
							raw = strings.Replace(raw, `"input_size":`, `"Input_size":`, 1)
						case "cursor-duplicate":
							raw = strings.Replace(raw, `"start":{`, `"start":{"input_size":5,`, 1)
						case "cursor-request-unknown":
							raw = strings.Replace(raw, `"request":{`, `"request":{"unknown":1,`, 1)
						case "cursor-request-alias":
							raw = strings.Replace(raw, `"type":`, `"Type":`, 1)
						case "cursor-request-duplicate":
							raw = strings.Replace(raw, `"request":{`, `"request":{"id":"id",`, 1)
						}
						root.Application = json.RawMessage(raw)
						s.Graph.Roots[name] = root
					}
				}
				assertRawEnvelopeVerdict(t, s, control == "valid" || control == "start-parent-valid")
			})
		}
	}
}

func assertRawEnvelopeVerdict(t *testing.T, s GraphJournalSnapshot, valid bool) {
	t.Helper()
	if _, err := CheckGraphReferences(context.Background(), s.Graph); err != nil {
		t.Fatal("physical references", err)
	}
	_, err := CheckGraphJournals(context.Background(), s)
	if valid && err != nil {
		t.Fatal(err)
	}
	if !valid && err == nil {
		t.Fatal("invalid cursor/start metadata accepted")
	}
}
