package integrity

import (
	"encoding/json"
	"fmt"
	"js-wf/journal"
	"strings"
	"testing"
)

func consumedSignalControls(level string) []string {
	controls := []string{"valid", "whitespace", "unknown", "alias", "duplicate", "escaped-duplicate"}
	required := []string{"sig_seq", "name"}
	nullable := []string{"sig_seq", "name", "payload", "ref", "hash", "graph_child", "canonical_signal"}
	if level == "canonical" {
		required = []string{"index", "token"}
		nullable = required
	}
	for _, field := range required {
		controls = append(controls, "missing-"+field)
	}
	for _, field := range nullable {
		controls = append(controls, "null-"+field)
	}
	return controls
}
func mutateConsumedSignalEnvelope(t *testing.T, raw []byte, control string) []byte {
	t.Helper()
	level, mode, _ := strings.Cut(control, "/")
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	data := raw
	if level == "canonical" {
		data = fields["canonical_signal"]
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(data, &obj); err != nil {
		t.Fatal(err)
	}
	first := "sig_seq"
	if level == "canonical" {
		first = "index"
	}
	switch mode {
	case "valid":
	case "whitespace":
		data = append(append([]byte(" \n"), data...), []byte("\n ")...)
	case "unknown":
		obj["unexpected"] = json.RawMessage(`1`)
		data, _ = json.Marshal(obj)
	case "alias":
		obj[strings.ToUpper(first[:1])+first[1:]] = obj[first]
		delete(obj, first)
		data, _ = json.Marshal(obj)
	case "duplicate", "escaped-duplicate":
		key := first
		if mode == "escaped-duplicate" {
			if level == "canonical" {
				key = `\u0069ndex`
			} else {
				key = `\u0073ig_seq`
			}
		}
		data = []byte(strings.TrimSuffix(strings.TrimSpace(string(data)), "}") + `,"` + key + `":` + string(obj[first]) + `}`)
	default:
		if strings.HasPrefix(mode, "missing-") {
			delete(obj, strings.TrimPrefix(mode, "missing-"))
		} else {
			obj[strings.TrimPrefix(mode, "null-")] = json.RawMessage(`null`)
		}
		data, _ = json.Marshal(obj)
	}
	if level == "canonical" {
		fields["canonical_signal"] = data
		data, _ = json.Marshal(fields)
	}
	return data
}
func TestRawGraphConsumedSignalEnvelope(t *testing.T) {
	for _, enc := range []journal.Encoding{journal.JSON, journal.ProtobufV1} {
		for _, level := range []string{"root", "canonical"} {
			for _, control := range consumedSignalControls(level) {
				t.Run(fmt.Sprintf("%s/%s/%s", enc, level, control), func(t *testing.T) {
					s := rawSignalFixture(t, enc, "consumed-wire/"+level+"/"+control)
					assertRawEnvelopeVerdict(t, s, control == "valid" || control == "whitespace")
				})
			}
		}
	}
}
