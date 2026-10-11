package integrity

import (
	"encoding/json"
	"fmt"
	"js-wf/journal"
	"strings"
	"testing"
)

func signalEnvelopeFields(stream string) map[string][]string {
	top := "input"
	if stream == "signal-queue" {
		top = "binding"
	}
	result := map[string][]string{"root": {top, "index_packet"}, "input": {"schema", "request", "index", "token", "input_sha256", "input_size"}, "request": {"type", "id", "invocation", "name", "key"}}
	if stream == "signal-queue" {
		result["binding"] = []string{"schema", "input", "index", "sequence"}
	}
	return result
}

func mutateSignalEnvelope(t *testing.T, raw []byte, stream, control string) []byte {
	t.Helper()
	level, mode, _ := strings.Cut(control, "/")
	path := []string{}
	if stream == "signal-queue" && level != "root" {
		path = append(path, "binding")
	}
	if level == "input" || level == "request" {
		path = append(path, "input")
	}
	if level == "request" {
		path = append(path, "request")
	}
	var mutate func([]byte, []string) []byte
	mutate = func(data []byte, path []string) []byte {
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(data, &obj); err != nil {
			t.Fatal(err)
		}
		if len(path) > 0 {
			obj[path[0]] = mutate(obj[path[0]], path[1:])
			out, _ := json.Marshal(obj)
			return out
		}
		first := signalEnvelopeFields(stream)[level][0]
		switch mode {
		case "valid":
			return data
		case "whitespace":
			return append(append([]byte(" \n"), data...), []byte("\n ")...)
		case "unknown":
			obj["unexpected"] = json.RawMessage(`1`)
		case "alias":
			obj[strings.ToUpper(first[:1])+first[1:]] = obj[first]
			delete(obj, first)
		case "duplicate":
			return []byte(strings.TrimSuffix(strings.TrimSpace(string(data)), "}") + `,"` + first + `":` + string(obj[first]) + `}`)
		default:
			if strings.HasPrefix(mode, "missing-") {
				delete(obj, strings.TrimPrefix(mode, "missing-"))
			} else {
				obj[strings.TrimPrefix(mode, "null-")] = json.RawMessage(`null`)
			}
		}
		out, _ := json.Marshal(obj)
		return out
	}
	return mutate(raw, path)
}

func TestRawGraphSignalEnvelope(t *testing.T) {
	for _, enc := range []journal.Encoding{journal.JSON, journal.ProtobufV1} {
		for _, stream := range []string{"signal-input", "signal-queue"} {
			for _, level := range []string{"root", "binding", "input", "request"} {
				fields, ok := signalEnvelopeFields(stream)[level]
				if !ok {
					continue
				}
				controls := []string{"valid", "whitespace", "unknown", "alias", "duplicate"}
				for _, field := range fields {
					controls = append(controls, "missing-"+field, "null-"+field)
				}
				for _, control := range controls {
					t.Run(fmt.Sprintf("%s/%s/%s/%s", enc, stream, level, control), func(t *testing.T) {
						s := rawSignalFixture(t, enc, "wire-"+stream+"/"+level+"/"+control)
						assertRawEnvelopeVerdict(t, s, control == "valid" || control == "whitespace")
					})
				}
			}
		}
	}
}
