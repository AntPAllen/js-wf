package integrity

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Independent complete canonical signal declarations; production admission and
// readers are not used to decode their own persisted output.
type auditedGraphSignalRequest struct {
	Type       string `json:"type"`
	ID         string `json:"id"`
	Invocation uint64 `json:"invocation"`
	Name       string `json:"name"`
	Key        string `json:"key"`
}
type auditedGraphSignalInput struct {
	Schema      string                    `json:"schema"`
	Request     auditedGraphSignalRequest `json:"request"`
	Token       string                    `json:"token"`
	Index       uint64                    `json:"index"`
	InputSHA256 string                    `json:"input_sha256"`
	InputSize   int                       `json:"input_size"`
}
type auditedGraphSignalBinding struct {
	Schema   string                  `json:"schema"`
	Input    auditedGraphSignalInput `json:"input"`
	Index    uint64                  `json:"index"`
	Sequence uint64                  `json:"sequence"`
}

func auditRequiredSignalFields(raw []byte, required ...string) (map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, err
	}
	for _, field := range required {
		value, ok := fields[field]
		if !ok || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return nil, fmt.Errorf("missing/null signal field %s", field)
		}
	}
	return fields, nil
}
func auditSignalFields(raw []byte, stream string) error {
	name := "input"
	if stream == "signal-queue" {
		name = "binding"
	}
	fields, err := auditRequiredSignalFields(raw, name, "index_packet")
	if err != nil {
		return err
	}
	input := fields[name]
	if stream == "signal-queue" {
		binding, err := auditRequiredSignalFields(input, "schema", "input", "index", "sequence")
		if err != nil {
			return err
		}
		input = binding["input"]
	}
	declaration, err := auditRequiredSignalFields(input, "schema", "request", "token", "index", "input_sha256", "input_size")
	if err != nil {
		return err
	}
	_, err = auditRequiredSignalFields(declaration["request"], "type", "id", "invocation", "name", "key")
	return err
}
