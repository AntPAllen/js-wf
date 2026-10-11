package stepwire

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Runtime metadata cannot use null to stand in for an absent scalar. The
// completion's result is user JSON and may contain null at any depth.
func rejectNullMetadata(raw []byte, result bool) (map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, err
	}
	if fields == nil {
		return nil, fmt.Errorf("missing runtime object")
	}
	for name, value := range fields {
		if result && name == "result" {
			continue
		}
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return nil, fmt.Errorf("null runtime metadata %s", name)
		}
	}
	return fields, nil
}

func requestMetadata(raw []byte) error {
	fields, err := rejectNullMetadata(raw, false)
	if err != nil {
		return err
	}
	if cases, ok := fields["cases"]; ok {
		var items []json.RawMessage
		if err = json.Unmarshal(cases, &items); err != nil {
			return err
		}
		for _, item := range items {
			if _, err = rejectNullMetadata(item, false); err != nil {
				return err
			}
		}
	}
	return nil
}
