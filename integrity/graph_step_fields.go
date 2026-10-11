package integrity

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// This presence check is independent of the production stepwire decoder.
// Only completion result is opaque user JSON; all other fields are metadata.
func auditSDKStepFields(raw []byte, completion bool) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	if fields == nil {
		return fmt.Errorf("missing SDK step object")
	}
	for field, value := range fields {
		if completion && field == "result" {
			continue
		}
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return fmt.Errorf("null SDK metadata %s", field)
		}
	}
	if !completion {
		if cases, present := fields["cases"]; present {
			var items []json.RawMessage
			if err := json.Unmarshal(cases, &items); err != nil {
				return err
			}
			for _, item := range items {
				var descriptor map[string]json.RawMessage
				if err := json.Unmarshal(item, &descriptor); err != nil {
					return err
				}
				if descriptor == nil {
					return fmt.Errorf("null SDK case")
				}
				for field, value := range descriptor {
					if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
						return fmt.Errorf("null SDK case metadata %s", field)
					}
				}
			}
		}
	}
	return nil
}
