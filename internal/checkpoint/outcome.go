package checkpoint

import (
	"bytes"
	"encoding/json"
)

// DecodeOutcomeEnvelope admits runtime outcome metadata without interpreting
// encoded user results or rejected request/payload contents. Callers supply the
// typed schema and enforce generation, kind and result-reference semantics.
// Present metadata cannot be null; absent legacy optional fields remain valid.
func DecodeOutcomeEnvelope(raw []byte, target any) error {
	if DecodeUnambiguous(raw, target) != nil {
		return invalid("outcome envelope")
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || fields == nil {
		return invalid("outcome envelope")
	}
	check := func(fields map[string]json.RawMessage) error {
		for _, value := range fields {
			if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
				return invalid("outcome envelope")
			}
		}
		return nil
	}
	if err := check(fields); err != nil {
		return err
	}
	if rawEntry, ok := fields["limit_entry"]; ok {
		var entry map[string]json.RawMessage
		if json.Unmarshal(rawEntry, &entry) != nil || entry == nil {
			return invalid("outcome envelope")
		}
		return check(entry)
	}
	return nil
}
