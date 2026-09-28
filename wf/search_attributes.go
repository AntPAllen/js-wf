package wf

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"unicode/utf8"

	"js-wf/identity"
)

const (
	MaxSearchAttributes     = 16
	MaxSearchAttributeKey   = 64
	MaxSearchAttributeValue = 256
	MaxSearchAttributeBytes = 4096
)

// ValidateSearchAttributes bounds journal and KV index growth. Keys are safe
// NATS tokens; values are UTF-8 strings and may contain spaces or punctuation.
func ValidateSearchAttributes(attributes map[string]string) error {
	if len(attributes) > MaxSearchAttributes {
		return fmt.Errorf("search attributes exceed %d keys", MaxSearchAttributes)
	}
	for key, value := range attributes {
		if err := identity.ValidateToken(key); err != nil || len(key) > MaxSearchAttributeKey {
			return fmt.Errorf("invalid search attribute key %q", key)
		}
		if len(value) > MaxSearchAttributeValue || !utf8.ValidString(value) {
			return fmt.Errorf("invalid search attribute value for %q", key)
		}
	}
	encoded, err := json.Marshal(attributes)
	if err != nil || len(encoded) > MaxSearchAttributeBytes {
		return fmt.Errorf("search attributes exceed %d encoded bytes", MaxSearchAttributeBytes)
	}
	return nil
}

// SetSearchAttributes replaces the invocation's searchable attributes. The
// complete map is journaled so replay and projection rebuild reach the same
// value; an empty map clears all attributes.
func (c *Context) SetSearchAttributes(attributes map[string]string) error {
	if err := ValidateSearchAttributes(attributes); err != nil {
		return err
	}
	declared := make(map[string]string, len(attributes))
	maps.Copy(declared, attributes)
	recorded, err := runWithKind(c, "search_attributes", "set", declared, func(context.Context) (map[string]string, error) {
		return declared, nil
	})
	if err != nil {
		return err
	}
	if !maps.Equal(recorded, declared) {
		return ErrCorruptJournal
	}
	return nil
}
