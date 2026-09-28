package wf

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"js-wf/identity"
)

type stateValue struct {
	Found bool            `json:"found"`
	Value json.RawMessage `json:"value,omitempty"`
}

// SetState records a value before it becomes visible to later GetState calls.
// Replaying the workflow applies the recorded writes in the same call order.
func (c *Context) SetState(key string, value any) error {
	if err := identity.ValidateToken(key); err != nil {
		return err
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("state value: %w", err)
	}
	recorded, err := runWithKind(c, "state_set", key, json.RawMessage(raw), func(context.Context) (stateValue, error) {
		return stateValue{Found: true, Value: raw}, nil
	})
	if err != nil {
		return err
	}
	if !recorded.Found || !bytes.Equal(recorded.Value, raw) {
		return ErrCorruptJournal
	}
	c.state[key] = append(json.RawMessage(nil), recorded.Value...)
	return nil
}

// GetState records the value observed at this point in workflow execution.
// out must be a JSON-unmarshalable pointer when the key exists.
func (c *Context) GetState(key string, out any) (bool, error) {
	if err := identity.ValidateToken(key); err != nil {
		return false, err
	}
	current, found := c.state[key]
	recorded, err := runWithKind(c, "state_get", key, nil, func(context.Context) (stateValue, error) {
		return stateValue{Found: found, Value: current}, nil
	})
	if err != nil {
		return false, err
	}
	if recorded.Found != found || !bytes.Equal(recorded.Value, current) {
		return false, ErrCorruptJournal
	}
	if !found {
		return false, nil
	}
	if err := json.Unmarshal(recorded.Value, out); err != nil {
		return false, err
	}
	return true, nil
}
