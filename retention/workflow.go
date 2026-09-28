package retention

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"js-wf/identity"
	"js-wf/wf"

	"github.com/nats-io/nats.go/jetstream"
)

type Request struct {
	Type string `json:"type"`
	ID   string `json:"id"`
}

// Handler runs the ordered purge as a durable workflow. Register its returned
// function under a separate workflow type such as "retention". A crash during
// Purge replays its unfinished Run step and resumes the idempotent pipeline.
func Handler(js jetstream.JetStream, grace time.Duration) func(*wf.Context, json.RawMessage) (json.RawMessage, error) {
	return func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
		var req Request
		if err := json.Unmarshal(input, &req); err != nil {
			return nil, err
		}
		if err := identity.Validate(req.Type, req.ID); err != nil {
			return nil, err
		}
		if grace <= 0 {
			return nil, fmt.Errorf("tombstone grace must be positive")
		}
		for attempt := 0; attempt < 10000; attempt++ {
			name := fmt.Sprintf("purge-%d", attempt)
			declared := struct {
				Request Request `json:"request"`
				Grace   int64   `json:"grace_nanos"`
			}{req, int64(grace)}
			done, err := wf.Run(c, name, declared, func(ctx context.Context) (bool, error) {
				err := Purge(ctx, js, req.Type, req.ID, grace)
				if errors.Is(err, ErrActive) || errors.Is(err, ErrNotTerminal) {
					return false, nil
				}
				return err == nil, err
			})
			if err != nil {
				return nil, err
			}
			if done {
				return json.RawMessage(`true`), nil
			}
			if err := wf.Sleep(c, fmt.Sprintf("purge-wait-%d", attempt), time.Second); err != nil {
				return nil, err
			}
		}
		return nil, fmt.Errorf("retention workflow exceeded retry limit")
	}
}
