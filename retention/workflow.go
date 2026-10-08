package retention

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"js-wf/identity"
	"js-wf/journal"
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
	return purgeHandler(grace, false, func(ctx context.Context, typ, id string) error { return Purge(ctx, js, typ, id, grace) })
}

// GraphHandler dogfoods the ordered canonical graph purge pipeline. Register it
// separately from legacy retention; its declared Run input records graph mode.
func GraphHandler(js jetstream.JetStream, graph *journal.GraphStore, grace time.Duration) func(*wf.Context, json.RawMessage) (json.RawMessage, error) {
	return func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
		if graph == nil || grace <= 0 {
			return nil, fmt.Errorf("invalid graph retention workflow configuration")
		}
		var req Request
		if err := json.Unmarshal(input, &req); err != nil {
			return nil, err
		}
		if err := identity.Validate(req.Type, req.ID); err != nil {
			return nil, err
		}
		lookup := func(ctx context.Context) (uint64, error) {
			stream, e := js.Stream(ctx, "WF_INV")
			if e != nil {
				return 0, e
			}
			invocation, e := stream.GetLastMsgForSubject(ctx, identity.InvocationSubject(req.Type, req.ID))
			if errors.Is(e, jetstream.ErrMsgNotFound) {
				status, e := graph.InspectRetirement(ctx, req.Type, req.ID)
				if e != nil {
					return 0, e
				}
				if status.Purging && status.Retired {
					return status.Invocation, nil
				}
				return 0, ErrNotFound
			}
			if e != nil {
				return 0, e
			}
			if invocation.Sequence == 0 {
				return 0, journal.ErrStale
			}
			return invocation.Sequence, nil
		}
		var generation uint64
		for attempt := 0; attempt < 10000; attempt++ {
			seq, err := wf.Run(c, fmt.Sprintf("purge-target-%d", attempt), req, func(ctx context.Context) (uint64, error) {
				seq, err := lookup(ctx)
				if graphPurgeRetryable(err) {
					return 0, nil
				}
				return seq, err
			})
			if err != nil {
				return nil, err
			}
			if seq != 0 {
				generation = seq
				break
			}
			if err := wf.Sleep(c, fmt.Sprintf("purge-target-wait-%d", attempt), time.Second); err != nil {
				return nil, err
			}
		}
		if generation == 0 {
			return nil, fmt.Errorf("retention target lookup exceeded retry limit")
		}
		return purgeHandler(grace, true, func(ctx context.Context, typ, id string) error {
			return PurgeGraphInvocation(ctx, js, graph, typ, id, generation, grace)
		})(c, input)
	}
}

func purgeHandler(grace time.Duration, graph bool, purge func(context.Context, string, string) error) func(*wf.Context, json.RawMessage) (json.RawMessage, error) {
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
				Graph   bool    `json:"graph,omitempty"`
			}{req, int64(grace), graph}
			done, err := wf.Run(c, name, declared, func(ctx context.Context) (bool, error) {
				err := purge(ctx, req.Type, req.ID)
				if errors.Is(err, ErrActive) || errors.Is(err, ErrNotTerminal) || graph && graphPurgeRetryable(err) {
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
