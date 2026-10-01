// Package testworkflow supplies the same workflow to worker and replay plugin fixtures.
package testworkflow

import (
	"context"
	"encoding/json"
	"fmt"
	"js-wf/wf"
	"js-wf/worker"
	"sync/atomic"
)

var Effects atomic.Int64

var Definition = worker.WorkflowDefinition{
	Handler: func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
		var n int
		if err := json.Unmarshal(input, &n); err != nil {
			return nil, err
		}
		if err := c.SetState("value", 23); err != nil {
			return nil, err
		}
		return nil, wf.Continue(c, "middle_v1", n)
	},
	Continuations: map[string]worker.ContinuationHandler{
		"middle_v1": func(c *wf.Context, input, locals json.RawMessage) (json.RawMessage, error) {
			var n, original int
			if err := json.Unmarshal(input, &original); err != nil {
				return nil, err
			}
			if err := json.Unmarshal(locals, &n); err != nil {
				return nil, err
			}
			if n != original {
				return nil, fmt.Errorf("original input lost")
			}
			var value int
			_, err := c.GetState("value", &value)
			if err != nil {
				return nil, err
			}
			return nil, wf.Continue(c, "finish_v1", value+n)
		},
		"finish_v1": func(c *wf.Context, input, locals json.RawMessage) (json.RawMessage, error) {
			var value int
			if err := json.Unmarshal(locals, &value); err != nil {
				return nil, err
			}
			if _, err := wf.AwaitSignal(c, "gate"); err != nil {
				return nil, err
			}
			result, err := wf.Run(c, "double", value, func(context.Context) (int, error) { Effects.Add(1); return value * 2, nil })
			if err != nil {
				return nil, err
			}
			return json.Marshal(result)
		},
	},
}
