package main

import (
	"encoding/json"
	"time"

	"js-wf/internal/testworkflow"
	"js-wf/wf"
	"js-wf/worker"
)

var Handlers = map[string]worker.Handler{
	"worker-graph-signal": func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		value, err := wf.AwaitSignal(c, "go")
		return json.RawMessage(value), err
	},
	"worker-smoke": func(_ *wf.Context, input json.RawMessage) (json.RawMessage, error) {
		return input, nil
	},
	"worker-timer": func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		if err := wf.Sleep(c, "fallback", 100*time.Millisecond); err != nil {
			return nil, err
		}
		return json.RawMessage(`42`), nil
	},
}

var Workflows = map[string]worker.WorkflowDefinition{"continued": testworkflow.Definition}

func WorkflowFactory() map[string]worker.WorkflowDefinition { return Workflows }

var InvalidWorkflows = map[string]worker.WorkflowDefinition{"continued": {Handler: testworkflow.Definition.Handler, Continuations: map[string]worker.ContinuationHandler{"bad.stage": nil}}}
