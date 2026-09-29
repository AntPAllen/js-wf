package main

import (
	"encoding/json"
	"time"

	"js-wf/wf"
	"js-wf/worker"
)

var Handlers = map[string]worker.Handler{
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
