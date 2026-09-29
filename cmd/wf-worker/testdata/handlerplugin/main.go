package main

import (
	"encoding/json"

	"js-wf/wf"
	"js-wf/worker"
)

var Handlers = map[string]worker.Handler{
	"worker-smoke": func(_ *wf.Context, input json.RawMessage) (json.RawMessage, error) {
		return input, nil
	},
}
