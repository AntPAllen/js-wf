// Package graphreplayfixture shares SDK handlers between native delivery and
// the separately compiled operator plugin, without depending on worker.
package graphreplayfixture

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync/atomic"

	"js-wf/wf"
)

var InitialCalls, MiddleCalls, FinishCalls, Effects atomic.Int64

func Initial(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
	InitialCalls.Add(1)
	var n int
	if err := json.Unmarshal(input, &n); err != nil {
		return nil, err
	}
	if err := c.SetState("value", 23); err != nil {
		return nil, err
	}
	return nil, wf.Continue(c, "middle_v1", n)
}

func Middle(c *wf.Context, input, locals json.RawMessage) (json.RawMessage, error) {
	MiddleCalls.Add(1)
	var original, n, value int
	if err := json.Unmarshal(input, &original); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(locals, &n); err != nil {
		return nil, err
	}
	if n != original {
		return nil, fmt.Errorf("original input lost")
	}
	if ok, err := c.GetState("value", &value); err != nil || !ok || value != 23 {
		return nil, wf.ErrCorruptJournal
	}
	return nil, wf.Continue(c, "finish_v1", value+n)
}

func Finish(c *wf.Context, input, locals json.RawMessage) (json.RawMessage, error) {
	FinishCalls.Add(1)
	if string(input) != "7" || string(locals) != "30" {
		return nil, wf.ErrCorruptJournal
	}
	if _, err := wf.AwaitSignal(c, "gate"); err != nil {
		return nil, err
	}
	result, err := wf.Run(c, "double", 30, func(context.Context) (int, error) {
		Effects.Add(1)
		if marker := os.Getenv("WF_REPLAY_EFFECT_MARKER"); marker != "" {
			if err := os.WriteFile(marker, []byte("effect ran"), 0600); err != nil {
				return 0, err
			}
		}
		return 60, nil
	})
	if err != nil {
		return nil, err
	}
	return json.Marshal(result)
}
