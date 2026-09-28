package main

import (
	"context"
	"encoding/json"
	"os"

	"js-wf/wf"
)

func replay(c *wf.Context, raw json.RawMessage, name string) (json.RawMessage, error) {
	var input struct {
		N       int    `json:"n"`
		Payload string `json:"payload"`
	}
	if err := json.Unmarshal(raw, &input); err != nil {
		return nil, err
	}
	value, err := wf.Run(c, name, input.N, func(context.Context) (int, error) {
		if marker := os.Getenv("WF_REPLAY_EFFECT_MARKER"); marker != "" {
			_ = os.WriteFile(marker, []byte("effect ran"), 0600)
		}
		return -1, nil
	})
	if err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

func Workflow(c *wf.Context, raw json.RawMessage) (json.RawMessage, error) {
	return replay(c, raw, "double")
}

func ChangedWorkflow(c *wf.Context, raw json.RawMessage) (json.RawMessage, error) {
	return replay(c, raw, "renamed")
}

func ChangedResult(c *wf.Context, raw json.RawMessage) (json.RawMessage, error) {
	result, err := replay(c, raw, "double")
	if err != nil {
		return nil, err
	}
	var value int
	if err := json.Unmarshal(result, &value); err != nil {
		return nil, err
	}
	return json.Marshal(value + 1)
}

func LargeWorkflow(c *wf.Context, raw json.RawMessage) (json.RawMessage, error) {
	var input struct {
		Payload string `json:"payload"`
	}
	if err := json.Unmarshal(raw, &input); err != nil {
		return nil, err
	}
	value, err := wf.Run(c, "large", len(input.Payload), func(context.Context) (string, error) {
		if marker := os.Getenv("WF_REPLAY_EFFECT_MARKER"); marker != "" {
			_ = os.WriteFile(marker, []byte("effect ran"), 0600)
		}
		return "", nil
	})
	if err != nil {
		return nil, err
	}
	return json.Marshal(len(value))
}
