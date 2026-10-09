package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"time"

	"js-wf/internal/testworkflow"
	"js-wf/wf"
	"js-wf/worker"
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

func FailWorkflow(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
	_, err := wf.Run(c, "fail", 7, func(context.Context) (int, error) {
		if marker := os.Getenv("WF_REPLAY_EFFECT_MARKER"); marker != "" {
			_ = os.WriteFile(marker, []byte("effect ran"), 0600)
		}
		return 0, errors.New("boom")
	})
	return nil, err
}

func ChangedFailure(c *wf.Context, raw json.RawMessage) (json.RawMessage, error) {
	_, err := FailWorkflow(c, raw)
	if err != nil {
		return nil, errors.New("changed failure")
	}
	return nil, nil
}

func ImmediateFailure(*wf.Context, json.RawMessage) (json.RawMessage, error) {
	return nil, errors.New("boom")
}

func WaitSignalWorkflow(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
	_, err := wf.AwaitSignal(c, "go")
	if err != nil {
		return nil, err
	}
	return json.RawMessage(`true`), nil
}

func ChangedWait(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
	_, err := wf.AwaitSignal(c, "other")
	return nil, err
}

func WaitTimerWorkflow(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
	if err := wf.Sleep(c, "later", time.Hour); err != nil {
		return nil, err
	}
	return json.RawMessage(`true`), nil
}

func PanicWorkflow(*wf.Context, json.RawMessage) (json.RawMessage, error) {
	panic("boom")
}

func LongPanicWorkflow(*wf.Context, json.RawMessage) (json.RawMessage, error) {
	panic(strings.Repeat("x", 4096-len("workflow panic: ")-1) + "é trailing")
}

func PendingWorkflow(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
	_, err := wf.Run(c, "block", 0, func(context.Context) (int, error) {
		if marker := os.Getenv("WF_REPLAY_EFFECT_MARKER"); marker != "" {
			_ = os.WriteFile(marker, []byte("effect ran"), 0600)
		}
		return 0, nil
	})
	return nil, err
}

func CompletedThenWaitWorkflow(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
	_, err := wf.Run(c, "done", 0, func(context.Context) (int, error) {
		if marker := os.Getenv("WF_REPLAY_EFFECT_MARKER"); marker != "" {
			_ = os.WriteFile(marker, []byte("effect ran"), 0600)
		}
		return 1, nil
	})
	return nil, err
}

var ContinuedWorkflow = testworkflow.Definition

func ContinuedFactory() worker.WorkflowDefinition { return testworkflow.Definition }

var MissingContinuation = worker.WorkflowDefinition{Handler: testworkflow.Definition.Handler, Continuations: map[string]worker.ContinuationHandler{"middle_v1": testworkflow.Definition.Continuations["middle_v1"]}}
var ChangedContinuation = worker.WorkflowDefinition{Handler: testworkflow.Definition.Handler, Continuations: map[string]worker.ContinuationHandler{
	"middle_v1": testworkflow.Definition.Continuations["middle_v1"],
	"finish_v1": func(c *wf.Context, input, locals json.RawMessage) (json.RawMessage, error) {
		if _, err := wf.AwaitSignal(c, "gate"); err != nil {
			return nil, err
		}
		_, err := wf.Run(c, "changed", 30, func(context.Context) (int, error) { panic("offline effect ran") })
		return nil, err
	},
}}

func GraphSignalWorkflow(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
	value, err := wf.AwaitSignal(c, "go")
	if err != nil {
		return nil, err
	}
	_, err = wf.Run(c, "once", 0, func(context.Context) (int, error) {
		if marker := os.Getenv("WF_REPLAY_EFFECT_MARKER"); marker != "" {
			_ = os.WriteFile(marker, []byte("effect ran"), 0600)
		}
		return -1, nil
	})
	return json.RawMessage(value), err
}
