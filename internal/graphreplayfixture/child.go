package graphreplayfixture

import (
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"

	"js-wf/wf"
)

const ChildType = "offlinechild"
const ChildFailure = "planned offline child failure"
const ChildResultBytes = 700000

var ChildCalls atomic.Int64

type ChildInput struct {
	Value  int  `json:"value"`
	Failed bool `json:"failed"`
	Cached bool `json:"cached"`
}

type ChildLocals struct {
	Count   int        `json:"count"`
	Promise wf.Promise `json:"promise"`
}

func ChildInitial(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
	InitialCalls.Add(1)
	var original ChildInput
	if json.Unmarshal(input, &original) != nil || original.Value != 7 {
		return nil, wf.ErrCorruptJournal
	}
	if err := c.SetState("value", 23); err != nil {
		return nil, err
	}
	childInput, err := json.Marshal(original.Failed)
	if err != nil {
		return nil, err
	}
	promise, err := wf.CallAsync(c, ChildType, childInput)
	if err != nil {
		return nil, err
	}
	return nil, wf.Continue(c, "child_middle_v1", ChildLocals{Count: 1, Promise: promise})
}

func ChildMiddle(c *wf.Context, input, locals json.RawMessage) (json.RawMessage, error) {
	MiddleCalls.Add(1)
	original, data, err := readChildLocals(c, input, locals, 1)
	if err != nil {
		return nil, err
	}
	if original.Cached {
		if err := checkChildOutcome(c, original, data.Promise); err != nil {
			return nil, err
		}
	}
	data.Count = 2
	return nil, wf.Continue(c, "child_finish_v1", data)
}

func ChildFinish(c *wf.Context, input, locals json.RawMessage) (json.RawMessage, error) {
	FinishCalls.Add(1)
	original, data, err := readChildLocals(c, input, locals, 2)
	if err != nil {
		return nil, err
	}
	if err := checkChildOutcome(c, original, data.Promise); err != nil {
		return nil, err
	}
	return finishEffect(c)
}

func Child(_ *wf.Context, input json.RawMessage) (json.RawMessage, error) {
	ChildCalls.Add(1)
	var failed bool
	if json.Unmarshal(input, &failed) != nil {
		return nil, wf.ErrCorruptJournal
	}
	if failed {
		return nil, errors.New(ChildFailure)
	}
	return json.Marshal(strings.Repeat("x", ChildResultBytes))
}

func readChildLocals(c *wf.Context, input, locals json.RawMessage, count int) (ChildInput, ChildLocals, error) {
	var original ChildInput
	var data ChildLocals
	var value int
	if json.Unmarshal(input, &original) != nil || original.Value != 7 || json.Unmarshal(locals, &data) != nil || data.Count != count || data.Promise.ChildType != ChildType || data.Promise.ChildID == "" || data.Promise.SignalName == "" {
		return original, data, wf.ErrCorruptJournal
	}
	if ok, err := c.GetState("value", &value); err != nil || !ok || value != 23 {
		return original, data, wf.ErrCorruptJournal
	}
	return original, data, nil
}

func checkChildOutcome(c *wf.Context, original ChildInput, promise wf.Promise) error {
	raw, err := wf.AwaitPromise(c, promise)
	if errors.Is(err, wf.ErrSuspended) {
		return err
	}
	if original.Failed {
		if err == nil || err.Error() != ChildFailure || len(raw) != 0 {
			return wf.ErrCorruptJournal
		}
		return nil
	}
	var value string
	if err != nil {
		return err
	}
	if json.Unmarshal(raw, &value) != nil || value != strings.Repeat("x", ChildResultBytes) {
		return wf.ErrCorruptJournal
	}
	return nil
}
