package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/wf"
	"js-wf/worker"
)

func TestLargeStepResultSpillsAndReplays(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const typ, id = "test", "large-result"
	const size = 5 * 1024 * 1024
	var effects atomic.Int64
	w, err := worker.New(ctx, all[1], "large-result-worker", map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		value, err := wf.Run(c, "large", 1, func(context.Context) (string, error) {
			effects.Add(1)
			return strings.Repeat("z", size), nil
		})
		if err != nil {
			return nil, err
		}
		return json.RawMessage(fmt.Sprintf("%d", len(value))), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.New(all[0]).Start(ctx, typ, id, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- w.RunPartition(workerCtx, identity.Partition(typ, id, provision.Partitions)) }()
	result, err := client.New(all[0]).Await(ctx, typ, id)
	if err != nil || string(result) != "5242880" || effects.Load() != 1 {
		t.Fatalf("result=%s effects=%d err=%v", result, effects.Load(), err)
	}
	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	records, _, err := journal.New(all[0]).Read(ctx, typ, id)
	if err != nil {
		t.Fatal(err)
	}
	var steps []wf.Entry
	var ref string
	for _, r := range records {
		if r.Kind != journal.StepRequested && r.Kind != journal.StepCompleted {
			continue
		}
		steps = append(steps, wf.Entry{Index: r.Index, Kind: wf.Kind(r.Kind), Payload: r.Payload})
		if r.Kind == journal.StepCompleted {
			var completion struct {
				ResultRef string `json:"result_ref"`
			}
			if err := json.Unmarshal(r.Payload, &completion); err != nil {
				t.Fatal(err)
			}
			ref = completion.ResultRef
			if len(r.Payload) > wf.MaxInlineResult {
				t.Fatalf("journal completion too large: %d", len(r.Payload))
			}
		}
	}
	if ref == "" || len(steps) != 2 {
		t.Fatalf("missing result ref: %q steps=%d", ref, len(steps))
	}
	objects, err := all[0].ObjectStore(ctx, "WF_BLOB")
	if err != nil {
		t.Fatal(err)
	}
	replay := wf.NewContext(ctx, steps, nil)
	replay.SetResultStore(nil, func(ctx context.Context, name string) ([]byte, error) { return objects.GetBytes(ctx, name) })
	value, err := wf.Run(replay, "large", 1, func(context.Context) (string, error) {
		return "", errors.New("effect executed on replay")
	})
	if err != nil || len(value) != size || effects.Load() != 1 {
		t.Fatalf("replay: len=%d effects=%d err=%v", len(value), effects.Load(), err)
	}
	if err := replay.CheckComplete(); err != nil {
		t.Fatal(err)
	}
	if _, err := objects.PutBytes(ctx, ref, []byte(`"corrupt"`)); err != nil {
		t.Fatal(err)
	}
	corrupt := wf.NewContext(ctx, steps, nil)
	corrupt.SetResultStore(nil, func(ctx context.Context, name string) ([]byte, error) { return objects.GetBytes(ctx, name) })
	if _, err := wf.Run(corrupt, "large", 1, func(context.Context) (string, error) {
		return "", errors.New("effect executed on replay")
	}); !errors.Is(err, wf.ErrCorruptJournal) {
		t.Fatalf("corrupt result object: %v", err)
	}
}

func TestLargeTerminalResultSpillsAndAwaitVerifies(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const typ, id = "test", "large-terminal"
	w, err := worker.New(ctx, all[1], "terminal-worker", map[string]worker.Handler{typ: func(*wf.Context, json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`"` + strings.Repeat("q", 5*1024*1024) + `"`), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	c := client.New(all[0])
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- w.RunPartition(workerCtx, identity.Partition(typ, id, provision.Partitions)) }()
	result, err := c.Await(ctx, typ, id)
	if err != nil || len(result) != 5*1024*1024+2 {
		t.Fatalf("await: len=%d err=%v", len(result), err)
	}
	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	state, err := all[0].KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	entry, err := state.Get(ctx, identity.Key(typ, id))
	if err != nil {
		t.Fatal(err)
	}
	if len(entry.Value()) > wf.MaxInlineTerminal {
		t.Fatalf("terminal KV too large: %d", len(entry.Value()))
	}
	var out wf.Outcome
	if err := json.Unmarshal(entry.Value(), &out); err != nil || out.ResultRef == "" || out.ResultHash == "" {
		t.Fatalf("terminal outcome: %+v err=%v", out, err)
	}
	objects, err := all[0].ObjectStore(ctx, "WF_BLOB")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := objects.PutBytes(ctx, out.ResultRef, []byte(`"corrupt"`)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Await(ctx, typ, id); !errors.Is(err, wf.ErrCorruptJournal) {
		t.Fatalf("corrupt terminal result: %v", err)
	}
}
