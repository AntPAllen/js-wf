package wf

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"js-wf/journal"
	"testing"
)

func TestReplayNullStepMetadataBeforeHandler(t *testing.T) {
	hash := sha256.Sum256([]byte(`null`))
	for _, level := range []string{"request", "case", "completion"} {
		fields := []string{"kind", "name", "input_hash", "duration_nanos", "fire_at", "clock_domain", "timer_step", "timer_name", "child_type", "child_id", "cases"}
		if level == "case" {
			fields = []string{"kind", "name", "timer_step", "fire_at", "clock_domain", "child_type", "child_id"}
		}
		if level == "completion" {
			fields = []string{"case_index", "result_ref", "result_hash", "error", "error_kind", "signal_seq", "selected", "cancelled", "checkpoint_metadata_ref", "checkpoint_metadata_hash"}
		}
		for _, field := range fields {
			t.Run(level+"/"+field, func(t *testing.T) {
				req := map[string]any{"kind": "run", "name": "step", "input_hash": hex.EncodeToString(hash[:])}
				done := map[string]any{"result": 42}
				switch level {
				case "request":
					req[field] = nil
				case "case":
					c := map[string]any{"kind": "signal", "name": "gate"}
					c[field] = nil
					req["cases"] = []any{c}
				case "completion":
					done[field] = nil
				}
				r, _ := json.Marshal(req)
				d, _ := json.Marshal(done)
				records := []journal.Record{{Entry: journal.Entry{Kind: journal.Started}, Sequence: 1}, {Entry: journal.Entry{Index: 1, Kind: journal.StepRequested, Payload: r}, Sequence: 2}, {Entry: journal.Entry{Index: 2, Kind: journal.StepCompleted, Payload: d}, Sequence: 3}, {Entry: journal.Entry{Index: 3, Kind: journal.Completed, Payload: []byte(`{"inv_seq":1,"result":"NDI="}`)}, Sequence: 4}}
				raw, err := json.Marshal(records)
				if err != nil {
					t.Fatal(err)
				}
				handlers, effects := 0, 0
				_, err = Replay(raw, func(c *Context) (int, error) {
					handlers++
					return Run(c, "step", nil, func(context.Context) (int, error) { effects++; return 42, nil })
				}, ReplayOptions{})
				if !errors.Is(err, ErrCorruptJournal) || handlers != 0 || effects != 0 {
					t.Fatalf("null metadata reached handler: handlers=%d effects=%d error=%v", handlers, effects, err)
				}
			})
		}
	}
}
