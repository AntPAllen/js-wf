package journal_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"js-wf/journal"
	"js-wf/sim"
)

func TestGraphCanonicalSignalConsumptionProgressRequiresOwnedInput(t *testing.T) {
	for _, mode := range []string{"index", "token", "name", "hash", "reference", "missing_body", "unknown_metadata", "healthy"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			graph, model, _ := signalInputFixture(t, 71)
			source := sim.NewSignalTransport(sim.NewScheduler(71))
			body := []byte("input")
			input, err := graph.ReserveSignal(ctx, journal.GraphSignalRequest{Type: "flow", ID: "id", Invocation: 9, Name: "signal", Key: "key"}, body, false)
			if err != nil {
				t.Fatal(err)
			}
			source.CommitSignal(queueSource(input))
			if progress, e := graph.BindNextSignal(ctx, "flow", "id", 9, 1, source); e != nil || !progress {
				t.Fatal(progress, e)
			}
			tail, err := graph.Begin(ctx, "flow", "id", 9)
			if err != nil {
				t.Fatal(err)
			}
			tail, err = graph.Append(ctx, "flow", "id", 9, journal.Entry{Kind: journal.Started}, tail, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			event := map[string]any{"sig_seq": uint64(1), "name": "signal", "hash": input.InputSHA256, "ref": "graph-signal-" + input.InputSHA256, "canonical_signal": map[string]any{"index": uint64(0), "token": input.Token}}
			switch mode {
			case "index":
				event["canonical_signal"].(map[string]any)["index"] = 1
			case "token":
				event["canonical_signal"].(map[string]any)["token"] = "foreign"
			case "name":
				event["name"] = "foreign"
			case "hash":
				event["hash"] = "foreign"
			case "reference":
				event["ref"] = "foreign"
			case "unknown_metadata":
				if err = model.QueueFault("get", sim.DropBeforeCommit); err != nil {
					t.Fatal(err)
				}
			}
			payload, _ := json.Marshal(event)
			payloads := [][]byte{body}
			if mode == "missing_body" {
				payloads = nil
			}
			_, err = graph.Append(ctx, "flow", "id", 9, journal.Entry{Index: 1, Kind: journal.SignalConsumed, Payload: payload}, tail, payloads, nil)
			if mode == "healthy" {
				if err != nil {
					t.Fatal(err)
				}
			} else if mode == "unknown_metadata" {
				if !errors.Is(err, journal.ErrUnknown) {
					t.Fatal(err)
				}
			} else if !errors.Is(err, journal.ErrGap) {
				t.Fatal(err)
			}
			status, e := graph.InspectStart(ctx, "flow", "id")
			want := uint64(0)
			if mode == "healthy" {
				want = 1
			}
			if e != nil || status.SignalConsumed != want {
				t.Fatal(status, e)
			}
			ready, e := graph.InspectReadySignal(ctx, "flow", "id", 9)
			if e != nil || (ready == nil) != (mode == "healthy") {
				t.Fatal(ready, e)
			}
			if e = model.CheckReferences(); e != nil {
				t.Fatal(e)
			}
		})
	}
}
