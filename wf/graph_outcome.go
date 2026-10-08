package wf

import (
	"context"
	"encoding/json"

	"js-wf/journal"
)

// GraphTerminal contains a verified canonical terminal entry and its resolved
// result. The supplied view owns payload reads; callers keep it pinned through
// any decisions that depend on this terminal snapshot.
type GraphTerminal struct {
	Outcome Outcome
	Payload []byte
	Result  []byte
}

// ReadGraphTerminal verifies terminal kind/generation and exact external edges.
// It does not observe invocation/purge lifecycle or release the supplied pin.
func ReadGraphTerminal(ctx context.Context, view *journal.GraphView, invocation uint64, maxBytes int) (GraphTerminal, error) {
	if view == nil || invocation == 0 || view.Count() == 0 {
		return GraphTerminal{}, ErrCorruptJournal
	}
	record, err := view.Read(ctx, view.Count()-1)
	if err != nil {
		return GraphTerminal{}, err
	}
	var outcome Outcome
	if json.Unmarshal(record.Payload, &outcome) != nil || outcome.InvSeq != invocation || record.Kind != journal.Completed && record.Kind != journal.Failed || record.Kind == journal.Completed && outcome.Error != "" || record.Kind == journal.Failed && (outcome.Error == "" || len(outcome.Result) != 0 || outcome.ResultRef != "" || outcome.ResultHash != "") {
		return GraphTerminal{}, ErrCorruptJournal
	}
	terminal := GraphTerminal{Outcome: outcome, Payload: record.Payload}
	if outcome.Error != "" {
		return terminal, nil
	}
	terminal.Result, err = outcome.ResultBytes(ctx, func(ctx context.Context, _ string) ([]byte, error) {
		for _, link := range append(record.Blobs, record.EntryBlob) {
			if link.Hash == outcome.ResultHash {
				return view.Payload(ctx, record.Index, link, maxBytes)
			}
		}
		return nil, ErrCorruptJournal
	})
	if err != nil {
		return GraphTerminal{}, err
	}
	return terminal, nil
}
