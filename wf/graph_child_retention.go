package wf

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"js-wf/identity"
	"js-wf/journal"
)

// GraphOwnsChildResult verifies a runtime request followed by an exact consumed
// child declaration and parent-owned terminal/result bytes. Ordinary user JSON
// references cannot authorize child retirement. Keep both source and parent
// views pinned while making the retirement decision.
func GraphOwnsChildResult(ctx context.Context, parent *journal.GraphView, childType, childID string, invocation uint64, signal string, terminal GraphTerminal, maxBytes int) (bool, error) {
	if parent == nil || identity.Validate(childType, childID) != nil || identity.ValidateToken(signal) != nil || invocation == 0 || terminal.Outcome.InvSeq != invocation {
		return false, ErrCorruptJournal
	}
	declared := false
	owned := false
	for i := uint64(0); i < parent.Count(); i++ {
		record, err := parent.Read(ctx, i)
		if err != nil {
			return false, err
		}
		if record.Kind == journal.StepRequested {
			var req request
			if json.Unmarshal(record.Payload, &req) != nil {
				return false, ErrCorruptJournal
			}
			if (req.Kind == "call" || req.Kind == "call_async") && req.Name == signal {
				if declared || req.ChildType != childType || req.ChildID != childID {
					return false, ErrCorruptJournal
				}
				declared = true
			}
		}
		if record.Kind != journal.SignalConsumed {
			continue
		}
		var event struct {
			Sequence uint64 `json:"sig_seq"`
			Name     string `json:"name"`
			Payload  []byte `json:"payload,omitempty"`
			Ref      string `json:"ref,omitempty"`
			Hash     string `json:"hash,omitempty"`
			Child    *struct {
				Type       string `json:"type"`
				ID         string `json:"id"`
				Invocation uint64 `json:"inv_seq"`
				Ref        string `json:"result_ref,omitempty"`
				Hash       string `json:"result_hash,omitempty"`
			} `json:"graph_child,omitempty"`
		}
		if json.Unmarshal(record.Payload, &event) != nil {
			return false, ErrCorruptJournal
		}
		if event.Name != signal || event.Child == nil {
			continue
		}
		child := event.Child
		if !declared || owned || event.Sequence == 0 || child.Type != childType || child.ID != childID || child.Invocation != invocation || child.Ref != terminal.Outcome.ResultRef || child.Hash != terminal.Outcome.ResultHash {
			return false, ErrCorruptJournal
		}
		payload := event.Payload
		if event.Ref != "" {
			if len(payload) != 0 {
				return false, ErrCorruptJournal
			}
			payload, err = graphRecordPayload(ctx, parent, record, event.Hash, maxBytes)
			if err != nil {
				return false, err
			}
		} else if event.Hash != "" {
			sum := sha256.Sum256(payload)
			if hex.EncodeToString(sum[:]) != event.Hash {
				return false, ErrCorruptJournal
			}
		}
		if !bytes.Equal(payload, terminal.Payload) {
			return false, ErrCorruptJournal
		}
		if child.Ref != "" {
			result, err := graphRecordPayload(ctx, parent, record, child.Hash, maxBytes)
			if err != nil {
				return false, err
			}
			if !bytes.Equal(result, terminal.Result) {
				return false, ErrCorruptJournal
			}
		}
		owned = true
	}
	return owned, nil
}

func graphRecordPayload(ctx context.Context, view *journal.GraphView, record journal.GraphRecord, hash string, maxBytes int) ([]byte, error) {
	decoded, err := hex.DecodeString(hash)
	if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != hash {
		return nil, ErrCorruptJournal
	}
	for _, link := range append(record.Blobs, record.EntryBlob) {
		if link.Hash != hash {
			continue
		}
		data, err := view.Payload(ctx, record.Index, link, maxBytes)
		if err != nil {
			return nil, err
		}
		sum := sha256.Sum256(data)
		if hex.EncodeToString(sum[:]) != hash {
			return nil, ErrCorruptJournal
		}
		return data, nil
	}
	return nil, ErrCorruptJournal
}
