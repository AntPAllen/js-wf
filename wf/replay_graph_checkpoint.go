package wf

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"

	"js-wf/internal/checkpoint"
	"js-wf/internal/stepwire"
	"js-wf/journal"
)

type replayCheckpointChild struct {
	Type       string `json:"type"`
	ID         string `json:"id"`
	Invocation uint64 `json:"inv_seq"`
	Ref        string `json:"result_ref,omitempty"`
	Hash       string `json:"result_hash,omitempty"`
}

type replayCheckpointSignal struct {
	Sequence  uint64                 `json:"sig_seq"`
	Name      string                 `json:"name"`
	Payload   []byte                 `json:"payload,omitempty"`
	Ref       string                 `json:"ref,omitempty"`
	Hash      string                 `json:"hash,omitempty"`
	Child     *replayCheckpointChild `json:"graph_child,omitempty"`
	Canonical *struct {
		Index uint64 `json:"index"`
		Token string `json:"token"`
	} `json:"canonical_signal,omitempty"`
}

type replayCheckpointMetadata struct {
	Version    int                               `json:"version"`
	Identity   checkpoint.Identity               `json:"identity"`
	Anchor     checkpoint.Anchor                 `json:"anchor"`
	FrameHash  string                            `json:"frame_sha256"`
	Children   map[string]replayCheckpointChild  `json:"children"`
	Signals    map[uint64]replayCheckpointSignal `json:"signals"`
	SignalNext uint64                            `json:"signal_next"`
	SignalLast uint64                            `json:"signal_last"`
}

type replayCheckpointRequest = stepwire.Request

// ValidateReplayGraphCheckpoints verifies exported worker checkpoint metadata
// before a handler or plugin is invoked. Unannotated legacy completions keep
// their existing SDK frame validation path. This does not establish storage
// ownership: an offline bundle supplies already exported object bytes.
func ValidateReplayGraphCheckpoints(records []journal.Record, objects map[string][]byte, typ, id string, invocation uint64) error {
	load := func(ref, hash string) ([]byte, error) {
		digest, err := hex.DecodeString(hash)
		if ref == "" || err != nil || len(digest) != sha256.Size || hex.EncodeToString(digest) != hash {
			return nil, ErrCorruptJournal
		}
		raw, ok := objects[ref]
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrReplayObjectMissing, ref)
		}
		sum := sha256.Sum256(raw)
		if hex.EncodeToString(sum[:]) != hash {
			return nil, ErrCorruptJournal
		}
		return raw, nil
	}
	children := map[string]replayCheckpointChild{}
	signals := map[uint64]replayCheckpointSignal{}
	var signalNext, signalLast uint64
	var request replayCheckpointRequest
	var pending bool
	for _, record := range records {
		switch record.Kind {
		case journal.StepRequested:
			request = replayCheckpointRequest{}
			if stepwire.Decode(record.Payload, &request) != nil {
				return ErrCorruptJournal
			}
			pending = true
			if request.Kind == "call" || request.Kind == "call_async" {
				children[request.Name] = replayCheckpointChild{Type: request.ChildType, ID: request.ChildID}
			}
		case journal.SignalConsumed:
			var signal replayCheckpointSignal
			if json.Unmarshal(record.Payload, &signal) != nil {
				return ErrCorruptJournal
			}
			signals[signal.Sequence] = signal
			signalNext++
			signalLast = signal.Sequence
		case journal.StepCompleted:
			var completion stepwire.Completion
			if stepwire.DecodeCompletion(record.Payload, &completion) != nil {
				return ErrCorruptJournal
			}
			annotated := len(completion.MetadataRef) > 0 || len(completion.MetadataHash) > 0
			declared := pending && request.Kind == "checkpoint"
			pending = false
			if !annotated {
				continue
			}
			var ref, hash string
			if !declared || json.Unmarshal(completion.MetadataRef, &ref) != nil || json.Unmarshal(completion.MetadataHash, &hash) != nil {
				return ErrCorruptJournal
			}
			raw, err := load(ref, hash)
			if err != nil {
				return err
			}
			if len(raw) > checkpoint.MaxBytes {
				return ErrCorruptJournal
			}
			var meta replayCheckpointMetadata
			identity := checkpoint.Identity{Type: typ, ID: id, InvSeq: invocation}
			anchor := checkpoint.Anchor{Index: record.Index, Epoch: record.Epoch}
			if checkpoint.DecodeUnambiguous(raw, &meta) != nil || meta.Version != 1 || meta.Identity != identity || meta.Anchor != anchor || meta.FrameHash != completion.ResultHash {
				return ErrCorruptJournal
			}
			frameBytes, err := load(completion.ResultRef, completion.ResultHash)
			if err != nil {
				return err
			}
			frame, err := checkpoint.Decode(frameBytes, completion.ResultHash, identity, anchor)
			if err != nil || frame.Stage != request.Name {
				return ErrCorruptJournal
			}
			locals := sha256.Sum256(frame.Data)
			if hex.EncodeToString(locals[:]) != request.InputHash || meta.SignalNext != signalNext || meta.SignalLast != signalLast || meta.SignalLast > frame.SignalCursor || len(meta.Children) != len(children) {
				return ErrCorruptJournal
			}
			for name, child := range children {
				if meta.Children[name] != child {
					return ErrCorruptJournal
				}
			}
			expected := map[uint64]replayCheckpointSignal{}
			for _, buffered := range frame.PendingSignals {
				event, ok := signals[buffered.Sequence]
				if !ok || event.Name != buffered.Name {
					return ErrCorruptJournal
				}
				if event.Child == nil {
					continue
				}
				if event.Canonical == nil || event.Canonical.Index >= signalNext || event.Canonical.Token == "" {
					return ErrCorruptJournal
				}
				payload := event.Payload
				if event.Ref != "" {
					payload, err = load(event.Ref, event.Hash)
					if err != nil {
						return err
					}
				}
				if !bytes.Equal(payload, buffered.Payload) {
					return ErrCorruptJournal
				}
				expected[buffered.Sequence] = event
			}
			if len(expected) != len(meta.Signals) {
				return ErrCorruptJournal
			}
			for sequence, event := range expected {
				if !reflect.DeepEqual(meta.Signals[sequence], event) {
					return ErrCorruptJournal
				}
			}
		}
	}
	return nil
}
