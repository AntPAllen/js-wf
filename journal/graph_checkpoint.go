package journal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"js-wf/internal/checkpoint"
)

// GraphCheckpointRead is a continuation frame and its anchored suffix from one
// pinned generation. Keep the GraphView alive while resolving payload references
// from the frame or suffix. The retained prefix remains available for audit.
// This does not compact the graph or publish a mutable resume manifest.
type GraphCheckpointRead struct {
	Runtime RuntimeCheckpoint
	Frame   []byte
	Anchor  Record
	Records []Record
	Tail    uint64
}

// ReadCheckpoint discovers the latest completed checkpoint in this exact view.
// Pending checkpoint requests do not become resume points. Corrupt metadata or
// unreadable owned frame bytes return an error, never an initial-replay result.
// The caller owns the view and its release/renewal lifecycle.
func (v *GraphView) ReadCheckpoint(ctx context.Context, typ, id string) (*GraphCheckpointRead, error) {
	if err := v.alive(); err != nil {
		return nil, err
	}
	destination, err := graphDestination(typ, id)
	if err != nil {
		return nil, err
	}
	if destination != v.destination {
		return nil, ErrCheckpointGeneration
	}
	records := make([]Record, 0, v.Count())
	var request Record
	var declaration struct {
		Kind      string `json:"kind"`
		Name      string `json:"name"`
		InputHash string `json:"input_hash"`
	}
	var pending bool
	var position uint64
	var candidate *GraphCheckpointRead
	var receipt GraphRecord
	var localsHash string
	for i := uint64(0); i < v.Count(); i++ {
		record, err := v.Read(ctx, i)
		if err != nil {
			return nil, err
		}
		if len(records) > 0 {
			previous := records[len(records)-1]
			if record.Sequence != previous.Sequence+1 || record.Epoch < previous.Epoch {
				return nil, ErrGap
			}
		}
		records = append(records, record.Record)
		switch record.Kind {
		case StepRequested:
			if pending {
				return nil, ErrGap
			}
			declaration = struct {
				Kind      string `json:"kind"`
				Name      string `json:"name"`
				InputHash string `json:"input_hash"`
			}{}
			if json.Unmarshal(record.Payload, &declaration) != nil {
				return nil, ErrGap
			}
			request = record.Record
			pending = true
			position++
		case StepCompleted:
			if !pending {
				return nil, ErrGap
			}
			pending = false
			position++
			if declaration.Kind != "checkpoint" {
				continue
			}
			var completion struct {
				ResultRef  string `json:"result_ref"`
				ResultHash string `json:"result_hash"`
			}
			if json.Unmarshal(record.Payload, &completion) != nil {
				return nil, ErrGap
			}
			runtime := RuntimeCheckpoint{InvSeq: v.cursor.Invocation, Stage: declaration.Name, Sequence: record.Sequence, Index: record.Index, Epoch: record.Epoch, StepPosition: position, Object: completion.ResultRef, SHA256: completion.ResultHash}
			// Reuse the existing checkpoint contract without introducing an archive
			// identity into the graph. Started guarantees the anchor is not index zero.
			if ValidateRuntimeSnapshot(Snapshot{Version: 2, Runtime: &runtime}) != nil || verifyCheckpointAnchor(record.Record, runtime) != nil || request.Index >= record.Index {
				return nil, ErrGap
			}
			candidate = &GraphCheckpointRead{Runtime: runtime, Anchor: record.Record}
			receipt = record
			localsHash = declaration.InputHash
		}
	}
	if candidate == nil {
		return nil, nil
	}
	var link GraphPayloadLink
	found := false
	for _, edge := range append(append([]GraphPayloadLink(nil), receipt.Blobs...), receipt.EntryBlob) {
		if edge.Hash == candidate.Runtime.SHA256 {
			link = edge
			found = true
			break
		}
	}
	if !found {
		return nil, ErrGap
	}
	frameBytes, err := v.Payload(ctx, receipt.Index, link, checkpoint.MaxBytes)
	if err != nil {
		return nil, err
	}
	runtime := candidate.Runtime
	frame, err := checkpoint.Decode(frameBytes, runtime.SHA256, checkpoint.Identity{Type: typ, ID: id, InvSeq: runtime.InvSeq}, checkpoint.Anchor{Index: runtime.Index, Epoch: runtime.Epoch})
	if err != nil {
		return nil, fmt.Errorf("%w: graph continuation frame: %v", ErrGap, err)
	}
	digest := sha256.Sum256(frame.Data)
	if frame.Stage != runtime.Stage || frame.StepPosition != runtime.StepPosition || hex.EncodeToString(digest[:]) != localsHash {
		return nil, ErrGap
	}
	candidate.Frame = frameBytes
	candidate.Records = append([]Record(nil), records[runtime.Index+1:]...)
	candidate.Tail = v.Tail()
	if err := v.alive(); err != nil {
		return nil, err
	}
	return candidate, nil
}

// ConfirmCheckpoint verifies a handoff's exact frame and observed tail using a
// newly acquired pin, then releases the pin before reporting confirmation.
// It performs no archive or prefix compaction. An uncertain release fails the
// confirmation, so callers cannot use a partial read to authorize handoff.
func (s *GraphStore) ConfirmCheckpoint(ctx context.Context, typ, id string, runtime RuntimeCheckpoint, tail uint64) (err error) {
	view, err := s.OpenExisting(ctx, typ, id, runtime.InvSeq)
	if err != nil {
		return err
	}
	if view == nil {
		return ErrStale
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer cancel()
		if closeErr := view.Close(cleanup); err == nil && closeErr != nil {
			err = closeErr
		}
	}()
	verified, err := view.ReadCheckpoint(ctx, typ, id)
	if err != nil {
		return err
	}
	if verified == nil || verified.Runtime != runtime {
		return ErrGap
	}
	if verified.Tail != tail {
		return ErrStale
	}
	return nil
}
