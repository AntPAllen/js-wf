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

type graphCheckpointPointer struct {
	Runtime      RuntimeCheckpoint `json:"runtime"`
	RequestIndex uint64            `json:"request_index"`
}

// GraphCheckpointRead is a continuation frame and its anchored suffix from one
// pinned generation. Keep the GraphView alive while resolving payload references
// from the frame or suffix. The retained prefix remains available for audit.
// This does not compact the graph or publish a mutable resume manifest.
type GraphCheckpointRead struct {
	Runtime RuntimeCheckpoint
	Request Record
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
	start := uint64(0)
	var records []Record
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
	if pointer := v.cursor.Checkpoint; pointer != nil {
		start = pointer.Runtime.Index
		anchor, err := v.Read(ctx, start)
		if err != nil {
			return nil, err
		}
		declared, err := v.Read(ctx, pointer.RequestIndex)
		if err != nil {
			return nil, err
		}
		if declared.Kind != StepRequested || json.Unmarshal(declared.Payload, &declaration) != nil || declaration.Kind != "checkpoint" || declaration.Name != pointer.Runtime.Stage || declared.Sequence >= anchor.Sequence || declared.Epoch > anchor.Epoch || verifyCheckpointAnchor(anchor.Record, pointer.Runtime) != nil {
			return nil, ErrGap
		}
		candidate = &GraphCheckpointRead{Runtime: pointer.Runtime, Request: declared.Record, Anchor: anchor.Record}
		receipt = anchor
		localsHash = declaration.InputHash
		position = pointer.Runtime.StepPosition
		if err := v.readCheckpointFrame(ctx, typ, id, candidate, receipt, localsHash); err != nil {
			return nil, err
		}
		records = make([]Record, 0, v.Count()-start)
		records = append(records, anchor.Record)
	}
	if records == nil {
		records = make([]Record, 0, v.Count())
	}
	first := start
	if candidate != nil {
		first++
	}
	err = v.ReadRange(ctx, first, v.Count(), func(record GraphRecord) error {
		if len(records) > 0 {
			previous := records[len(records)-1]
			if record.Sequence != previous.Sequence+1 || record.Epoch < previous.Epoch {
				return ErrGap
			}
		}
		records = append(records, record.Record)
		switch record.Kind {
		case StepRequested:
			if pending {
				return ErrGap
			}
			declaration = struct {
				Kind      string `json:"kind"`
				Name      string `json:"name"`
				InputHash string `json:"input_hash"`
			}{}
			if json.Unmarshal(record.Payload, &declaration) != nil {
				return ErrGap
			}
			request = record.Record
			pending = true
			position++
		case StepCompleted:
			if !pending {
				return ErrGap
			}
			pending = false
			position++
			if declaration.Kind != "checkpoint" {
				return nil
			}
			var completion struct {
				ResultRef  string `json:"result_ref"`
				ResultHash string `json:"result_hash"`
			}
			if json.Unmarshal(record.Payload, &completion) != nil {
				return ErrGap
			}
			runtime := RuntimeCheckpoint{InvSeq: v.cursor.Invocation, Stage: declaration.Name, Sequence: record.Sequence, Index: record.Index, Epoch: record.Epoch, StepPosition: position, Object: completion.ResultRef, SHA256: completion.ResultHash}
			// Reuse the existing checkpoint contract without introducing an archive
			// identity into the graph. Started guarantees the anchor is not index zero.
			if ValidateRuntimeSnapshot(Snapshot{Version: 2, Runtime: &runtime}) != nil || verifyCheckpointAnchor(record.Record, runtime) != nil || request.Index >= record.Index {
				return ErrGap
			}
			candidate = &GraphCheckpointRead{Runtime: runtime, Request: request, Anchor: record.Record}
			receipt = record
			localsHash = declaration.InputHash
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if candidate == nil {
		return nil, nil
	}
	if candidate.Frame == nil {
		if err := v.readCheckpointFrame(ctx, typ, id, candidate, receipt, localsHash); err != nil {
			return nil, err
		}
	}
	candidate.Records = append([]Record(nil), records[candidate.Runtime.Index+1-start:]...)
	candidate.Tail = v.Tail()
	if err := v.alive(); err != nil {
		return nil, err
	}
	return candidate, nil
}

func (v *GraphView) readCheckpointFrame(ctx context.Context, typ, id string, candidate *GraphCheckpointRead, receipt GraphRecord, localsHash string) error {
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
		return ErrGap
	}
	frameBytes, err := v.Payload(ctx, receipt.Index, link, checkpoint.MaxBytes)
	if err != nil {
		return err
	}
	runtime := candidate.Runtime
	frame, err := checkpoint.Decode(frameBytes, runtime.SHA256, checkpoint.Identity{Type: typ, ID: id, InvSeq: runtime.InvSeq}, checkpoint.Anchor{Index: runtime.Index, Epoch: runtime.Epoch})
	if err != nil {
		return fmt.Errorf("%w: graph continuation frame: %v", ErrGap, err)
	}
	digest := sha256.Sum256(frame.Data)
	if frame.Stage != runtime.Stage || frame.StepPosition != runtime.StepPosition || hex.EncodeToString(digest[:]) != localsHash {
		return ErrGap
	}
	candidate.Frame = frameBytes
	return nil
}

// ConfirmCheckpoint verifies a handoff's exact frame and observed tail using a
// newly acquired pin, then releases the pin before reporting confirmation.
// It performs no archive or prefix compaction. An uncertain release fails the
// confirmation, so callers cannot use a partial read to authorize handoff.
func (s *GraphStore) ConfirmCheckpoint(ctx context.Context, typ, id string, runtime RuntimeCheckpoint, tail uint64) error {
	_, err := s.confirmCheckpoint(ctx, typ, id, runtime, tail)
	return err
}

func (s *GraphStore) confirmCheckpoint(ctx context.Context, typ, id string, runtime RuntimeCheckpoint, tail uint64) (verified *GraphCheckpointRead, err error) {
	view, err := s.OpenExisting(ctx, typ, id, runtime.InvSeq)
	if err != nil {
		return nil, err
	}
	if view == nil {
		return nil, ErrStale
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer cancel()
		if closeErr := view.Close(cleanup); err == nil && closeErr != nil {
			verified, err = nil, fmt.Errorf("%w: checkpoint reader release: %w", ErrUnknown, closeErr)
		}
	}()
	verified, err = view.ReadCheckpoint(ctx, typ, id)
	if err != nil {
		return nil, err
	}
	if verified == nil || verified.Runtime != runtime {
		return nil, ErrGap
	}
	if verified.Tail != tail {
		return nil, ErrStale
	}
	return verified, nil
}

// PublishCheckpoint publishes a v5 canonical resume pointer only after the
// owned frame, exact boundary and reader release have been confirmed. The same
// root CAS fences the generation, logical tail and pointer. Unknown CAS outcomes
// require a fresh caller observation; matching retries are idempotent.
// This retains full history and does not compact the graph or enable stages.
func (s *GraphStore) PublishCheckpoint(ctx context.Context, typ, id string, runtime RuntimeCheckpoint, tail uint64) error {
	if !s.cfg.CheckpointIndex {
		return fmt.Errorf("checkpoint publication requires the explicit v5 checkpoint index")
	}
	verified, err := s.confirmCheckpoint(ctx, typ, id, runtime, tail)
	if err != nil {
		return err
	}
	destination, root, cursor, err := s.observe(ctx, typ, id)
	if err != nil {
		return err
	}
	if cursor == nil || cursor.Invocation != runtime.InvSeq || cursor.Retired || cursor.Purging || cursor.Kind == Completed || cursor.Kind == Failed || cursor.Base+cursor.Count != tail {
		return ErrStale
	}
	pointer := graphCheckpointPointer{Runtime: runtime, RequestIndex: verified.Request.Index}
	if cursor.Checkpoint != nil {
		if *cursor.Checkpoint == pointer {
			return nil
		}
		if cursor.Checkpoint.Runtime.Index >= runtime.Index {
			return ErrStale
		}
	}
	next := *cursor
	next.Checkpoint = &pointer
	data, err := json.Marshal(next)
	if err != nil {
		return err
	}
	_, err = s.cfg.Protocol.UpdateApplication(ctx, destination, root.Head, data)
	return graphMutationError(err)
}
