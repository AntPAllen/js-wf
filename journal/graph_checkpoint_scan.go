package journal

import (
	"bytes"
	"context"
	"errors"

	"js-wf/internal/checkpoint"
	"js-wf/internal/stepwire"
)

// CheckpointScan resumes verification in bounded record batches against one
// exact GraphView. Construct it with NewCheckpointScan and keep that view alive.
// It retains parser state and the latest checkpoint's necessary suffix, rather
// than buffering the discarded prefix. It is process-local, not a durable proof.
// No partial result authorizes publication or continuation stage admission.
type CheckpointScan struct {
	view                            *GraphView
	typ, id                         string
	next, end                       uint64
	previousSequence, previousEpoch uint64
	request                         Record
	declaration                     stepwire.Request
	pending                         bool
	position                        uint64
	candidate                       *GraphCheckpointRead
	receipt                         GraphRecord
	localsHash                      string
	suffix                          []Record
	err                             error
	done                            bool
}

// NewCheckpointScan validates identity and the existing indexed anchor before
// any prefix work. Its caller supplies a bounded context for these reads too.
func (v *GraphView) NewCheckpointScan(ctx context.Context, typ, id string) (*CheckpointScan, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
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
	if err = v.RenewIfNeeded(ctx); err != nil {
		return nil, err
	}
	s := &CheckpointScan{view: v, typ: typ, id: id, end: v.Count()}
	if pointer := v.cursor.Checkpoint; pointer != nil {
		read := func(index uint64) (GraphRecord, error) {
			if err := v.RenewIfNeeded(ctx); err != nil {
				return GraphRecord{}, err
			}
			return v.Read(ctx, index)
		}
		anchor, err := read(pointer.Runtime.Index)
		if err != nil {
			return nil, err
		}
		declared, err := read(pointer.RequestIndex)
		if err != nil {
			return nil, err
		}
		if declared.Kind != StepRequested || stepwire.Decode(declared.Payload, &s.declaration) != nil || s.declaration.Kind != "checkpoint" || s.declaration.Name != pointer.Runtime.Stage || declared.Sequence >= anchor.Sequence || declared.Epoch > anchor.Epoch || verifyCheckpointAnchor(anchor.Record, pointer.Runtime) != nil {
			return nil, ErrGap
		}
		s.candidate = &GraphCheckpointRead{Runtime: pointer.Runtime, Request: declared.Record, Anchor: anchor.Record}
		s.receipt, s.localsHash, s.position = anchor, s.declaration.InputHash, pointer.Runtime.StepPosition
		if err = v.readCheckpointFrame(ctx, typ, id, s.candidate, anchor, s.localsHash); err != nil {
			return nil, err
		}
		s.next = pointer.Runtime.Index + 1
		s.previousSequence, s.previousEpoch = anchor.Sequence, anchor.Epoch
	}
	return s, nil
}

// NextIndex is the next absolute logical record requiring verification. It is
// progress against this pinned forest, not a committed journal/checkpoint cursor.
func (s *CheckpointScan) NextIndex() uint64 { return s.next }

func (s *CheckpointScan) fail(err error) (*GraphCheckpointRead, bool, error) {
	// Only deadline/cancellation pauses are resumable. Other failures retain a
	// sticky rejection; a fresh scan can perform a separate authority observation.
	if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		s.err = err
	}
	return nil, false, err
}

// Advance verifies at most maxRecords new logical records, using fresh range
// reads and reader renewal. Cancellation/deadline preserves only fully verified
// records, permitting a new context to continue. Other errors fail closed.
// done=true is returned only after the owned final frame and pin are verified.
// Record budgets bound work count; callers also need native request deadlines.
func (s *CheckpointScan) Advance(ctx context.Context, maxRecords uint64) (*GraphCheckpointRead, bool, error) {
	if s.err != nil {
		return nil, false, s.err
	}
	if maxRecords == 0 {
		return nil, false, ErrGap
	}
	if err := ctx.Err(); err != nil {
		return s.fail(err)
	}
	if err := s.view.alive(); err != nil {
		return s.fail(err)
	}
	if s.done {
		return s.result(), true, nil
	}
	if err := s.view.RenewIfNeeded(ctx); err != nil {
		return s.fail(err)
	}
	end := s.next + min(maxRecords, s.end-s.next)
	err := s.view.ReadRange(ctx, s.next, end, func(record GraphRecord) error {
		if err := s.view.RenewIfNeeded(ctx); err != nil {
			return err
		}
		if record.Index != s.next || s.next > 0 && (record.Sequence != s.previousSequence+1 || record.Epoch < s.previousEpoch) {
			return ErrGap
		}
		if err := s.consume(record); err != nil {
			return err
		}
		s.next++
		s.previousSequence, s.previousEpoch = record.Sequence, record.Epoch
		return nil
	})
	if err != nil {
		return s.fail(err)
	}
	if s.next != s.end {
		return nil, false, nil
	}
	if s.candidate != nil {
		if s.candidate.Frame == nil {
			if err := s.view.readCheckpointFrame(ctx, s.typ, s.id, s.candidate, s.receipt, s.localsHash); err != nil {
				return s.fail(err)
			}
		}
		s.candidate.Records = append([]Record(nil), s.suffix...)
		s.candidate.Tail = s.view.Tail()
		pointer := s.view.cursor.Checkpoint
		published := !s.view.store.cfg.CheckpointIndex || pointer != nil && pointer.Runtime == s.candidate.Runtime && pointer.RequestIndex == s.candidate.Request.Index
		archived := !s.view.store.cfg.ArchiveCheckpoints || s.view.cursor.RetainedFrom == s.candidate.Request.Index
		suspended := false
		for _, record := range s.suffix {
			var marker struct {
				WaitingOn string `json:"waiting_on"`
			}
			if record.Kind == Suspended && checkpoint.DecodeUnambiguous(record.Payload, &marker) == nil && marker.WaitingOn == "continuation:"+s.candidate.Runtime.Stage {
				suspended = true
				break
			}
		}
		s.candidate.HandoffPending = !published || !archived || !suspended
	}
	if err := ctx.Err(); err != nil {
		return s.fail(err)
	}
	if err := s.view.alive(); err != nil {
		return s.fail(err)
	}
	s.done = true
	return s.result(), true, nil
}

// Keep the verified internal state private when callers edit their read result.
func (s *CheckpointScan) result() *GraphCheckpointRead {
	if s.candidate == nil {
		return nil
	}
	copy := *s.candidate
	copy.Frame = bytes.Clone(copy.Frame)
	copy.Request.Payload = bytes.Clone(copy.Request.Payload)
	copy.Anchor.Payload = bytes.Clone(copy.Anchor.Payload)
	if copy.Records != nil {
		copy.Records = append([]Record(nil), copy.Records...)
		for i := range copy.Records {
			copy.Records[i].Payload = bytes.Clone(copy.Records[i].Payload)
		}
	}
	return &copy
}

func (s *CheckpointScan) consume(record GraphRecord) error {
	switch record.Kind {
	case StepRequested:
		if s.pending {
			return ErrGap
		}
		var declaration stepwire.Request
		if stepwire.Decode(record.Payload, &declaration) != nil {
			return ErrGap
		}
		s.request, s.declaration, s.pending = record.Record, declaration, true
		s.position++
	case StepCompleted:
		if !s.pending {
			return ErrGap
		}
		var completion stepwire.Completion
		if stepwire.DecodeCompletion(record.Payload, &completion) != nil {
			return ErrGap
		}
		position := s.position + 1
		if s.declaration.Kind == "checkpoint" {
			runtime := RuntimeCheckpoint{InvSeq: s.view.cursor.Invocation, Stage: s.declaration.Name, Sequence: record.Sequence, Index: record.Index, Epoch: record.Epoch, StepPosition: position, Object: completion.ResultRef, SHA256: completion.ResultHash}
			if ValidateRuntimeSnapshot(Snapshot{Version: 2, Runtime: &runtime}) != nil || verifyCheckpointAnchor(record.Record, runtime) != nil || s.request.Index >= record.Index {
				return ErrGap
			}
			s.candidate = &GraphCheckpointRead{Runtime: runtime, Request: s.request, Anchor: record.Record}
			s.receipt, s.localsHash = record, s.declaration.InputHash
			s.suffix = nil
			s.pending, s.position = false, position
			return nil
		}
		s.pending, s.position = false, position
	}
	if s.candidate != nil {
		s.suffix = append(s.suffix, record.Record)
	}
	return nil
}
