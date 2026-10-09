package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"js-wf/identity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/wf"
)

// ContinuationHandler receives the original input and the preceding boundary's
// explicit locals. Names must remain registered while their checkpoints exist.
type ContinuationHandler func(*wf.Context, json.RawMessage, json.RawMessage) (json.RawMessage, error)

// WorkflowDefinition packages an initial handler with its named continuation
// stages. Go plugins can export a definition for offline replay or a map of
// definitions for wf-worker. Keep names registered while checkpoints exist.
type WorkflowDefinition struct {
	Handler       Handler
	Continuations map[string]ContinuationHandler
}

// Validate checks the exported handler and stage registrations before startup.
func (d WorkflowDefinition) Validate() error {
	if d.Handler == nil {
		return fmt.Errorf("workflow definition has no initial handler")
	}
	for name, handler := range d.Continuations {
		if identity.ValidateToken(name) != nil || handler == nil {
			return fmt.Errorf("invalid continuation stage %q", name)
		}
	}
	return nil
}

// WithContinuations opts an existing workflow type into boundary compaction.
// Registration is copied at construction and must be identical on all workers.
func WithContinuations(typ string, stages map[string]ContinuationHandler) Option {
	return func(w *Worker) error {
		if identity.ValidateToken(typ) != nil || w.Handlers[typ] == nil || len(stages) == 0 {
			return fmt.Errorf("invalid continuation registration for %q", typ)
		}
		if w.continuations[typ] != nil {
			return fmt.Errorf("duplicate continuation registration for %q", typ)
		}
		registered := make(map[string]ContinuationHandler, len(stages))
		for name, handler := range stages {
			if identity.ValidateToken(name) != nil || handler == nil {
				return fmt.Errorf("invalid continuation stage %q", name)
			}
			registered[name] = handler
		}
		if w.continuations == nil {
			w.continuations = make(map[string]map[string]ContinuationHandler)
		}
		w.continuations[typ] = registered
		return nil
	}
}

func containsTimer(steps []uint64, step uint64) bool {
	for _, candidate := range steps {
		if candidate == step {
			return true
		}
	}
	return false
}

// Count runtime facts only through the recorded completion. Later deliveries
// can add attempts and signals without changing the already stored frame.
func continuationAnchor(records []journal.Record, next, epoch uint64, info wf.CheckpointInfo, completed uint64, recorded bool) (wf.ContinuationAnchor, error) {
	anchor := wf.ContinuationAnchor{Epoch: epoch, PanicAttempts: info.PanicAttempts, SignalCursor: info.SignalCursor}
	for _, record := range records {
		if completed != 0 && record.Index > completed {
			break
		}
		switch record.Kind {
		case journal.Attempt:
			attempt, err := journal.DecodeAttempt(record.Payload)
			if err != nil || uint64(attempt.Count) != anchor.PanicAttempts+1 {
				return wf.ContinuationAnchor{}, wf.ErrCorruptJournal
			}
			anchor.PanicAttempts = uint64(attempt.Count)
		case journal.SignalConsumed:
			var signal signalRecord
			if json.Unmarshal(record.Payload, &signal) != nil || signal.Sequence <= anchor.SignalCursor {
				return wf.ContinuationAnchor{}, wf.ErrCorruptJournal
			}
			anchor.SignalCursor = signal.Sequence
		}
		if completed != 0 && record.Index == completed {
			if record.Kind != journal.StepCompleted {
				return wf.ContinuationAnchor{}, wf.ErrCorruptJournal
			}
			anchor.Index, anchor.Epoch = record.Index, record.Epoch
			return anchor, nil
		}
	}
	if completed != 0 {
		return wf.ContinuationAnchor{}, wf.ErrCorruptJournal
	}
	anchor.Index = next
	if !recorded {
		anchor.Index++
	}
	return anchor, nil
}

func (w *Worker) publishContinuation(ctx context.Context, typ, id string, invSeq uint64, owner *lease.Lease, records []journal.Record, point wf.ContinuationCheckpoint, appendEntry func(journal.Kind, json.RawMessage) error) error {
	var sequence uint64
	for _, record := range records {
		if record.Index == point.Index {
			sequence = record.Sequence
			break
		}
	}
	if sequence == 0 {
		return wf.ErrCorruptJournal
	}
	runtime := journal.RuntimeCheckpoint{InvSeq: invSeq, Stage: point.Stage, Sequence: sequence, Index: point.Index, Epoch: point.Epoch, StepPosition: point.StepPosition, Object: point.Object, SHA256: point.SHA256}
	if err := owner.Renew(ctx); err != nil {
		return err
	}
	if w.graphJournal != nil {
		if err := w.graphJournal.ConfirmCheckpoint(ctx, typ, id, runtime, records[len(records)-1].Sequence); err != nil {
			return err
		}
	} else {
		snapshot, err := w.jrn.WriteCheckpointSnapshot(ctx, typ, id, runtime)
		if err != nil {
			return err
		}
		if err := owner.Renew(ctx); err != nil {
			return err
		}
		if err := w.jrn.PurgeSnapshot(ctx, typ, id, snapshot); err != nil {
			return err
		}
	}
	payload, _ := json.Marshal(struct {
		WaitingOn string `json:"waiting_on"`
	}{"continuation:" + point.Stage})
	last := records[len(records)-1]
	if last.Kind != journal.Suspended || !bytes.Equal(last.Payload, payload) {
		if err := appendEntry(journal.Suspended, payload); err != nil {
			return err
		}
	}
	if err := owner.Renew(ctx); err != nil {
		return err
	}
	messageID := fmt.Sprintf("continuation:%d:%d", invSeq, sequence)
	if w.graphJournal != nil {
		// Recovery dispatch must survive a prior delivery inside the dedup window.
		messageID = ""
	}
	return w.client.Enqueue(ctx, typ, id, messageID)
}

// WithJournalStore supplies the worker's journal transport, including snapshot
// reads and writes. It supports transport contracts and embedded deployments.
func WithJournalStore(store *journal.Store) Option {
	return func(w *Worker) error {
		if store == nil {
			return fmt.Errorf("nil worker journal store")
		}
		w.jrn = store
		w.legacyJournalOption = true
		return nil
	}
}
