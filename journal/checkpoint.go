package journal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"js-wf/identity"
	"js-wf/internal/checkpoint"
)

var ErrCheckpointCompaction = errors.New("continuation journal must compact at a checkpoint boundary")

// RuntimeCheckpoint is the manifest's fast-resume pointer. Its completion
// anchor remains in the live journal while the archival prefix supports audit.
type RuntimeCheckpoint struct {
	InvSeq       uint64 `json:"inv_seq"`
	Stage        string `json:"stage"`
	Sequence     uint64 `json:"sequence"`
	Index        uint64 `json:"index"`
	Epoch        uint64 `json:"epoch"`
	StepPosition uint64 `json:"step_position"`
	Object       string `json:"object"`
	SHA256       string `json:"sha256"`
}

// ValidateRuntimeSnapshot checks pointer shape and that prefix purge cannot
// remove its anchor. Frame bytes and the actual anchor need separate checks.
func ValidateRuntimeSnapshot(snap Snapshot) error {
	r := snap.Runtime
	if r == nil {
		if snap.Version == 2 {
			return ErrGap
		}
		return nil
	}
	if snap.Version != 2 {
		return ErrGap
	}
	hash, err := hex.DecodeString(r.SHA256)
	if err != nil || len(hash) != 32 || hex.EncodeToString(hash) != r.SHA256 || r.Object != "step-result-"+r.SHA256 ||
		r.InvSeq == 0 || identity.ValidateToken(r.Stage) != nil || r.Epoch == 0 || r.Index >= MaxEntries || r.Sequence <= snap.LastSeq || r.Index <= snap.LastIndex ||
		r.StepPosition < 2 || r.StepPosition%2 != 0 || r.StepPosition > r.Index {
		return ErrGap
	}
	return nil
}

// WriteCheckpointSnapshot publishes a verified archive and runtime pointer in
// one manifest CAS. It does not purge. Call PurgeSnapshot only after success;
// retrying after a lost manifest acknowledgement confirms the same pointer.
// The caller must hold the invocation lease and fence every journal append.
func (s *Store) WriteCheckpointSnapshot(ctx context.Context, typ, id string, runtime RuntimeCheckpoint) (Snapshot, error) {
	return s.writeSnapshot(ctx, typ, id, 1, &runtime)
}

func (s *Store) verifyRuntimeCheckpoint(ctx context.Context, typ, id string, records []Record, runtime RuntimeCheckpoint) error {
	// Check shape before indexing or making object requests.
	if ValidateRuntimeSnapshot(Snapshot{Version: 2, Runtime: &runtime}) != nil || runtime.Index >= uint64(len(records)) {
		return ErrGap
	}
	anchor := records[runtime.Index]
	if err := verifyCheckpointAnchor(anchor, runtime); err != nil {
		return err
	}
	var sdkPosition uint64
	var request Record
	pending := false
	for _, record := range records[:runtime.Index+1] {
		if record.Kind == StepRequested {
			if pending {
				return ErrGap
			}
			pending = true
			request = record
			sdkPosition++
		} else if record.Kind == StepCompleted {
			if !pending {
				return ErrGap
			}
			pending = false
			sdkPosition++
		}
	}
	var declared struct {
		Kind      string `json:"kind"`
		Name      string `json:"name"`
		InputHash string `json:"input_hash"`
	}
	if sdkPosition != runtime.StepPosition || request.Kind != StepRequested || json.Unmarshal(request.Payload, &declared) != nil || declared.Kind != "checkpoint" || declared.Name != runtime.Stage {
		return ErrGap
	}
	get := s.snapshotWritePort.GetObject
	var data []byte
	var err error
	if waiter, ok := s.snapshotWritePort.(interface {
		Wait(context.Context, time.Duration) error
	}); ok {
		data, err = verifiedSnapshotObjectVirtual(ctx, get, waiter.Wait, runtime.Object, runtime.SHA256)
	} else {
		data, err = verifiedSnapshotObject(ctx, get, runtime.Object, runtime.SHA256)
	}
	if err != nil {
		return err
	}
	frame, err := checkpoint.Decode(data, runtime.SHA256, checkpoint.Identity{Type: typ, ID: id, InvSeq: runtime.InvSeq}, checkpoint.Anchor{Index: runtime.Index, Epoch: runtime.Epoch})
	if err != nil {
		return fmt.Errorf("%w: continuation frame: %v", ErrGap, err)
	}
	inputDigest := sha256.Sum256(frame.Data)
	if frame.Stage != runtime.Stage || frame.StepPosition != runtime.StepPosition || declared.InputHash != hex.EncodeToString(inputDigest[:]) {
		return ErrGap
	}
	return nil
}

func verifyCheckpointAnchor(anchor Record, runtime RuntimeCheckpoint) error {
	if anchor.Index != runtime.Index || anchor.Sequence != runtime.Sequence || anchor.Epoch != runtime.Epoch || anchor.Kind != StepCompleted {
		return ErrGap
	}
	var done struct {
		Result     json.RawMessage `json:"result"`
		ResultRef  string          `json:"result_ref"`
		ResultHash string          `json:"result_hash"`
		Error      string          `json:"error"`
		ErrorKind  string          `json:"error_kind"`
		SignalSeq  uint64          `json:"signal_seq"`
		Selected   string          `json:"selected"`
	}
	if json.Unmarshal(anchor.Payload, &done) != nil || len(done.Result) != 0 || done.Error != "" || done.ErrorKind != "" || done.SignalSeq != 0 || done.Selected != "" || done.ResultRef != runtime.Object || done.ResultHash != runtime.SHA256 {
		return ErrGap
	}
	return nil
}
