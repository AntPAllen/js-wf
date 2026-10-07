package journal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"js-wf/identity"
	"js-wf/internal/checkpoint"

	"github.com/nats-io/nats.go/jetstream"
)

type checkpointReadCorruption struct{ reason string }

func (e *checkpointReadCorruption) Error() string { return fmt.Sprintf("%v: %s", ErrGap, e.reason) }
func (e *checkpointReadCorruption) Unwrap() error { return ErrGap }
func invalidCheckpointRead(reason string) error   { return &checkpointReadCorruption{reason: reason} }

var ErrCheckpointGeneration = errors.New("checkpoint belongs to another invocation generation")

// CheckpointRead contains verified frame bytes and only records after the live
// completion anchor. No archival object is fetched on this path.
type CheckpointRead struct {
	Snapshot Snapshot
	Frame    []byte
	Anchor   Record
	Records  []Record
	Tail     uint64
}

// ReadCheckpoint returns nil only for an absent manifest or an ordinary v1
// snapshot. Corruption and generation mismatch never become initial replay.
// The caller must check stage registration before executing a continuation.
func (s *Store) ReadCheckpoint(ctx context.Context, typ, id string, invSeq uint64) (*CheckpointRead, error) {
	if err := identity.Validate(typ, id); err != nil {
		return nil, err
	}
	if invSeq == 0 {
		return nil, fmt.Errorf("%w: %w", ErrGap, ErrCheckpointGeneration)
	}
	// A newer checkpoint can purge an older anchor between metadata and scan.
	// Retry the same invocation's manifest, not an incomplete old suffix.
	for attempt := 0; attempt <= 80; attempt++ {
		view, err := s.readCheckpointOnce(ctx, typ, id, invSeq)
		var objectGap *snapshotObjectGap
		var corrupt *checkpointReadCorruption
		if !errors.Is(err, ErrGap) || errors.Is(err, ErrCheckpointGeneration) || errors.As(err, &objectGap) || errors.As(err, &corrupt) || attempt == 80 {
			return view, err
		}
		wait := waitReadRequest
		if s.readPort != nil {
			wait = s.readPort.Wait
		}
		if err := wait(ctx, 25*time.Millisecond); err != nil {
			return nil, err
		}
	}
	panic("unreachable checkpoint read retry")
}

func (s *Store) checkpointManifest(ctx context.Context, typ, id string) (*Snapshot, error) {
	port := s.snapshotReadPort
	if port == nil {
		if s.js == nil {
			return nil, fmt.Errorf("checkpoint snapshot transport unavailable")
		}
		port = NewSnapshotReadPort(s.js)
	}
	wait := waitReadRequest
	if waiter, ok := port.(interface {
		Wait(context.Context, time.Duration) error
	}); ok {
		wait = waiter.Wait
	}
	key := snapshotKey(typ, id)
	raw, err := boundedReadRequest(ctx, wait, "checkpoint manifest "+key, func(request context.Context) ([]byte, error) { return port.GetManifest(request, key) })
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var snap Snapshot
	if json.Unmarshal(raw, &snap) != nil || (snap.Version != 1 && snap.Version != 2) || snap.Object == "" || ValidateRuntimeSnapshot(snap) != nil {
		return nil, invalidCheckpointRead("invalid checkpoint manifest")
	}
	hash, err := hex.DecodeString(snap.SHA256)
	keyDigest := sha256.Sum256([]byte(identity.Key(typ, id)))
	if err != nil || len(hash) != 32 || hex.EncodeToString(hash) != snap.SHA256 || !strings.HasPrefix(snap.Object, "snapshot-"+hex.EncodeToString(keyDigest[:8])+"-") {
		return nil, invalidCheckpointRead("invalid archive identity or hash metadata")
	}
	if snap.Runtime == nil {
		return nil, nil
	}
	return &snap, nil
}

func (s *Store) readCheckpointOnce(ctx context.Context, typ, id string, invSeq uint64) (*CheckpointRead, error) {
	snap, err := s.checkpointManifest(ctx, typ, id)
	if err != nil || snap == nil {
		return nil, err
	}
	runtime := *snap.Runtime
	if runtime.InvSeq != invSeq {
		return nil, fmt.Errorf("%w: %w", ErrGap, ErrCheckpointGeneration)
	}
	port := s.snapshotReadPort
	if port == nil {
		port = NewSnapshotReadPort(s.js)
	}
	var data []byte
	if waiter, ok := port.(interface {
		Wait(context.Context, time.Duration) error
	}); ok {
		data, err = verifiedSnapshotObjectVirtual(ctx, snapshotObjectGetter(port, snapshotKey(typ, id)), waiter.Wait, runtime.Object, runtime.SHA256)
	} else {
		data, err = verifiedSnapshotObject(ctx, snapshotObjectGetter(port, snapshotKey(typ, id)), runtime.Object, runtime.SHA256)
	}
	if err != nil {
		return nil, err
	}
	frame, err := checkpoint.Decode(data, runtime.SHA256, checkpoint.Identity{Type: typ, ID: id, InvSeq: invSeq}, checkpoint.Anchor{Index: runtime.Index, Epoch: runtime.Epoch})
	if err != nil || frame.Stage != runtime.Stage || frame.StepPosition != runtime.StepPosition {
		return nil, invalidCheckpointRead("invalid frame or runtime metadata mismatch")
	}
	var stream jetstream.Stream
	if s.readPort == nil {
		stream, err = s.journalStream(ctx)
		if err != nil {
			return nil, err
		}
	}
	batch := s.batchReadPort
	if batch == nil && stream != nil {
		batch = jetStreamBatchReadPort{stream: stream}
	}
	subject := identity.JournalSubject(typ, id)
	raw, err := s.nextLive(ctx, stream, subject, runtime.Sequence)
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return nil, ErrGap
	}
	if err != nil {
		return nil, err
	}
	var entry Entry
	if UnmarshalEntry(raw.Data, &entry) != nil {
		return nil, ErrGap
	}
	anchor := Record{Entry: entry, Sequence: raw.Sequence}
	if err := verifyCheckpointAnchor(anchor, runtime); err != nil {
		return nil, err
	}
	out := []Record{anchor}
	tail := anchor.Sequence
	seq := tail + 1
	serial := 0
	for {
		if batch != nil && serial == serialReadLimit {
			out, tail, _, err = readLiveBatchFrom(ctx, batch, subject, seq, out, tail, runtime.Index)
			if err != nil {
				return nil, err
			}
			break
		}
		raw, err := s.nextLive(ctx, stream, subject, seq)
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			break
		}
		if err != nil {
			return nil, err
		}
		var entry Entry
		if UnmarshalEntry(raw.Data, &entry) != nil {
			return nil, ErrGap
		}
		if entry.Index >= MaxEntries {
			return nil, ErrTooLong
		}
		out, err = verifyNextFrom(out, Record{Entry: entry, Sequence: raw.Sequence}, runtime.Index)
		if err != nil {
			return nil, err
		}
		tail = raw.Sequence
		seq = tail + 1
		serial++
	}
	for _, record := range out[1:] {
		if record.Index >= MaxEntries {
			return nil, ErrTooLong
		}
		switch record.Kind {
		case StepRequested, StepCompleted, Suspended, SignalConsumed, Attempt, Completed, Failed:
		default:
			return nil, ErrGap
		}
	}
	current, err := s.checkpointManifest(ctx, typ, id)
	if err != nil {
		return nil, err
	}
	if current == nil || *current.Runtime != runtime {
		return nil, ErrGap
	}
	return &CheckpointRead{Snapshot: *snap, Frame: data, Anchor: anchor, Records: out[1:], Tail: tail}, nil
}
