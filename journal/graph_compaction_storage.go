package journal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
)

// CompactionCheckpointPort stores descriptors in a dedicated KV namespace.
// Mutations must implement exact revision CAS. Descriptors are untrusted input;
// storage revisions do not grant lease ownership or certify verified content.
type CompactionCheckpointPort interface {
	Create(context.Context, string, []byte) (uint64, error)
	Get(context.Context, string) ([]byte, uint64, error)
	Update(context.Context, string, []byte, uint64) (uint64, error)
	Delete(context.Context, string, uint64) error
}

type nativeCompactionCheckpointPort struct{ kv jetstream.KeyValue }

func (s *GraphStore) HasCompactionCheckpointStorage() bool { return s.cfg.CompactionCheckpoints != nil }

// CheckpointCompactionHandoff is a canonical scheduling hint, never a content
// certificate. Recovery must independently verify saved staging before publish.
type CheckpointCompactionHandoff struct {
	Runtime  RuntimeCheckpoint
	Tail     uint64
	Complete bool
}

// InspectCheckpointCompactionHandoff observes an exact checkpoint/suspension
// boundary without acquiring a reader and changing saved source authority.
// Its invocation pointer must match the canonical Start before maintenance.
func (s *GraphStore) InspectCheckpointCompactionHandoff(ctx context.Context, typ, id string, input *jetstream.RawStreamMsg) (*CheckpointCompactionHandoff, error) {
	if !s.cfg.ArchiveCheckpoints || input == nil || input.Sequence == 0 {
		return nil, ErrGap
	}
	_, _, cursor, err := s.observe(ctx, typ, id)
	if err != nil {
		return nil, err
	}
	if cursor == nil || cursor.Invocation != input.Sequence || cursor.Retired || cursor.Purging {
		return nil, nil
	}
	if cursor.Start == nil || !cursor.Start.MatchesInvocation(input) {
		return nil, ErrGap
	}
	if cursor.Kind != Suspended || cursor.Checkpoint == nil || cursor.Base+cursor.Count != cursor.Checkpoint.Runtime.Sequence+1 {
		return nil, nil
	}
	return &CheckpointCompactionHandoff{Runtime: cursor.Checkpoint.Runtime, Tail: cursor.Base + cursor.Count, Complete: cursor.RetainedFrom == cursor.Checkpoint.RequestIndex}, nil
}

// NewCompactionCheckpointPort adapts a separately provisioned KV bucket. Use no
// TTL if descriptors must survive process outages; grant expiry still limits
// whether their original publication can resume. This does not provision a bucket.
func NewCompactionCheckpointPort(kv jetstream.KeyValue) CompactionCheckpointPort {
	return nativeCompactionCheckpointPort{kv}
}

func (p nativeCompactionCheckpointPort) Create(ctx context.Context, key string, data []byte) (uint64, error) {
	return p.kv.Create(ctx, key, data)
}
func (p nativeCompactionCheckpointPort) Get(ctx context.Context, key string) ([]byte, uint64, error) {
	e, err := p.kv.Get(ctx, key)
	if err != nil {
		return nil, 0, err
	}
	return e.Value(), e.Revision(), nil
}
func (p nativeCompactionCheckpointPort) Update(ctx context.Context, key string, data []byte, revision uint64) (uint64, error) {
	return p.kv.Update(ctx, key, data, revision)
}
func (p nativeCompactionCheckpointPort) Delete(ctx context.Context, key string, revision uint64) error {
	return p.kv.Delete(ctx, key, jetstream.LastRevision(revision))
}

func compactionStorageKey(typ, id string, runtime RuntimeCheckpoint, tail uint64) (string, error) {
	if err := identity.Validate(typ, id); err != nil {
		return "", err
	}
	if runtime.InvSeq == 0 || runtime.Sequence == 0 || tail < runtime.Sequence {
		return "", ErrGap
	}
	data, err := json.Marshal(struct {
		Type, ID string
		Runtime  RuntimeCheckpoint
		Tail     uint64
	}{typ, id, runtime, tail})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return "compaction.v1." + hex.EncodeToString(sum[:]), nil
}

func compactionStorageMutationError(err error) error {
	if err == nil {
		return nil
	}
	var api *jetstream.APIError
	if errors.Is(err, jetstream.ErrKeyExists) || errors.Is(err, jetstream.ErrKeyRevisionMismatch) || errors.As(err, &api) && (api.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence || api.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequenceConstant) {
		return fmt.Errorf("%w: compaction descriptor CAS: %w", ErrStale, err)
	}
	return fmt.Errorf("%w: compaction descriptor mutation: %w", ErrUnknown, err)
}

// SaveCheckpoint stores the current portable staging/renewal input. Zero creates;
// a positive expected revision updates exactly that observation. Unknown replies
// are returned without retry or readback. Stop this delivery and reload on a
// fresh recovery attempt. Save pending renewal before advancing its grant CASes.
func (c *CheckpointCompaction) SaveCheckpoint(ctx context.Context, expected uint64) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	port := c.store.cfg.CompactionCheckpoints
	if port == nil {
		return 0, fmt.Errorf("compaction checkpoint storage is not configured")
	}
	key, err := compactionStorageKey(c.typ, c.id, c.runtime, c.tail)
	if err != nil {
		return 0, err
	}
	data, err := c.Checkpoint()
	if err != nil {
		return 0, err
	}
	var revision uint64
	if expected == 0 {
		revision, err = port.Create(ctx, key, data)
	} else {
		revision, err = port.Update(ctx, key, data, expected)
	}
	if err != nil {
		return 0, compactionStorageMutationError(err)
	}
	if revision == 0 || revision <= expected {
		return 0, fmt.Errorf("%w: invalid compaction storage revision", ErrUnknown)
	}
	return revision, nil
}

// ResumeStoredCheckpointCompaction reads a descriptor and freshly validates its
// complete identity, canonical source head and requested expiry. Grant/content
// checks remain required by bounded Advance before publication.
// Missing storage returns nil/zero. Read errors, stale roots, expiry and corrupt
// input never fall back to a new operation. Verification always starts fresh.
func (s *GraphStore) ResumeStoredCheckpointCompaction(ctx context.Context, typ, id string, runtime RuntimeCheckpoint, tail uint64) (*CheckpointCompaction, uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	if s.cfg.CompactionCheckpoints == nil {
		return nil, 0, fmt.Errorf("compaction checkpoint storage is not configured")
	}
	key, err := compactionStorageKey(typ, id, runtime, tail)
	if err != nil {
		return nil, 0, err
	}
	data, revision, err := s.cfg.CompactionCheckpoints.Get(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) || errors.Is(err, jetstream.ErrKeyDeleted) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	if revision == 0 {
		return nil, 0, ErrGap
	}
	op, err := s.ResumeCheckpointCompaction(ctx, typ, id, runtime, tail, data)
	if err != nil {
		return nil, revision, err
	}
	if !s.cfg.Now().Before(op.stage.IntentExpiry()) {
		return nil, revision, ErrStale
	}
	return op, revision, nil
}

// DeleteCompactionCheckpoint removes only the caller's observed revision. Lease
// ownership must be checked by its caller. An unknown delete is never retried;
// an old descriptor cannot authorize publication after the source head changes.
func (s *GraphStore) DeleteCompactionCheckpoint(ctx context.Context, typ, id string, runtime RuntimeCheckpoint, tail, expected uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.cfg.CompactionCheckpoints == nil || expected == 0 {
		return ErrGap
	}
	key, err := compactionStorageKey(typ, id, runtime, tail)
	if err != nil {
		return err
	}
	return compactionStorageMutationError(s.cfg.CompactionCheckpoints.Delete(ctx, key, expected))
}
