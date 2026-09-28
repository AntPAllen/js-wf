// Package assignment stores durable, revision-fenced partition ownership.
package assignment

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"js-wf/provision"

	"github.com/nats-io/nats.go/jetstream"
)

var ErrConflict = errors.New("partition assignment changed")

type Store struct{ kv jetstream.KeyValue }

func New(ctx context.Context, js jetstream.JetStream) (*Store, error) {
	kv, err := js.KeyValue(ctx, "WF_ASSIGN")
	if err != nil {
		return nil, err
	}
	return &Store{kv: kv}, nil
}

func key(partition uint32) (string, error) {
	if partition >= provision.Partitions {
		return "", fmt.Errorf("partition %d out of range", partition)
	}
	return fmt.Sprintf("p%02d", partition), nil
}

// ParseKey accepts only the canonical keys for the 64 dispatch partitions.
func ParseKey(value string) (uint32, error) {
	if len(value) != 3 || value[0] != 'p' {
		return 0, fmt.Errorf("invalid assignment key %q", value)
	}
	n, err := strconv.ParseUint(value[1:], 10, 32)
	if err != nil || n >= uint64(provision.Partitions) {
		return 0, fmt.Errorf("invalid assignment key %q", value)
	}
	return uint32(n), nil
}

func validOwner(owner string) error {
	if owner == "" || strings.TrimSpace(owner) != owner || len(owner) > 128 {
		return fmt.Errorf("invalid assignment owner %q", owner)
	}
	return nil
}

// Get returns an empty owner and revision zero when no owner is assigned.
func (s *Store) Get(ctx context.Context, partition uint32) (string, uint64, error) {
	k, err := key(partition)
	if err != nil {
		return "", 0, err
	}
	e, err := s.kv.Get(ctx, k)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return "", 0, nil
	}
	if err != nil {
		return "", 0, err
	}
	owner := string(e.Value())
	if err := validOwner(owner); err != nil {
		return "", 0, err
	}
	return owner, e.Revision(), nil
}

// GetLatest also reports a delete marker's revision. It is used to reconcile
// a missed delete without allowing an older watch event to restore its owner.
func (s *Store) GetLatest(ctx context.Context, partition uint32) (string, uint64, error) {
	owner, revision, err := s.Get(ctx, partition)
	if err != nil || revision != 0 {
		return owner, revision, err
	}
	k, err := key(partition)
	if err != nil {
		return "", 0, err
	}
	history, err := s.kv.History(ctx, k)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return "", 0, nil
	}
	if err != nil {
		return "", 0, err
	}
	if len(history) == 0 {
		return "", 0, nil
	}
	last := history[len(history)-1]
	if last.Operation() != jetstream.KeyValuePut {
		return "", last.Revision(), nil
	}
	owner = string(last.Value())
	if err := validOwner(owner); err != nil {
		return "", 0, err
	}
	return owner, last.Revision(), nil
}

// Assign creates or moves a partition only if its revision still matches.
// expectedRevision zero means the partition must currently be unassigned.
func (s *Store) Assign(ctx context.Context, partition uint32, owner string, expectedRevision uint64) (uint64, error) {
	k, err := key(partition)
	if err != nil {
		return 0, err
	}
	if err := validOwner(owner); err != nil {
		return 0, err
	}
	var revision uint64
	if expectedRevision == 0 {
		revision, err = s.kv.Create(ctx, k, []byte(owner))
	} else {
		revision, err = s.kv.Update(ctx, k, []byte(owner), expectedRevision)
	}
	if errors.Is(err, jetstream.ErrKeyExists) || errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
		return 0, fmt.Errorf("partition %d: %w: %v", partition, ErrConflict, err)
	}
	return revision, err
}

// InitializeStatic fills missing partitions for a fixed set of workers.
// Existing assignments remain in place so a restart preserves later moves.
func (s *Store) InitializeStatic(ctx context.Context, workers []string) error {
	if len(workers) == 0 || len(workers) > int(provision.Partitions) {
		return fmt.Errorf("worker count must be between 1 and %d", provision.Partitions)
	}
	seen := make(map[string]bool, len(workers))
	for _, owner := range workers {
		if err := validOwner(owner); err != nil {
			return err
		}
		if seen[owner] {
			return fmt.Errorf("duplicate worker ID %q", owner)
		}
		seen[owner] = true
	}
	for partition := uint32(0); partition < provision.Partitions; partition++ {
		_, err := s.Assign(ctx, partition, workers[int(partition)%len(workers)], 0)
		if errors.Is(err, ErrConflict) {
			continue
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// WatchAll includes the current assignment snapshot and subsequent changes.
func (s *Store) WatchAll(ctx context.Context) (jetstream.KeyWatcher, error) {
	return s.kv.WatchAll(ctx)
}
