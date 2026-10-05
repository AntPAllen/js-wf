package lease

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"js-wf/identity"

	"github.com/nats-io/nats.go/jetstream"
)

var (
	ErrHeld = errors.New("workflow lease is held")
	ErrLost = errors.New("workflow lease was lost")
)

type Value struct {
	Worker string `json:"worker"`
	Epoch  uint64 `json:"epoch"`
}

type Store struct {
	kv   jetstream.KeyValue
	port KVPort
}

// KVPort is the revision-CAS boundary used by lease decisions. It allows the
// same Acquire, Renew, Release, and Cleanup code to run against a deterministic
// KV model without implementing the full JetStream KeyValue interface.
type KVPort interface {
	Create(context.Context, string, []byte) (uint64, error)
	Get(context.Context, string) (KVEntry, error)
	Update(context.Context, string, []byte, uint64) (uint64, error)
	Delete(context.Context, string, uint64) error
	Now() time.Time
}

type KVEntry struct {
	Value    []byte
	Revision uint64
	Created  time.Time
}

// NewWithKVPort builds a lease store over a deterministic KV transport.
func NewWithKVPort(port KVPort) *Store { return &Store{port: port} }

type jetStreamKVPort struct{ kv jetstream.KeyValue }

func (p jetStreamKVPort) Create(ctx context.Context, key string, value []byte) (uint64, error) {
	revision, err := p.kv.Create(ctx, key, value)
	if err != nil && !errors.Is(err, jetstream.ErrKeyExists) {
		var apiErr *jetstream.APIError
		if errors.As(err, &apiErr) && (apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence || apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequenceConstant) {
			// Create can expose the raw CAS response when concurrent callers
			// replace an expired key's delete marker. It still means another
			// contender won this revision race.
			return 0, fmt.Errorf("%w: %w", err, jetstream.ErrKeyExists)
		}
	}
	return revision, err
}

func (p jetStreamKVPort) Get(ctx context.Context, key string) (KVEntry, error) {
	entry, err := p.kv.Get(ctx, key)
	if err != nil {
		return KVEntry{}, err
	}
	return KVEntry{Value: entry.Value(), Revision: entry.Revision(), Created: entry.Created()}, nil
}

func (p jetStreamKVPort) Update(ctx context.Context, key string, value []byte, revision uint64) (uint64, error) {
	return p.kv.Update(ctx, key, value, revision)
}

func (p jetStreamKVPort) Delete(ctx context.Context, key string, revision uint64) error {
	return p.kv.Delete(ctx, key, jetstream.LastRevision(revision))
}

func (jetStreamKVPort) Now() time.Time { return time.Now() }

func (s *Store) operations() KVPort {
	if s.port != nil {
		return s.port
	}
	return jetStreamKVPort{kv: s.kv}
}

func New(ctx context.Context, js jetstream.JetStream) (*Store, error) {
	kv, err := js.KeyValue(ctx, "WF_LEASE")
	if err != nil {
		return nil, err
	}
	return NewWithKeyValue(kv), nil
}

// NewWithKeyValue uses a supplied bucket with the normal JetStream adapter.
// This also permits short-TTL contract tests without changing WF_LEASE.
func NewWithKeyValue(kv jetstream.KeyValue) *Store { return &Store{kv: kv} }

type Lease struct {
	store       *Store
	key         string
	value       Value
	gate        chan struct{}
	revision    uint64
	lost        bool
	lastRenewal time.Time
}

func (l *Lease) Epoch() uint64 { return l.value.Epoch }

// HeldObservation describes the entry already read during a failed acquisition.
// Created is the KV/server timestamp; ObservedAt is the client's clock. Neither
// proves server-side expiry, and reclaim_revision_conflict describes a prior
// read rather than the winning owner's current value.
type HeldObservation struct {
	Key           string    `json:"key"`
	Reason        string    `json:"reason"`
	EntryObserved bool      `json:"entry_observed"`
	Revision      uint64    `json:"revision,omitempty"`
	Created       time.Time `json:"created"`
	ObservedAt    time.Time `json:"observed_at"`
	ValueValid    bool      `json:"value_valid"`
	Worker        string    `json:"worker,omitempty"`
	Epoch         uint64    `json:"epoch,omitempty"`
}

// AcquireWithHeldObserver reports a held decision without another KV request.
// The observer must be fast and must not call back into this Store. Error and
// acquisition decisions are identical to Acquire, including ErrHeld identity.
func (s *Store) AcquireWithHeldObserver(ctx context.Context, typ, id, worker string, observe func(HeldObservation)) (*Lease, error) {
	return s.acquire(ctx, typ, id, worker, observe)
}

// Acquire uses the KV create CAS. The creation revision is the fencing epoch:
// it stays unique even when an earlier lease expires without a journal write.
func (s *Store) Acquire(ctx context.Context, typ, id, worker string) (*Lease, error) {
	return s.acquire(ctx, typ, id, worker, nil)
}

func (s *Store) acquire(ctx context.Context, typ, id, worker string, observe func(HeldObservation)) (*Lease, error) {
	if err := identity.Validate(typ, id); err != nil {
		return nil, err
	}
	if worker == "" {
		return nil, fmt.Errorf("empty worker ID")
	}
	key := identity.Key(typ, id)
	port := s.operations()
	heldError := func(reason string, entry *KVEntry) error {
		if observe != nil {
			o := HeldObservation{Key: key, Reason: reason, ObservedAt: port.Now()}
			if entry != nil {
				o.EntryObserved, o.Revision, o.Created = true, entry.Revision, entry.Created
				var value Value
				if json.Unmarshal(entry.Value, &value) == nil {
					o.ValueValid, o.Worker, o.Epoch = true, value.Worker, value.Epoch
				}
			}
			observe(o)
		}
		return ErrHeld
	}
	// The revision itself becomes the epoch, so the value is updated once by
	// its owner after Create. Until that update, other acquirers still fail.
	data, _ := json.Marshal(Value{Worker: worker})
	var rev uint64
	var err error
	for attempt := 0; attempt < 2; attempt++ {
		rev, err = port.Create(ctx, key, data)
		if err == nil {
			break
		}
		if !errors.Is(err, jetstream.ErrKeyExists) {
			return nil, err
		}
		if attempt > 0 {
			return nil, heldError("create_race_after_missing_entry", nil)
		}
		entry, getErr := port.Get(ctx, key)
		if errors.Is(getErr, jetstream.ErrKeyNotFound) {
			continue
		}
		if getErr != nil {
			return nil, getErr
		}
		var held Value
		if json.Unmarshal(entry.Value, &held) != nil || held.Worker == "" || held.Epoch != 0 || port.Now().Sub(entry.Created) < time.Second {
			return nil, heldError("held_entry", &entry)
		}
		// Create succeeded, but its owner never finished epoch initialization.
		// The revision CAS makes reclaim safe against an in-flight Update: only
		// one of Delete and Update can win, and no handler has started yet.
		if deleteErr := port.Delete(ctx, key, entry.Revision); deleteErr != nil {
			if errors.Is(deleteErr, jetstream.ErrKeyRevisionMismatch) {
				return nil, heldError("reclaim_revision_conflict", &entry)
			}
			return nil, deleteErr
		}
	}
	v := Value{Worker: worker, Epoch: rev}
	data, _ = json.Marshal(v)
	renewalStarted := port.Now()
	newRev, err := port.Update(ctx, key, data, rev)
	if err != nil {
		return nil, fmt.Errorf("%w: initialization: %w", ErrLost, err)
	}
	return &Lease{store: s, key: key, value: v, revision: newRev, lastRenewal: renewalStarted, gate: make(chan struct{}, 1)}, nil
}

// lock serializes revision decisions without keeping a canceled caller behind
// another operation's network request. Cancellation before acquiring the gate
// has not attempted a KV mutation and must not mark this lease lost.
func (l *Lease) lock(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case l.gate <- struct{}{}:
	}
	if err := ctx.Err(); err != nil {
		l.unlock()
		return err
	}
	return nil
}

func (l *Lease) unlock() { <-l.gate }

func (l *Lease) Renew(ctx context.Context) error {
	_, err := l.renew(ctx, 0, nil)
	return err
}

// RenewIfIdle avoids a redundant heartbeat update when an acknowledged renewal
// started less than minIdle ago. Journal writers must still call Renew before
// every append. The returned bool reports whether a KV update was attempted.
func (l *Lease) RenewIfIdle(ctx context.Context, minIdle time.Duration) (bool, error) {
	return l.renew(ctx, minIdle, nil)
}

// RenewalTiming separates local revision-gate waiting from the KV Update call.
// KV duration includes client/network time and is not server execution time.
type RenewalTiming struct {
	GateWait        time.Duration
	Update          time.Duration
	UpdateAttempted bool
}

// RenewTimed makes the same decision as RenewIfIdle, with optional diagnostics.
// minIdle=0 keeps renewal unconditional. Durations use the transport clock.
func (l *Lease) RenewTimed(ctx context.Context, minIdle time.Duration) (bool, RenewalTiming, error) {
	var timing RenewalTiming
	updated, err := l.renew(ctx, minIdle, &timing)
	return updated, timing, err
}

func (l *Lease) renew(ctx context.Context, minIdle time.Duration, timing *RenewalTiming) (bool, error) {
	var gateStarted time.Time
	if timing != nil {
		gateStarted = l.store.operations().Now()
	}
	lockErr := l.lock(ctx)
	if timing != nil {
		timing.GateWait = l.store.operations().Now().Sub(gateStarted)
	}
	if lockErr != nil {
		return false, lockErr
	}
	defer l.unlock()
	if l.lost {
		return false, ErrLost
	}
	port := l.store.operations()
	started := port.Now()
	age := started.Sub(l.lastRenewal)
	if minIdle > 0 && !l.lastRenewal.IsZero() && age >= 0 && age < minIdle {
		return false, nil
	}
	data, _ := json.Marshal(l.value)
	var updateStarted time.Time
	if timing != nil {
		updateStarted = port.Now()
		timing.UpdateAttempted = true
	}
	rev, err := port.Update(ctx, l.key, data, l.revision)
	if timing != nil {
		timing.Update = port.Now().Sub(updateStarted)
	}
	if err != nil {
		l.lost = true
		return true, fmt.Errorf("%w: %v", ErrLost, err)
	}
	l.revision = rev
	// Use request start, not receipt of its reply: a delayed acknowledgement
	// must not extend the interval during which a heartbeat can reuse this write.
	l.lastRenewal = started
	return true, nil
}

func (l *Lease) Release(ctx context.Context) error {
	if err := l.lock(ctx); err != nil {
		return err
	}
	defer l.unlock()
	if l.lost {
		return ErrLost
	}
	err := l.store.operations().Delete(ctx, l.key, l.revision)
	if err != nil {
		l.lost = true
		return fmt.Errorf("%w: %v", ErrLost, err)
	}
	l.lost = true
	return nil
}

// Cleanup resolves a failed or uncertain release after the handler has stopped.
// It may delete only the lease with this worker and fencing epoch, using the
// revision returned by KV. A successor's lease is never deleted.
func (l *Lease) Cleanup(ctx context.Context) error {
	if err := l.lock(ctx); err != nil {
		return err
	}
	defer l.unlock()
	port := l.store.operations()
	// An uncertain renewal can commit after the read but before the delete.
	// A revision conflict alone does not establish a successor: reread identity.
	for attempt := 0; attempt < 3; attempt++ {
		entry, err := port.Get(ctx, l.key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			l.lost = true
			return nil
		}
		if err != nil {
			return err
		}
		// A replica may return the pre-initialization value (epoch zero), or
		// an earlier owner. Neither can prove a successor if its revision
		// predates this lease's last acknowledged update.
		if entry.Revision < l.revision {
			continue
		}
		var current Value
		if json.Unmarshal(entry.Value, &current) != nil || current.Worker != l.value.Worker || current.Epoch != l.value.Epoch {
			l.lost = true
			return ErrLost
		}
		if err := port.Delete(ctx, l.key, entry.Revision); err != nil {
			if errors.Is(err, jetstream.ErrKeyRevisionMismatch) && attempt < 2 {
				continue
			}
			return err
		}
		l.lost = true
		return nil
	}
	return jetstream.ErrKeyRevisionMismatch
}
