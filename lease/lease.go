package lease

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

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

type Store struct{ kv jetstream.KeyValue }

func New(ctx context.Context, js jetstream.JetStream) (*Store, error) {
	kv, err := js.KeyValue(ctx, "WF_LEASE")
	if err != nil {
		return nil, err
	}
	return &Store{kv: kv}, nil
}

type Lease struct {
	store    *Store
	key      string
	value    Value
	mu       sync.Mutex
	revision uint64
	lost     bool
}

func (l *Lease) Epoch() uint64 { return l.value.Epoch }

// Acquire uses the KV create CAS. The creation revision is the fencing epoch:
// it stays unique even when an earlier lease expires without a journal write.
func (s *Store) Acquire(ctx context.Context, typ, id, worker string) (*Lease, error) {
	if err := identity.Validate(typ, id); err != nil {
		return nil, err
	}
	if worker == "" {
		return nil, fmt.Errorf("empty worker ID")
	}
	key := identity.Key(typ, id)
	// The revision itself becomes the epoch, so the value is updated once by
	// its owner after Create. Until that update, other acquirers still fail.
	data, _ := json.Marshal(Value{Worker: worker})
	rev, err := s.kv.Create(ctx, key, data)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyExists) {
			return nil, ErrHeld
		}
		return nil, err
	}
	v := Value{Worker: worker, Epoch: rev}
	data, _ = json.Marshal(v)
	newRev, err := s.kv.Update(ctx, key, data, rev)
	if err != nil {
		return nil, fmt.Errorf("%w: initialization: %v", ErrLost, err)
	}
	return &Lease{store: s, key: key, value: v, revision: newRev}, nil
}

func (l *Lease) Renew(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.lost {
		return ErrLost
	}
	data, _ := json.Marshal(l.value)
	rev, err := l.store.kv.Update(ctx, l.key, data, l.revision)
	if err != nil {
		l.lost = true
		return fmt.Errorf("%w: %v", ErrLost, err)
	}
	l.revision = rev
	return nil
}

func (l *Lease) Release(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.lost {
		return ErrLost
	}
	err := l.store.kv.Delete(ctx, l.key, jetstream.LastRevision(l.revision))
	if err != nil {
		l.lost = true
		return fmt.Errorf("%w: %v", ErrLost, err)
	}
	l.lost = true
	return nil
}
