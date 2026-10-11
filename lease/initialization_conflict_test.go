package lease

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Model a successor replacing the created key before the original acquirer
// initializes its epoch. A CAS conflict must preserve that successor exactly.
type initializationSuccessorPort struct {
	KVPort
	now                             time.Time
	value                           []byte
	revision                        uint64
	creates, updates, gets, deletes int
}

func (p *initializationSuccessorPort) Now() time.Time { return p.now }
func (p *initializationSuccessorPort) Create(_ context.Context, _ string, value []byte) (uint64, error) {
	p.creates++
	if p.value != nil {
		return 0, jetstream.ErrKeyExists
	}
	p.revision = 7
	p.value = append([]byte(nil), value...)
	return p.revision, nil
}
func (p *initializationSuccessorPort) Update(_ context.Context, _ string, value []byte, expected uint64) (uint64, error) {
	p.updates++
	// The original Create revision is7, while a successor owns revision11.
	if expected != 7 {
		panic("initialization ignored acknowledged create revision")
	}
	var requested Value
	if json.Unmarshal(value, &requested) != nil || requested.Epoch != 7 {
		panic("incorrect initialization epoch")
	}
	p.revision = 11
	p.value = []byte(`{"worker":"successor","epoch":10}`)
	return 0, jetstream.ErrKeyRevisionMismatch
}
func (p *initializationSuccessorPort) Get(context.Context, string) (KVEntry, error) {
	p.gets++
	return KVEntry{Value: append([]byte(nil), p.value...), Revision: p.revision, Created: p.now}, nil
}
func (p *initializationSuccessorPort) Delete(context.Context, string, uint64) error {
	p.deletes++
	return errors.New("successor must not be deleted")
}

func TestInitializationConflictPreservesSuccessor(t *testing.T) {
	p := &initializationSuccessorPort{now: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)}
	s := NewWithKVPort(p)
	owner, err := s.Acquire(context.Background(), "test", "one", "original")
	if owner != nil || !errors.Is(err, ErrLost) || !errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
		t.Fatalf("conflicted initialization produced owner: owner=%v err=%v", owner, err)
	}
	owner, err = s.Acquire(context.Background(), "test", "one", "original")
	if owner != nil || !errors.Is(err, ErrHeld) {
		t.Fatalf("successor bypassed: %v %v", owner, err)
	}
	if p.creates != 2 || p.updates != 1 || p.gets != 1 || p.deletes != 0 || p.revision != 11 || string(p.value) != `{"worker":"successor","epoch":10}` {
		t.Fatalf("successor changed or initialization retried: %+v", p)
	}
}
