package lease

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type heldObservationPort struct {
	entry               KVEntry
	getErr, errorDelete error
	free                bool
	calls               []string
}

func (p *heldObservationPort) Create(context.Context, string, []byte) (uint64, error) {
	p.calls = append(p.calls, "create")
	if p.free {
		return 100, nil
	}
	return 0, jetstream.ErrKeyExists
}
func (p *heldObservationPort) Get(context.Context, string) (KVEntry, error) {
	p.calls = append(p.calls, "get")
	return p.entry, p.getErr
}
func (p *heldObservationPort) Update(context.Context, string, []byte, uint64) (uint64, error) {
	p.calls = append(p.calls, "update")
	return 101, nil
}
func (p *heldObservationPort) Delete(context.Context, string, uint64) error {
	p.calls = append(p.calls, "delete")
	return p.errorDelete
}
func (p *heldObservationPort) Now() time.Time { return time.Unix(35, 0) }

func TestHeldObservationPreservesAcquisitionAndRequestCounts(t *testing.T) {
	for _, tc := range []struct {
		name, value, reason string
		getErr, deleteErr   error
		free                bool
		observed, valid     bool
	}{
		{name: "initialized", value: `{"worker":"prior","epoch":12}`, reason: "held_entry", observed: true, valid: true},
		{name: "malformed", value: `{"worker":"prior","epoch":"bad"}`, reason: "held_entry", observed: true},
		{name: "missing then create conflict", getErr: jetstream.ErrKeyNotFound, reason: "create_race_after_missing_entry"},
		{name: "stale orphan reclaim conflict", value: `{"worker":"prior","epoch":0}`, deleteErr: jetstream.ErrKeyRevisionMismatch, reason: "reclaim_revision_conflict", observed: true, valid: true},
		{name: "unavailable", getErr: context.DeadlineExceeded},
		{name: "success", free: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			makePort := func() *heldObservationPort {
				return &heldObservationPort{entry: KVEntry{Value: []byte(tc.value), Revision: 31, Created: time.Unix(20, 0)}, getErr: tc.getErr, errorDelete: tc.deleteErr, free: tc.free}
			}
			baseline := makePort()
			_, baselineErr := NewWithKVPort(baseline).Acquire(context.Background(), "test", "one", "next")
			observed := makePort()
			var observations []HeldObservation
			_, err := NewWithKVPort(observed).AcquireWithHeldObserver(context.Background(), "test", "one", "next", func(o HeldObservation) { observations = append(observations, o) })
			if err != baselineErr || !reflect.DeepEqual(observed.calls, baseline.calls) {
				t.Fatalf("observer changed result/requests: %v/%v %v/%v", err, baselineErr, observed.calls, baseline.calls)
			}
			if tc.reason == "" {
				if len(observations) != 0 {
					t.Fatalf("nonheld observation: %v", observations)
				}
				return
			}
			if err != ErrHeld || !errors.Is(err, ErrHeld) || len(observations) != 1 {
				t.Fatalf("held identity/observations: %v %v", err, observations)
			}
			o := observations[0]
			if o.Key != "test.one" || o.Reason != tc.reason || o.EntryObserved != tc.observed || o.ValueValid != tc.valid || !o.ObservedAt.Equal(observed.Now()) {
				t.Fatalf("incorrect observation: %+v", o)
			}
			if tc.observed {
				if o.Revision != 31 || !o.Created.Equal(observed.entry.Created) {
					t.Fatalf("wrong entry identity: %+v", o)
				}
			} else if o.Revision != 0 || !o.Created.IsZero() {
				t.Fatalf("invented unavailable entry: %+v", o)
			}
			if tc.valid && o.Worker != "prior" {
				t.Fatalf("wrong owner: %+v", o)
			}
			if !tc.valid && (o.Worker != "" || o.Epoch != 0) {
				t.Fatalf("attributed malformed/missing value: %+v", o)
			}
		})
	}
}
