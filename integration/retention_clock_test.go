package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/retention"

	"github.com/nats-io/nats.go/jetstream"
)

// A retained tombstone is authoritative until the sweeper removes it. The
// client must not reinterpret it using its own wall clock after expiry.
func TestAwaitRetainedExpiredTombstone(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	state, err := all[0].KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	const typ, id = "test", "expired-tombstone"
	now := time.Now()
	marker, err := json.Marshal(retention.Tombstone{
		Tombstone: true,
		InvSeq:    19,
		PurgedAt:  now.Add(-2 * time.Hour),
		ExpiresAt: now.Add(-time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.Put(ctx, identity.Key(typ, id), marker); err != nil {
		t.Fatal(err)
	}
	peerState, err := all[1].KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	for until := time.Now().Add(3 * time.Second); ; {
		entry, getErr := peerState.Get(ctx, identity.Key(typ, id))
		if getErr == nil && bytes.Equal(entry.Value(), marker) {
			break
		}
		if time.Now().After(until) {
			t.Fatalf("retained tombstone did not reach peer: %v", getErr)
		}
		time.Sleep(25 * time.Millisecond)
	}
	c := client.New(all[1])
	if _, err := c.Await(ctx, typ, id); !errors.Is(err, client.ErrPurged) {
		t.Fatalf("retained expired tombstone returned %v, want ErrPurged", err)
	}
	if err := state.Delete(ctx, identity.Key(typ, id)); err != nil {
		t.Fatal(err)
	}
	for until := time.Now().Add(3 * time.Second); ; {
		_, getErr := peerState.Get(ctx, identity.Key(typ, id))
		if errors.Is(getErr, jetstream.ErrKeyNotFound) {
			break
		}
		if time.Now().After(until) {
			t.Fatalf("deleted tombstone still visible to peer: %v", getErr)
		}
		time.Sleep(25 * time.Millisecond)
	}
	if _, err := c.Await(ctx, typ, id); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("deleted tombstone returned %v, want ErrNotFound", err)
	}
}
