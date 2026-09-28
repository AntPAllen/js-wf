package retention

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"js-wf/identity"
	"js-wf/provision"
	"js-wf/testcluster"

	"github.com/nats-io/nats.go/jetstream"
)

func TestSweepTombstonesChecksExpiryGenerationAndPurgeOrder(t *testing.T) {
	cluster, err := testcluster.Start(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := provision.Ensure(ctx, js, 1); err != nil {
		t.Fatal(err)
	}
	state, err := js.KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	inv, err := js.Stream(ctx, "WF_INV")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	putTombstone := func(id string, seq uint64, expires time.Time) {
		t.Helper()
		data, err := json.Marshal(Tombstone{Tombstone: true, InvSeq: seq, PurgedAt: now.Add(-time.Hour), ExpiresAt: expires})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := state.Put(ctx, identity.Key("test", id), data); err != nil {
			t.Fatal(err)
		}
	}
	putInvocation := func(id string) uint64 {
		t.Helper()
		ack, err := js.Publish(ctx, identity.InvocationSubject("test", id), []byte(`null`))
		if err != nil {
			t.Fatal(err)
		}
		return ack.Sequence
	}
	holdSeq := putInvocation("hold")
	putTombstone("hold", holdSeq, now.Add(-time.Minute))
	putTombstone("gone", 1, now.Add(-time.Minute))
	oldSeq := putInvocation("reuse")
	if err := inv.Purge(ctx, jetstream.WithPurgeSubject(identity.InvocationSubject("test", "reuse"))); err != nil {
		t.Fatal(err)
	}
	if newSeq := putInvocation("reuse"); newSeq <= oldSeq {
		t.Fatalf("reused sequence %d <= %d", newSeq, oldSeq)
	}
	putTombstone("reuse", oldSeq, now.Add(-time.Minute))
	putTombstone("live", 1, now.Add(time.Minute))
	if _, err := state.Put(ctx, "test.result", []byte(`{"inv_seq":99,"result":true}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := state.Put(ctx, "scan.start", []byte(`1`)); err != nil {
		t.Fatal(err)
	}
	first, err := SweepTombstones(ctx, js, now)
	if err != nil || first.Expired != 3 || first.Deleted != 2 {
		t.Fatalf("first sweep=%+v err=%v", first, err)
	}
	for _, id := range []string{"gone", "reuse"} {
		if _, err := state.Get(ctx, identity.Key("test", id)); !errors.Is(err, jetstream.ErrKeyNotFound) {
			t.Fatalf("%s tombstone retained: %v", id, err)
		}
	}
	for _, id := range []string{"hold", "live", "result"} {
		if _, err := state.Get(ctx, identity.Key("test", id)); err != nil {
			t.Fatalf("%s deleted: %v", id, err)
		}
	}
	if err := inv.Purge(ctx, jetstream.WithPurgeSubject(identity.InvocationSubject("test", "hold"))); err != nil {
		t.Fatal(err)
	}
	second, err := SweepTombstones(ctx, js, now)
	if err != nil || second.Deleted != 1 {
		t.Fatalf("second sweep=%+v err=%v", second, err)
	}
}
