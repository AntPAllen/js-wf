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

func TestTombstoneScanPagesAndProtectsNewerState(t *testing.T) {
	cluster, err := testcluster.Start(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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
	putTombstone("gone", 1, now.Add(-time.Minute))
	holdSeq := putInvocation("hold")
	putTombstone("hold", holdSeq, now.Add(-time.Minute))
	oldSeq := putInvocation("reuse")
	if err := inv.Purge(ctx, jetstream.WithPurgeSubject(identity.InvocationSubject("test", "reuse"))); err != nil {
		t.Fatal(err)
	}
	if nextSeq := putInvocation("reuse"); nextSeq <= oldSeq {
		t.Fatalf("reused sequence %d <= %d", nextSeq, oldSeq)
	}
	putTombstone("reuse", oldSeq, now.Add(-time.Minute))
	putTombstone("live", 1, now.Add(time.Minute))
	if _, err := state.Put(ctx, "test.result", []byte(`{"inv_seq":99,"result":true}`)); err != nil {
		t.Fatal(err)
	}
	scan := NewTombstoneScan(js)
	pageCursor := uint64(1)
	var dryEligible int
	for pageNumber := 0; pageNumber < 10; pageNumber++ {
		page, err := scan.Scan(ctx, pageCursor, 2, now, true)
		if err != nil {
			t.Fatal(err)
		}
		if page.Deleted != 0 {
			t.Fatalf("dry scan deleted a tombstone: %+v", page)
		}
		dryEligible += page.Eligible
		pageCursor = page.NextSequence
		if pageCursor == 1 {
			break
		}
	}
	if pageCursor != 1 || dryEligible != 2 {
		t.Fatalf("dry scan cursor=%d eligible=%d", pageCursor, dryEligible)
	}
	if _, err := state.Put(ctx, "test.gone", []byte(`{"inv_seq":99,"result":true}`)); err != nil {
		t.Fatal(err)
	}
	putTombstone("other", 1, now.Add(-time.Minute))
	pageCursor = 1
	var deleted int
	for pageNumber := 0; pageNumber < 20; pageNumber++ {
		page, err := scan.Scan(ctx, pageCursor, 2, now, false)
		if err != nil {
			t.Fatal(err)
		}
		deleted += page.Deleted
		pageCursor = page.NextSequence
		if pageCursor == 1 {
			break
		}
	}
	if pageCursor != 1 || deleted != 2 {
		t.Fatalf("apply scan cursor=%d deleted=%d", pageCursor, deleted)
	}
	for _, id := range []string{"reuse", "other"} {
		if _, err := state.Get(ctx, identity.Key("test", id)); !errors.Is(err, jetstream.ErrKeyNotFound) {
			t.Fatalf("%s tombstone retained: %v", id, err)
		}
	}
	for _, id := range []string{"gone", "hold", "live", "result"} {
		if _, err := state.Get(ctx, identity.Key("test", id)); err != nil {
			t.Fatalf("%s newer state removed: %v", id, err)
		}
	}
}
