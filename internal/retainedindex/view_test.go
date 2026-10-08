package retainedindex_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"
	"time"

	"js-wf/internal/graphpublication"
	"js-wf/internal/retainedindex"
	"js-wf/sim"
)

func key(s string) retainedindex.Key { return retainedindex.Key(sha256.Sum256([]byte(s))) }

func TestOwnedIndexSnapshotsForksRetirementAndDrain(t *testing.T) {
	ctx := context.Background()
	schedule := sim.NewScheduler(7)
	model := sim.NewGraphPublicationTransport(schedule)
	p := model.Protocol()
	now := func() time.Time { return time.UnixMilli(schedule.NowMillis()).UTC() }
	root := graphpublication.EmptyRoot()
	var pinned graphpublication.Reader
	var old *retainedindex.View
	for i := 0; i < 64; i++ {
		reader, next, err := p.AcquireReader(ctx, "indexed", root.Head, now().Add(time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		root = next
		view, err := retainedindex.OpenView(ctx, p, reader, "signals", now)
		if err != nil {
			t.Fatal(err)
		}
		if view.Count() != uint64(i) {
			t.Fatal("forest population differs", view.Count(), i)
		}
		packet, _, found, err := view.Update(ctx, key(fmt.Sprint(i)), uint64(i)+1)
		if err != nil || found {
			t.Fatal(found, err)
		}
		root, err = p.ReleaseReader(ctx, reader, root.Head)
		if err != nil {
			t.Fatal(err)
		}
		prepared, err := p.PrepareStreamAppendWithApplication(ctx, "indexed", root.Head, "signals", packet, [][]byte{[]byte(fmt.Sprintf("input:%d", i))}, nil, now().Add(time.Second), []byte("active"))
		if err != nil {
			t.Fatal(err)
		}
		root, err = p.Commit(ctx, prepared)
		if err != nil {
			t.Fatal(err)
		}
		if i == 31 {
			pinned, root, err = p.AcquireReader(ctx, "indexed", root.Head, now().Add(time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			old, err = retainedindex.OpenView(ctx, p, pinned, "signals", now)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	// Forks with the same source population must not merge by object presence.
	reader, root, err := p.AcquireReader(ctx, "indexed", root.Head, now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	live, err := retainedindex.OpenView(ctx, p, reader, "signals", now)
	if err != nil {
		t.Fatal(err)
	}
	a, _, _, err := live.Update(ctx, key("winner"), 100)
	if err != nil {
		t.Fatal(err)
	}
	b, _, _, err := live.Update(ctx, key("loser"), 200)
	if err != nil {
		t.Fatal(err)
	}
	root, err = p.ReleaseReader(ctx, reader, root.Head)
	if err != nil {
		t.Fatal(err)
	}
	first, err := p.PrepareStreamAppendWithApplication(ctx, "indexed", root.Head, "signals", a, nil, nil, now().Add(time.Second), []byte("active"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := p.PrepareStreamAppendWithApplication(ctx, "indexed", root.Head, "signals", b, nil, nil, now().Add(time.Second), []byte("active"))
	if err != nil {
		t.Fatal(err)
	}
	root, err = p.Commit(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = p.Commit(ctx, second); !errors.Is(err, graphpublication.ErrConflict) {
		t.Fatal("fork adopted", err)
	}
	reader, root, err = p.AcquireReader(ctx, "indexed", root.Head, now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	live, err = retainedindex.OpenView(ctx, p, reader, "signals", now)
	if err != nil {
		t.Fatal(err)
	}
	if value, found, e := live.Lookup(ctx, key("winner")); e != nil || !found || value != 100 {
		t.Fatal(value, found, e)
	}
	if _, found, e := live.Lookup(ctx, key("loser")); e != nil || found {
		t.Fatal("unpublished key visible", found, e)
	}
	if err = model.QueueFault("get", sim.DropBeforeCommit); err != nil {
		t.Fatal(err)
	}
	if _, _, e := live.Lookup(ctx, key("winner")); e == nil {
		t.Fatal("uncertain owned read became absence")
	}
	root, err = p.RetireLiveWithApplication(ctx, "indexed", root.Head, []byte("retired"))
	if err != nil {
		t.Fatal(err)
	}
	if err = schedule.AdvanceMillis(2000); err != nil {
		t.Fatal(err)
	}
	if _, err = p.SweepWithReaders(ctx, now()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 64; i++ {
		v, found, e := old.Lookup(ctx, key(fmt.Sprint(i)))
		if e != nil || found != (i < 32) || found && v != uint64(i)+1 {
			t.Fatal("captured population changed", i, v, found, e)
		}
	}
	root, err = model.ReadRoot(ctx, "indexed")
	if err != nil {
		t.Fatal(err)
	}
	root, err = p.ReleaseReader(ctx, pinned, root.Head)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, e := old.Lookup(ctx, key("missing")); !errors.Is(e, graphpublication.ErrRevoked) {
		t.Fatal("released reader proved absence", e)
	}
	root, err = p.ReleaseReader(ctx, reader, root.Head)
	if err != nil {
		t.Fatal(err)
	}
	if err = schedule.AdvanceMillis(60000); err != nil {
		t.Fatal(err)
	}
	if _, err = p.SweepWithReaders(ctx, now()); err != nil {
		t.Fatal(err)
	}
	objects, err := model.Objects(ctx)
	if err != nil || len(objects) != 0 {
		t.Fatal("physical fixture not drained", len(objects), err)
	}
	if err = model.CheckReferences(); err != nil {
		t.Fatal(err)
	}
}

func TestEmptyOwnedIndexRequiresLivePin(t *testing.T) {
	ctx := context.Background()
	schedule := sim.NewScheduler(9)
	model := sim.NewGraphPublicationTransport(schedule)
	p := model.Protocol()
	now := func() time.Time { return time.UnixMilli(schedule.NowMillis()).UTC() }
	if _, err := retainedindex.OpenView(ctx, p, graphpublication.Reader{}, "signals", now); err == nil {
		t.Fatal("copied empty root created authority")
	}
	reader, _, err := p.AcquireReader(ctx, "empty-index", 0, now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	view, err := retainedindex.OpenView(ctx, p, reader, "signals", now)
	if err != nil {
		t.Fatal(err)
	}
	if _, found, e := view.Lookup(ctx, key("missing")); e != nil || found {
		t.Fatal(found, e)
	}
	if err = schedule.AdvanceMillis(1000); err != nil {
		t.Fatal(err)
	}
	if _, _, e := view.Lookup(ctx, key("missing")); !errors.Is(e, graphpublication.ErrRevoked) {
		t.Fatal("expired empty pin proved absence", e)
	}
	if _, _, _, e := view.Update(ctx, key("new"), 1); !errors.Is(e, graphpublication.ErrRevoked) {
		t.Fatal("expired empty pin authorized preparation", e)
	}
}
