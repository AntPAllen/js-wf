package journal_test

import (
	"context"
	"js-wf/internal/graphpublication"
	"js-wf/journal"
	"js-wf/sim"
	"strings"
	"testing"
	"time"
)

type scopedReaderModel struct {
	*sim.GraphPublicationTransport
	scope string
}

func (m scopedReaderModel) ReaderMaintenanceScope() string { return m.scope }

func TestGraphReaderMaintenanceFacadeBoundsAndScope(t *testing.T) {
	ctx := context.Background()
	s := sim.NewScheduler(1)
	model := sim.NewGraphPublicationTransport(s)
	p := model.Protocol()
	p.Port = scopedReaderModel{model, strings.Repeat("a", 64)}
	store, err := journal.NewGraphStore(journal.GraphConfig{Protocol: p})
	if err != nil {
		t.Fatal(err)
	}
	before := len(s.Trace().Transport)
	m, err := store.ReaderMaintenance()
	if err != nil || m.ReaderMaintenanceScope() != strings.Repeat("a", 64) || len(s.Trace().Transport) != before {
		t.Fatal(m, err)
	}
	for _, id := range []string{"a", "b"} {
		if _, err = store.Begin(ctx, "flow", id, 1); err != nil {
			t.Fatal(err)
		}
	}
	keys, err := model.RootKeys(ctx)
	if err != nil || len(keys) != 2 {
		t.Fatal(keys, err)
	}
	expires := time.Unix(1000, 0).UTC()
	for _, key := range keys {
		root, e := model.ReadRoot(ctx, key)
		if e != nil {
			t.Fatal(e)
		}
		if _, _, e = p.AcquireReader(ctx, key, root.Head, expires); e != nil {
			t.Fatal(e)
		}
	}
	cursor, err := m.BeginReaderSweep(ctx)
	if err != nil {
		t.Fatal(err)
	}
	first, err := m.ExpireReaderBatch(ctx, cursor, 1, expires)
	if err != nil || first.Inspected != 1 || first.Complete {
		t.Fatal(first, err)
	}
	// Reconstruct the facade and resume its persisted checkpoint.
	m, err = store.ReaderMaintenance()
	if err != nil {
		t.Fatal(err)
	}
	last, err := m.ExpireReaderBatch(ctx, first.Cursor, 1, expires)
	if err != nil || !last.Complete || last.Inspected != 1 {
		t.Fatal(last, err)
	}
	for _, key := range keys {
		root, e := model.ReadRoot(ctx, key)
		if e != nil || len(root.Readers) != 0 || len(root.Application) == 0 {
			t.Fatal(root, e)
		}
	}
	if _, err = m.ExpireReaderBatch(ctx, cursor, graphpublication.MaxReaderSweepBatch+1, expires); err == nil {
		t.Fatal("unbounded facade batch")
	}
	if _, ok := any(m).(graphpublication.Port); ok {
		t.Fatal("facade exposes publication port")
	}
	if _, err = (*journal.GraphStore)(nil).ReaderMaintenance(); err == nil {
		t.Fatal("nil store accepted")
	}
	plain, err := journal.NewGraphStore(journal.GraphConfig{Protocol: model.Protocol()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = plain.ReaderMaintenance(); err == nil {
		t.Fatal("unscoped model accepted")
	}
}
