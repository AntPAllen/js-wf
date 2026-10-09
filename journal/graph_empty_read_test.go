package journal_test

import (
	"context"
	"testing"

	"js-wf/journal"
	"js-wf/sim"
)

func TestGraphEmptyHistoryInspectionDoesNotPublishReader(t *testing.T) {
	ctx := context.Background()
	scheduler := sim.NewScheduler(29)
	model := sim.NewGraphPublicationTransport(scheduler)
	graph, err := journal.NewGraphStore(journal.GraphConfig{Protocol: model.Protocol(), CanonicalStarts: true})
	if err != nil {
		t.Fatal(err)
	}
	pending, err := graph.ReserveStart(ctx, journal.GraphStartRequest{Type: "test", ID: "empty"}, []byte(`7`))
	if err != nil {
		t.Fatal(err)
	}
	if err = graph.BindStart(ctx, "test", "empty", pending.Start.Token, 5); err != nil {
		t.Fatal(err)
	}
	before := len(scheduler.Trace().Transport)
	records, tail, err := graph.ReadExisting(ctx, "test", "empty", 5)
	if err != nil || len(records) != 0 || tail != 0 {
		t.Fatalf("empty records=%v tail=%d err=%v", records, tail, err)
	}
	for _, event := range scheduler.Trace().Transport[before:] {
		if event.Operation == "graph_publication_cas_root" || event.Operation == "graph_publication_cas_blob" || event.Operation == "graph_publication_get" {
			t.Fatalf("empty record inspection performed %s", event.Operation)
		}
	}
	// Empty journal history still owns a Start input. A public payload view must
	// retain the graph normally; the record-only shortcut cannot escape to it.
	before = len(scheduler.Trace().Transport)
	view, err := graph.OpenExisting(ctx, "test", "empty", 5)
	if err != nil || view == nil {
		t.Fatal(view, err)
	}
	pinned := false
	for _, event := range scheduler.Trace().Transport[before:] {
		if event.Operation == "graph_publication_cas_root" {
			pinned = true
		}
	}
	if !pinned {
		t.Fatal("public empty view did not acquire a reader")
	}
	if err = view.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
