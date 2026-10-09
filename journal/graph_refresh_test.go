package journal_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"js-wf/internal/graphpublication"
	"js-wf/journal"
	"js-wf/sim"
)

func TestGraphViewRefreshFencesAndOneCAS(t *testing.T) {
	for _, mode := range []string{"append", "unchanged", "lost-ack", "expired", "expiry-during-observe", "different-id", "retired", "replaced-generation", "unknown-read", "conflict", "unknown-ack"} {
		t.Run(mode, func(t *testing.T) {
			store, model, now := graphModel(t, journal.JSON)
			ctx := context.Background()
			tail, err := store.Begin(ctx, "flow", "refresh", 7)
			if err != nil {
				t.Fatal(err)
			}
			tail, err = store.Append(ctx, "flow", "refresh", 7, journal.Entry{Kind: journal.Started, Epoch: 3}, tail, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			view, err := store.Open(ctx, "flow", "refresh", 7)
			if err != nil {
				t.Fatal(err)
			}
			old := *view
			keys, err := model.RootKeys(ctx)
			if err != nil || len(keys) != 1 {
				t.Fatal(keys, err)
			}
			typ, id := "flow", "refresh"
			switch mode {
			case "append":
				tail, err = store.Append(ctx, typ, id, 7, journal.Entry{Index: 1, Epoch: 3, Kind: journal.StepRequested, Payload: []byte(`{"kind":"run","name":"next"}`)}, tail, nil, nil)
			case "lost-ack":
				err = model.QueueFault("cas_root", sim.LoseAckAfterCommit)
			case "expired":
				*now = now.Add(time.Minute)
			case "expiry-during-observe":
				// Install the boundary after the independent authority census.
			case "different-id":
				id = "foreign"
			case "retired", "replaced-generation":
				tail, err = store.Append(ctx, typ, id, 7, journal.Entry{Index: 1, Epoch: 3, Kind: journal.Completed, Payload: []byte(`{"result":42}`)}, tail, nil, nil)
				if err == nil {
					err = store.Retire(ctx, typ, id, 7, tail)
				}
				if err == nil && mode == "replaced-generation" {
					_, err = store.Begin(ctx, typ, id, 8)
				}
			case "unknown-read":
				// Queue this fault after capturing the unchanged authority below.
			case "conflict":
				model.PauseBefore("cas_root", func() error { *now = now.Add(time.Second); return old.Renew(ctx) })
			case "unknown-ack":
				err = model.QueueFault("cas_root", sim.LoseAckAfterCommit)
				model.PauseBefore("cas_root", func() error { return model.QueueFault("read_root", sim.DropBeforeCommit) })
			}
			if err != nil {
				t.Fatal(err)
			}
			before, err := model.ReadRoot(ctx, keys[0])
			if err != nil {
				t.Fatal(err)
			}
			if mode == "unknown-read" {
				if err = model.QueueFault("read_root", sim.DropBeforeCommit); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "expiry-during-observe" {
				model.PauseBefore("read_root", func() error { *now = now.Add(time.Minute); return nil })
			}
			err = view.Refresh(ctx, typ, id)
			if mode == "expired" || mode == "expiry-during-observe" || mode == "different-id" || mode == "retired" || mode == "replaced-generation" || mode == "unknown-read" || mode == "unknown-ack" {
				if err == nil || view.Count() != old.Count() || view.Tail() != old.Tail() {
					t.Fatal("invalid refresh admitted", mode, err)
				}
				if mode == "unknown-ack" {
					if _, e := old.Read(ctx, 0); !errors.Is(e, graphpublication.ErrRevoked) {
						t.Fatal("unknown committed replacement retained old handle", e)
					}
				}
				return
			}
			if err != nil || view.Tail() != tail {
				t.Fatal(mode, view.Tail(), tail, err)
			}
			after, err := model.ReadRoot(ctx, keys[0])
			advance := uint64(1)
			if mode == "conflict" {
				advance = 2
			}
			if err != nil || after.Head != before.Head+advance || len(after.Readers) != 1 || before.Readers[0].ID == after.Readers[0].ID {
				t.Fatal("replacement was not one fenced CAS", before, after, err)
			}
			if _, e := old.Read(ctx, 0); !errors.Is(e, graphpublication.ErrRevoked) {
				t.Fatal("old handle survived refresh", e)
			}
			if _, err = view.Read(ctx, view.Count()-1); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before.Graph, after.Graph) || !reflect.DeepEqual(before.Application, after.Application) {
				t.Fatal("refresh changed publication")
			}
			if err = view.Close(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}
