package integration_test

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/wf"
)

// Run with WF_LONG_TEST=1. This is a scale proof, kept out of the fast suite.
func TestFiveThousandEntriesWithRepeatedSnapshots(t *testing.T) {
	if os.Getenv("WF_LONG_TEST") == "" {
		t.Skip("set WF_LONG_TEST=1 for the 5,000-entry compaction proof")
	}
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	const typ = "test"
	const compactedID = "long-compact"
	const controlID = "long-control"
	for _, id := range []string{compactedID, controlID} {
		if _, err := client.New(all[0]).Start(ctx, typ, id, []byte(`null`)); err != nil {
			t.Fatal(err)
		}
	}
	store := journal.New(all[0])
	var compactTail, controlTail uint64
	expected := make([]journal.Entry, 0, 5000)
	terminal, _ := json.Marshal(wf.Outcome{Result: []byte(`1`)})
	start := time.Now()
	for i := uint64(0); i < 5000; i++ {
		kind := journal.StepCompleted
		switch {
		case i == 0:
			kind = journal.Started
		case i == 4999:
			kind = journal.Completed
		case i%2 == 1:
			kind = journal.StepRequested
		}
		e := journal.Entry{Epoch: 1, Index: i, Kind: kind}
		if kind == journal.Completed {
			e.Payload = terminal
		}
		var err error
		compactTail, err = store.Append(ctx, typ, compactedID, e, compactTail)
		if err != nil {
			t.Fatalf("compact append %d: %v", i, err)
		}
		controlTail, err = store.Append(ctx, typ, controlID, e, controlTail)
		if err != nil {
			t.Fatalf("control append %d: %v", i, err)
		}
		expected = append(expected, e)
		if (i+1)%256 != 0 {
			continue
		}
		if _, err := store.SnapshotPrefix(ctx, typ, compactedID, 16); err != nil {
			state, stateErr := all[0].KeyValue(ctx, "WF_STATE")
			if stateErr == nil {
				if manifest, getErr := state.Get(ctx, "snap."+identity.Key(typ, compactedID)); getErr == nil {
					t.Logf("manifest at failure: %s", manifest.Value())
					var snap journal.Snapshot
					if json.Unmarshal(manifest.Value(), &snap) == nil {
						objects, objErr := all[0].ObjectStore(ctx, "WF_BLOB")
						if objErr == nil {
							_, objErr = objects.GetInfo(ctx, snap.Object)
							t.Logf("object metadata from node 0: %v", objErr)
						}
						objects, objErr = all[1].ObjectStore(ctx, "WF_BLOB")
						if objErr == nil {
							_, objErr = objects.GetInfo(ctx, snap.Object)
							t.Logf("object metadata from node 1: %v", objErr)
						}
					}
				}
			}
			t.Fatalf("snapshot after %d: %v", i+1, err)
		}
		records, _, err := store.Read(ctx, typ, compactedID)
		if err != nil || len(records) != len(expected) {
			t.Fatalf("read after %d: entries=%d err=%v", i+1, len(records), err)
		}
		for j := range records {
			if !reflect.DeepEqual(records[j].Entry, expected[j]) {
				t.Fatalf("snapshot after %d changed entry %d", i+1, j)
			}
		}
	}
	compacted, tail, err := store.Read(ctx, typ, compactedID)
	if err != nil || tail != compactTail || len(compacted) != 5000 {
		t.Fatalf("compacted final: entries=%d tail=%d err=%v", len(compacted), tail, err)
	}
	control, tail, err := store.Read(ctx, typ, controlID)
	if err != nil || tail != controlTail || len(control) != 5000 {
		t.Fatalf("control final: entries=%d tail=%d err=%v", len(control), tail, err)
	}
	for i := range compacted {
		if !reflect.DeepEqual(compacted[i].Entry, control[i].Entry) {
			t.Fatalf("final entry %d differs from control", i)
		}
	}
	state, err := all[0].KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{compactedID, controlID} {
		if _, err := state.Put(ctx, identity.Key(typ, id), terminal); err != nil {
			t.Fatal(err)
		}
	}
	report, err := integrity.Check(ctx, all[0])
	if err != nil || report.Entries != 10000 || report.Terminal != 2 {
		t.Fatalf("integrity: %+v err=%v", report, err)
	}
	t.Logf("10,000 total CAS appends, 19 snapshots, verified in %s", time.Since(start))
}
