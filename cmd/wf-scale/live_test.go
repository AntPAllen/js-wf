package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/integrity"
	"js-wf/provision"
	"js-wf/testcluster"
)

func TestLiveWorkflowsRetainCohortsAndSpillInputs(t *testing.T) {
	root := t.TempDir()
	cluster, err := testcluster.Start(filepath.Join(root, "cluster"), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	var all [3]jetstream.JetStream
	for i, nc := range cluster.Clients {
		all[i], err = jetstream.New(nc)
		if err != nil {
			t.Fatal(err)
		}
	}
	// Metadata leadership may still be settling when the fixture connects.
	for {
		attempt, stop := context.WithTimeout(ctx, 3*time.Second)
		err = provision.Ensure(attempt, all[0], 3)
		stop()
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	previous, priorLive, priorEntries := 0, 0, 0
	for _, c := range []struct{ background, count, size, spill int }{{20, 6, 64, 0}, {40, 3, 1100000, 3}} {
		if err := publishRange(ctx, all, previous, c.background, 4); err != nil {
			t.Fatal(err)
		}
		s := sample{Subjects: c.background, LiveInvocations: priorLive, LiveEntries: priorEntries}
		if err := inspect(ctx, all, &s); err != nil {
			t.Fatal(err)
		}
		phase, err := runLiveWorkflows(ctx, all, root, c.background, priorLive, priorEntries, liveConfig{c.count, c.size})
		if err != nil {
			t.Fatal(err)
		}
		want := integrity.Report{Invocations: c.count, Journals: c.count, Entries: c.count * 4, Terminal: c.count}
		if phase.Audit != want || phase.SpilledInputs != c.spill || phase.Effects < int64(c.count) {
			t.Fatalf("phase=%+v", phase)
		}
		raw, err := os.ReadFile(filepath.Join(root, "live-"+strconv.Itoa(c.background)+"-audit.json"))
		if err != nil {
			t.Fatal(err)
		}
		var evidence liveEvidence
		if err := json.Unmarshal(raw, &evidence); err != nil {
			t.Fatal(err)
		}
		audit, err := integrity.CheckSnapshot(evidence.Snapshot)
		if err != nil || audit != want || len(evidence.Expected) != c.count {
			t.Fatalf("saved evidence: audit=%+v err=%v", audit, err)
		}
		previous, priorLive, priorEntries = c.background, priorLive+c.count, priorEntries+c.count*4
	}
	// Deliberately wrong prior totals must not produce a green capacity sample.
	if err := inspect(ctx, all, &sample{Subjects: 40}); err == nil {
		t.Fatal("accepted totals excluding retained live cohorts")
	}
}
