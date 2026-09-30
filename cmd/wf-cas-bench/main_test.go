package main

import "testing"

func TestCompareBaseline(t *testing.T) {
	base := report{NATSServerVersion: "2.15.0", GoVersion: "go1.27.1", Replicas: 3, Storage: "file", ParallelWorkers: 1000, Hot: result{Invocations: 1, EntriesEach: 10000, AppendsSec: 3000}, Parallel: result{Invocations: 1000, EntriesEach: 100, AppendsSec: 14000}}
	current := base
	current.HotPlacement = "pinned" // Legacy reports omitted the default placement.
	current.Hot.AppendsSec = 2400
	current.Parallel.AppendsSec = 11200
	if err := compareBaseline(current, base, 0.8); err != nil {
		t.Fatalf("boundary should pass: %v", err)
	}
	current.Hot.AppendsSec = 2399
	if err := compareBaseline(current, base, 0.8); err == nil {
		t.Fatal("hot throughput below boundary passed")
	}
	current = base
	current.Parallel.AppendsSec = 11199
	if err := compareBaseline(current, base, 0.8); err == nil {
		t.Fatal("parallel throughput below boundary passed")
	}
	current = base
	current.ParallelWorkers = 96
	if err := compareBaseline(current, base, 0.8); err == nil {
		t.Fatal("mismatched workload passed")
	}
}

func TestTopologyProbeAcrossPinnedNodes(t *testing.T) {
	rep, err := runTopologyProbe(t.TempDir(), 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Samples) != 9 || rep.Messages != 180 || rep.Subjects != 9 {
		t.Fatalf("wrong probe coverage: %+v", rep)
	}
	seen := map[int]int{}
	for _, sample := range rep.Samples {
		seen[sample.Node]++
		if sample.Measurement.Appends != 20 || sample.Measurement.AppendsSec <= 0 {
			t.Fatalf("invalid sample: %+v", sample)
		}
		for _, snap := range []topology{sample.Before, sample.After} {
			if snap.ClientServer == "" || snap.Leader == "" || snap.ClientIsLeader != (snap.ClientServer == snap.Leader) {
				t.Fatalf("invalid topology: %+v", snap)
			}
		}
	}
	for node := 0; node < 3; node++ {
		if seen[node] != 3 {
			t.Fatalf("node %d samples=%d", node, seen[node])
		}
	}
}

func TestBenchmarkRetainsTopologyOutsideTimedSamples(t *testing.T) {
	rep, err := run(t.TempDir(), 20, 3, 4, 3)
	if err != nil {
		t.Fatal(err)
	}
	if rep.JournalMessages != 32 || rep.JournalSubjects != 4 || rep.Hot.Appends != 20 || rep.Parallel.Appends != 12 {
		t.Fatalf("benchmark counts changed: %+v", rep)
	}
	for _, snap := range []topology{rep.HotBefore, rep.HotAfter} {
		if snap.ClientServer != "wf-test-0" || snap.Leader == "" || snap.ClientIsLeader != (snap.ClientServer == snap.Leader) {
			t.Fatalf("wrong hot client topology: %+v", snap)
		}
	}
}

func TestControlledHotPlacement(t *testing.T) {
	for _, placement := range []string{"leader", "follower"} {
		t.Run(placement, func(t *testing.T) {
			rep, err := runWithPlacement(t.TempDir(), 20, 3, 4, 3, placement)
			if err != nil {
				t.Fatal(err)
			}
			if rep.HotPlacement != placement || rep.HotBefore != rep.HotAfter || rep.HotBefore.ClientIsLeader != (placement == "leader") {
				t.Fatalf("incorrect controlled placement: %+v", rep)
			}
			if rep.JournalMessages != 32 || rep.JournalSubjects != 4 {
				t.Fatalf("counts changed: %+v", rep)
			}
		})
	}
}
