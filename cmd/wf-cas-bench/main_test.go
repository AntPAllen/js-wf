package main

import "testing"

func TestCompareBaseline(t *testing.T) {
	base := report{NATSServerVersion: "2.15.0", GoVersion: "go1.27.1", Replicas: 3, Storage: "file", ParallelWorkers: 1000, Hot: result{Invocations: 1, EntriesEach: 10000, AppendsSec: 3000}, Parallel: result{Invocations: 1000, EntriesEach: 100, AppendsSec: 14000}}
	current := base
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
