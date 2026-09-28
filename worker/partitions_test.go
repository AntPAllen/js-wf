package worker

import (
	"testing"

	"js-wf/provision"
)

func TestStaticPartitionsCoverAllWithoutOverlap(t *testing.T) {
	seen := map[uint32]int{}
	for worker := 0; worker < 6; worker++ {
		partitions, err := StaticPartitions(worker, 6)
		if err != nil || len(partitions) < 10 {
			t.Fatalf("worker %d partitions=%v err=%v", worker, partitions, err)
		}
		for _, partition := range partitions {
			seen[partition]++
		}
	}
	if len(seen) != int(provision.Partitions) {
		t.Fatalf("covered %d partitions, want %d", len(seen), provision.Partitions)
	}
	for partition := uint32(0); partition < provision.Partitions; partition++ {
		if seen[partition] != 1 {
			t.Fatalf("partition %d assigned %d times", partition, seen[partition])
		}
	}
	for _, assignment := range [][2]int{{0, 0}, {-1, 6}, {6, 6}, {0, 65}} {
		if _, err := StaticPartitions(assignment[0], assignment[1]); err == nil {
			t.Fatalf("accepted invalid assignment %v", assignment)
		}
	}
}
