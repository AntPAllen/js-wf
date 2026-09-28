package testcluster

import (
	"context"
	"testing"
	"time"
)

func waitRoutes(t *testing.T, c *Cluster, want [3]int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		got := [3]int{c.Servers[0].NumRoutes(), c.Servers[1].NumRoutes(), c.Servers[2].NumRoutes()}
		if got == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("routes=%d,%d,%d want=%d,%d,%d", c.Servers[0].NumRoutes(), c.Servers[1].NumRoutes(), c.Servers[2].NumRoutes(), want[0], want[1], want[2])
}

func TestRouteMeshPartitionsAndHeals(t *testing.T) {
	c, err := StartPartitionable(t.TempDir(), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	waitRoutes(t, c, [3]int{2, 2, 2})
	if err := c.RouteMesh().PartitionNode(2); err != nil {
		t.Fatal(err)
	}
	waitRoutes(t, c, [3]int{1, 1, 0})
	c.RouteMesh().Heal()
	waitRoutes(t, c, [3]int{2, 2, 2})
}

func TestRouteMeshCutsOnlySelectedPair(t *testing.T) {
	c, err := StartPartitionable(t.TempDir(), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	waitRoutes(t, c, [3]int{2, 2, 2})
	schedule := FaultSchedule{Seed: 42, Events: []FaultEvent{{Op: PartitionNodes, A: 0, B: 1}}}
	if err := schedule.Run(context.Background(), c.ApplyFault); err != nil {
		t.Fatal(err)
	}
	waitRoutes(t, c, [3]int{1, 1, 2})
	c.RouteMesh().Heal()
	waitRoutes(t, c, [3]int{2, 2, 2})
}
