//go:build linux

package testcluster

import "testing"

func TestRestoredDockerIdentityPreservesRaftNamesAndFreshDockerNamespace(t *testing.T) {
	c := &DockerCluster{network: "fresh-network", names: []string{"fresh-n0", "fresh-n1", "fresh-n2"}, restoredIdentity: "original-cluster"}
	for node := 0; node < 3; node++ {
		if c.NodeName(node) == c.names[node] {
			t.Fatal("restored Raft identity followed new container name")
		}
	}
	if c.NodeName(0) != "original-cluster-n0" || c.serverClusterName() != "original-cluster" || c.network != "fresh-network" {
		t.Fatal("restored identity mismatch")
	}
	c.restoredIdentity = ""
	if c.NodeName(0) != "fresh-n0" || c.serverClusterName() != "fresh-network" {
		t.Fatal("default fixture changed")
	}
	for _, identity := range []string{"", "/bad", "-bad", "two words", "line\nbreak"} {
		if validateRestoredDockerIdentity(identity) == nil {
			t.Fatalf("accepted invalid identity %q", identity)
		}
	}
	if err := validateRestoredDockerIdentity("js-wf-route-221956-1791160248227439562"); err != nil {
		t.Fatal(err)
	}
	if _, err := StartDockerClusterWithRestoredIdentity(t.TempDir(), 3, map[int]string{}, "original-cluster"); err == nil {
		t.Fatal("accepted incomplete restored stores")
	}
}
