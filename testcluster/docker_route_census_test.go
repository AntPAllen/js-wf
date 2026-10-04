//go:build linux

package testcluster

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestDockerRouteSeedsKeepDefaultsAndExplicitPeersAcrossRestarts(t *testing.T) {
	for count := 3; count <= 5; count++ {
		names := make([]string, count)
		for i := range names {
			names[i] = fmt.Sprintf("route-only-%d", i)
		}
		c := DockerCluster{routeNames: names}
		for node := range names {
			peer := 0
			if node == 0 {
				peer = 1
			}
			if got := c.routeSeeds(node); got != "nats://"+names[peer]+":6222" {
				t.Fatalf("default node%d=%s", node, got)
			}
		}
		c.explicitRouteSeeds = true
		for round := 0; round < 2; round++ {
			for node := range names {
				got := strings.Split(c.routeSeeds(node), ",")
				if len(got) != count-1 {
					t.Fatal(got)
				}
				seen := map[string]bool{}
				for _, route := range got {
					if seen[route] || route == "nats://"+names[node]+":6222" {
						t.Fatal(got)
					}
					seen[route] = true
				}
				for peer, name := range names {
					if peer != node && !seen["nats://"+name+":6222"] {
						t.Fatal(got)
					}
				}
			}
		}
	}
}
func TestDockerRouteCensusDistinguishesPoolsFromPeers(t *testing.T) {
	members := []string{"n0", "n1", "n2", "n3", "n4"}
	for _, full := range []bool{false, true} {
		routes := []map[string]string{}
		for i := 1; i <= 4; i++ {
			peer := i
			if !full {
				peer = 1
			}
			routes = append(routes, map[string]string{"remote_name": members[peer], "remote_id": fmt.Sprintf("id%d", peer)})
		}
		data, _ := json.Marshal(map[string]any{"server_name": "n0", "server_id": "id0", "num_routes": 4, "routes": routes})
		census, err := decodeRouteCensus(data, "n0", members)
		if err != nil || census.FullMesh != full || census.Connections != 4 {
			t.Fatalf("census=%+v err=%v", census, err)
		}
		if !full && (len(census.Peers) != 1 || len(census.Missing) != 3) {
			t.Fatal(census)
		}
	}
}
func TestDockerRouteCensusRejectsWrongIdentityAndCounts(t *testing.T) {
	for _, data := range []string{
		`{"server_name":"wrong","server_id":"id0","num_routes":0}`,
		`{"server_name":"n0","server_id":"id0","num_routes":4,"routes":[]}`,
		`{"server_name":"n0","server_id":"id0","num_routes":1,"routes":[{"remote_name":"foreign","remote_id":"id1"}]}`,
		`{"server_name":"n0","server_id":"id0","num_routes":2,"routes":[{"remote_name":"n1","remote_id":"id1"},{"remote_name":"n1","remote_id":"different"}]}`,
	} {
		if _, err := decodeRouteCensus([]byte(data), "n0", []string{"n0", "n1"}); err == nil {
			t.Fatalf("accepted %s", data)
		}
	}
}
