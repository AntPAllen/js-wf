//go:build linux

package testcluster

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/nats-io/nats-server/v2/server"
)

func TestDockerTagsRejectInvalidTopologyAndCopyInputs(t *testing.T) {
	for _, tags := range []map[int][]string{{-1: {"a"}}, {3: {"a"}}, {0: {""}}, {0: {" a"}}, {0: {"a\nb"}}, {0: {"a", "a"}}} {
		if _, err := copyDockerServerTags(3, tags); err == nil {
			t.Fatalf("invalid tags accepted: %v", tags)
		}
	}
	input := map[int][]string{0: {"clock-a"}, 1: {"clock-b"}}
	copied, err := copyDockerServerTags(3, input)
	if err != nil {
		t.Fatal(err)
	}
	input[0][0] = "mutated"
	delete(input, 1)
	if !reflect.DeepEqual(copied, map[int][]string{0: {"clock-a"}, 1: {"clock-b"}}) {
		t.Fatalf("caller mutated tags: %v", copied)
	}
}

func TestDockerNodeConfigRetainsActualTagsOnRestart(t *testing.T) {
	root := t.TempDir()
	base := []byte("jetstream: { sync_interval: \"2m\" }\n")
	if err := os.WriteFile(filepath.Join(root, "nats.conf"), base, 0644); err != nil {
		t.Fatal(err)
	}
	c := DockerCluster{root: root, serverTags: map[int][]string{0: {"clock-a"}, 1: {"clock-b"}}}
	for _, node := range []int{0, 1, 0} {
		path, err := c.serverConfig(node)
		if err != nil {
			t.Fatal(err)
		}
		opts, err := server.ProcessConfigFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual([]string(opts.Tags), c.serverTags[node]) {
			t.Fatalf("node %d config tags=%v", node, opts.Tags)
		}
	}
	path, err := c.serverConfig(2)
	if err != nil || path != filepath.Join(root, "nats.conf") {
		t.Fatalf("legacy config changed: %s %v", path, err)
	}
	after, err := os.ReadFile(path)
	if err != nil || string(after) != string(base) {
		t.Fatalf("base config modified: %v", err)
	}
}
