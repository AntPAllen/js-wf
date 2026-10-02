//go:build linux

package testcluster

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDockerStorePathsRejectSharedOrUnintendedBinds(t *testing.T) {
	root := t.TempDir()
	private := t.TempDir()
	file := filepath.Join(private, "file")
	if err := os.WriteFile(file, nil, 0600); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(private, alias); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		stores map[int]string
	}{
		{"relative", map[int]string{4: "relative"}},
		{"missing", map[int]string{4: filepath.Join(private, "missing")}},
		{"file", map[int]string{4: file}},
		{"out_of_range", map[int]string{5: private}},
		{"negative_node", map[int]string{-1: private}},
		{"shared", map[int]string{3: private, 4: private}},
		{"symlink_alias", map[int]string{3: private, 4: alias}},
		{"ancestor", map[int]string{4: root}},
		{"filesystem_root", map[int]string{4: "/"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := dockerStorePaths(root, 5, test.stores); err == nil {
				t.Fatal("unsafe bind accepted")
			}
		})
	}
	stores, err := dockerStorePaths(root, 5, map[int]string{4: private})
	if err != nil {
		t.Fatal(err)
	}
	if stores[4] != private || stores[0] != filepath.Join(root, "node-0") {
		t.Fatalf("incorrect bindings: %v", stores)
	}
}

func TestDockerStorePathsResolveMissingRootUnderSymlink(t *testing.T) {
	host, private, separate := t.TempDir(), t.TempDir(), t.TempDir()
	alias := filepath.Join(host, "alias")
	if err := os.Symlink(private, alias); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(alias, "not-created", "nested")
	if _, err := dockerStorePaths(root, 5, map[int]string{4: private}); err == nil {
		t.Fatal("physical ancestor overlap beneath missing symlinked root was accepted")
	}
	paths, err := dockerStorePaths(root, 5, map[int]string{4: separate})
	if err != nil {
		t.Fatal(err)
	}
	if paths[0] != filepath.Join(private, "not-created", "nested", "node-0") || paths[4] != separate {
		t.Fatalf("missing root bindings are not canonical: %v", paths)
	}
}

func TestDockerStorePathsRejectDanglingRootSymlink(t *testing.T) {
	host := t.TempDir()
	alias := filepath.Join(host, "dangling")
	if err := os.Symlink(filepath.Join(host, "missing"), alias); err != nil {
		t.Fatal(err)
	}
	if _, err := dockerStorePaths(filepath.Join(alias, "cluster"), 5, nil); err == nil {
		t.Fatal("dangling cluster root symlink accepted")
	}
}
