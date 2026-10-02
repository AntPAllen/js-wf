//go:build linux

package testcluster

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func dockerStorePaths(root string, count int, overrides map[int]string) ([]string, error) {
	if count < 3 || count > 5 {
		return nil, fmt.Errorf("docker cluster count must be 3..5")
	}
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if resolved, err := filepath.EvalSymlinks(root); err == nil {
		root = resolved
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	stores := make([]string, count)
	for node := range stores {
		stores[node] = filepath.Join(root, fmt.Sprintf("node-%d", node))
	}
	for node, path := range overrides {
		if node < 0 || node >= count || !filepath.IsAbs(path) {
			return nil, fmt.Errorf("invalid Docker store override node=%d path=%q: require an absolute directory", node, path)
		}
		info, err := os.Stat(path)
		if err != nil || !info.IsDir() {
			return nil, fmt.Errorf("Docker store override node=%d path=%q must be an existing directory: %v", node, path, err)
		}
		stores[node], err = filepath.EvalSymlinks(path)
		if err != nil {
			return nil, err
		}
	}
	for node, path := range stores {
		if resolved, err := filepath.EvalSymlinks(path); err == nil {
			stores[node], path = resolved, resolved
		} else if !os.IsNotExist(err) {
			return nil, err
		}
		// Docker -v uses ':' as a separator. Reject an ambiguous bind before
		// creating any containers or writing into an unintended host path.
		if path == string(os.PathSeparator) || strings.Contains(path, ":") {
			return nil, fmt.Errorf("Docker store path contains a volume separator: %q", path)
		}
	}
	for node, path := range stores {
		for other := 0; other < node; other++ {
			previous := stores[other]
			if path == previous || strings.HasPrefix(path, previous+string(os.PathSeparator)) || strings.HasPrefix(previous, path+string(os.PathSeparator)) {
				return nil, fmt.Errorf("Docker nodes %d and %d have overlapping stores", node, other)
			}
		}
	}
	return stores, nil
}

// StoreBinding returns Docker's observed /data mount and rejects a container
// whose writable bind differs from its configured persistent store.
func (c *DockerCluster) StoreBinding(ctx context.Context, node int) (DockerStoreBinding, error) {
	var binding DockerStoreBinding
	if node < 0 || node >= len(c.stores) {
		return binding, fmt.Errorf("invalid Docker store node %d", node)
	}
	output, err := dockerCommand(ctx, "inspect", "--format", "{{json .Mounts}}", c.names[node])
	if err != nil {
		return binding, err
	}
	var mounts []struct {
		Type, Source, Destination string
		RW                        bool
	}
	if err := json.Unmarshal([]byte(output), &mounts); err != nil {
		return binding, err
	}
	for _, mount := range mounts {
		if mount.Destination != "/data" {
			continue
		}
		if mount.Type != "bind" || !mount.RW || mount.Source != c.stores[node] {
			return binding, fmt.Errorf("Docker store binding differs for node %d: %+v", node, mount)
		}
		return DockerStoreBinding{Node: node, Container: c.names[node], Source: mount.Source, Destination: mount.Destination, Writable: mount.RW}, nil
	}
	return binding, fmt.Errorf("Docker store binding missing for node %d", node)
}

type DockerStoreBinding struct {
	Node        int    `json:"node"`
	Container   string `json:"container"`
	Source      string `json:"source"`
	Destination string `json:"destination"`
	Writable    bool   `json:"writable"`
}
