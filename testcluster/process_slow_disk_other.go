//go:build !linux && !windows

package testcluster

import (
	"fmt"
	"path/filepath"
	"time"
)

func (c *ProcessCluster) SlowDisk(i int, latency time.Duration) error {
	return fmt.Errorf("disk delay requires Linux strace")
}

func (c *ProcessCluster) DiskTracePath(i int) string {
	return filepath.Join(c.root, fmt.Sprintf("node-%d.strace", i))
}

func (c *ProcessCluster) StopSlowDisk(i int) error {
	return fmt.Errorf("disk delay requires Linux strace")
}
