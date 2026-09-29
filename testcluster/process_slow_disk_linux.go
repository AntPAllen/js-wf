//go:build linux

package testcluster

import (
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// SlowDisk delays writes and syncs on files already present in one node's
// JetStream store. strace filters by exact path, so newly created files are
// outside this fault until it is reapplied after they exist.
func (c *ProcessCluster) SlowDisk(i int, latency time.Duration) error {
	if i < 0 || i >= len(c.Commands) || c.Commands[i] == nil || c.Commands[i].ProcessState != nil || c.paused[i] {
		return fmt.Errorf("process node %d cannot have disk delay applied", i)
	}
	if latency < time.Millisecond || c.slowDisk[i] != nil {
		return fmt.Errorf("process node %d disk delay must be at least 1ms and not already active", i)
	}
	storeDir := filepath.Join(c.root, fmt.Sprintf("node-%d", i))
	var files []string
	if err := filepath.WalkDir(storeDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type().IsRegular() {
			files = append(files, path)
		}
		return nil
	}); err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("process node %d has no existing store files to delay", i)
	}
	args := []string{"-f", "-p", strconv.Itoa(c.Commands[i].Process.Pid), "-o", c.DiskTracePath(i), "-e", "inject=write,writev,pwrite64,pwritev,pwritev2,fsync,fdatasync:delay_enter=" + strconv.FormatInt(latency.Milliseconds(), 10) + "ms"}
	for _, path := range files {
		args = append(args, "-P", path)
	}
	cmd := exec.Command("strace", args...)
	logFile, err := os.OpenFile(filepath.Join(c.root, fmt.Sprintf("node-%d.strace-error.log", i)), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	cmd.Stderr = logFile
	err = cmd.Start()
	_ = logFile.Close()
	if err != nil {
		return fmt.Errorf("start disk delay tracer: %w", err)
	}
	c.slowDisk[i] = cmd
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		data, readErr := os.ReadFile(fmt.Sprintf("/proc/%d/status", c.Commands[i].Process.Pid))
		if readErr == nil {
			for _, line := range strings.Split(string(data), "\n") {
				if strings.HasPrefix(line, "TracerPid:") {
					fields := strings.Fields(line)
					if len(fields) == 2 && fields[1] == strconv.Itoa(cmd.Process.Pid) {
						return nil
					}
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	_ = c.StopSlowDisk(i)
	data, _ := os.ReadFile(filepath.Join(c.root, fmt.Sprintf("node-%d.strace-error.log", i)))
	return fmt.Errorf("disk delay tracer did not attach to node %d: %s", i, strings.TrimSpace(string(data)))
}

func (c *ProcessCluster) DiskTracePath(i int) string {
	return filepath.Join(c.root, fmt.Sprintf("node-%d.strace", i))
}

func (c *ProcessCluster) StopSlowDisk(i int) error {
	if i < 0 || i >= len(c.slowDisk) || c.slowDisk[i] == nil {
		return fmt.Errorf("process node %d has no disk delay", i)
	}
	cmd := c.slowDisk[i]
	c.slowDisk[i] = nil
	_ = cmd.Process.Signal(syscall.SIGINT)
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
		return nil
	case <-time.After(3 * time.Second):
		_ = cmd.Process.Kill()
		<-done
		return fmt.Errorf("disk delay tracer for node %d did not detach promptly", i)
	}
}
