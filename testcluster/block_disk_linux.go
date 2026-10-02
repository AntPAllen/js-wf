//go:build linux

package testcluster

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// BlockDisk mounts a private sparse image through a device-mapper linear
// target. It needs passwordless sudo and never maps a host data device.
// Stop all processes using StoreDir before Close. This models an I/O stall,
// not dm-delay's per-request delay or hardware durability semantics.
type BlockDisk struct {
	StoreDir         string
	root, loop, name string
	mounted, mapped  bool
}

func NewBlockDisk(parent string) (_ *BlockDisk, err error) {
	root, err := os.MkdirTemp(parent, "wf-block-")
	if err != nil {
		return nil, err
	}
	disk := &BlockDisk{root: root, StoreDir: filepath.Join(root, "store"), name: filepath.Base(root)}
	defer func() {
		if err != nil {
			if cleanup := disk.Close(); cleanup != nil {
				err = fmt.Errorf("%w; cleanup: %v", err, cleanup)
			}
		}
	}()
	image := filepath.Join(root, "backing.img")
	file, err := os.Create(image)
	if err != nil {
		return nil, err
	}
	err = file.Truncate(512 << 20)
	closeErr := file.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return nil, err
	}
	if err = os.Mkdir(disk.StoreDir, 0755); err != nil {
		return nil, err
	}
	ctx, done := context.WithTimeout(context.Background(), 30*time.Second)
	defer done()
	output, err := blockDiskCommand(ctx, "losetup", "--find", "--show", image)
	if err != nil {
		return nil, err
	}
	disk.loop = strings.TrimSpace(output)
	if !strings.HasPrefix(disk.loop, "/dev/loop") || strings.ContainsAny(disk.loop, " \n\t") {
		return nil, fmt.Errorf("unexpected loop device %q", disk.loop)
	}
	_, err = blockDiskCommand(ctx, "dmsetup", "create", disk.name, "--table", "0 1048576 linear "+disk.loop+" 0")
	if err != nil {
		return nil, err
	}
	disk.mapped = true
	if _, err = blockDiskCommand(ctx, "dmsetup", "mknodes", disk.name); err != nil {
		return nil, err
	}
	device := filepath.Join("/dev/mapper", disk.name)
	if _, err = blockDiskCommand(ctx, "mkfs.ext4", "-q", "-F", device); err != nil {
		return nil, err
	}
	if _, err = blockDiskCommand(ctx, "mount", "-o", "noatime", device, disk.StoreDir); err != nil {
		return nil, err
	}
	disk.mounted = true
	if _, err = blockDiskCommand(ctx, "chown", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()), disk.StoreDir); err != nil {
		return nil, err
	}
	return disk, nil
}

func blockDiskCommand(ctx context.Context, command string, args ...string) (string, error) {
	output, err := exec.CommandContext(ctx, "sudo", append([]string{"-n", command}, args...)...).CombinedOutput()
	if err != nil {
		return string(output), fmt.Errorf("%s %v: %w: %s", command, args, err, output)
	}
	return string(output), nil
}

type BlockStallProof struct {
	Suspended     time.Time `json:"suspended"`
	Resumed       time.Time `json:"resumed"`
	ResumeStarted time.Time `json:"resume_started"`
	SyncReturned  time.Time `json:"sync_returned"`
	DeviceState   string    `json:"device_state"`
}

// Stall blocks every block request on the mounted device and proves an actual
// dirty-file sync cannot finish before heal. The proof file is outside NATS's
// jetstream directory but on exactly the same filesystem as the store.
func (d *BlockDisk) Stall(ctx context.Context, duration time.Duration) (proof BlockStallProof, err error) {
	if !d.mounted || !d.mapped || duration <= 0 {
		return proof, fmt.Errorf("invalid block disk stall")
	}
	file, err := os.OpenFile(filepath.Join(d.StoreDir, ".stall-proof"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return proof, err
	}
	defer file.Close()
	if _, err = file.WriteAt(make([]byte, 4096), 0); err != nil {
		return proof, err
	}
	if err = file.Sync(); err != nil {
		return proof, err
	}
	if _, err = blockDiskCommand(ctx, "dmsetup", "suspend", "--noflush", "--nolockfs", d.name); err != nil {
		return proof, err
	}
	proof.Suspended = time.Now()
	// Always resume using an independent context, including workload cancellation.
	resumed := false
	resume := func() error {
		if resumed {
			return nil
		}
		resumeCtx, done := context.WithTimeout(context.Background(), 15*time.Second)
		defer done()
		proof.ResumeStarted = time.Now()
		_, resumeErr := blockDiskCommand(resumeCtx, "dmsetup", "resume", d.name)
		if resumeErr == nil {
			resumed = true
			proof.Resumed = time.Now()
		}
		return resumeErr
	}
	defer func() {
		if resumeErr := resume(); err == nil {
			err = resumeErr
		}
	}()
	proof.DeviceState, err = blockDiskCommand(ctx, "dmsetup", "info", "-c", "--noheadings", "-o", "suspended", d.name)
	proof.DeviceState = strings.TrimSpace(proof.DeviceState)
	if err != nil {
		return proof, err
	}
	if proof.DeviceState != "Suspended" {
		return proof, fmt.Errorf("device not suspended: %q", proof.DeviceState)
	}
	type syncResult struct {
		at  time.Time
		err error
	}
	synced := make(chan syncResult, 1)
	go func() {
		_, writeErr := file.WriteAt([]byte(proof.Suspended.Format(time.RFC3339Nano)), 0)
		if writeErr == nil {
			writeErr = file.Sync()
		}
		synced <- syncResult{time.Now(), writeErr}
	}()
	syncFinished := false
	// Joining the sync is required after cancellation: closing its descriptor
	// does not guarantee an in-flight fsync has released its mount reference.
	defer func() {
		if resumeErr := resume(); resumeErr != nil && err == nil {
			err = resumeErr
		}
		if !syncFinished {
			select {
			case <-synced:
			case <-time.After(15 * time.Second):
				if err == nil {
					err = fmt.Errorf("sync did not exit after block device resume")
				}
			}
		}
	}()
	timer := time.NewTimer(time.Until(proof.Suspended.Add(duration)))
	defer timer.Stop()
	select {
	case result := <-synced:
		syncFinished = true
		return proof, fmt.Errorf("sync escaped suspended device at %s: %v", result.at, result.err)
	case <-ctx.Done():
		return proof, ctx.Err()
	case <-timer.C:
	}
	err = resume()
	if err != nil {
		return proof, err
	}
	select {
	case result := <-synced:
		syncFinished = true
		proof.SyncReturned = result.at
		return proof, result.err
	case <-ctx.Done():
		return proof, ctx.Err()
	}
}

func (d *BlockDisk) Close() error {
	ctx, done := context.WithTimeout(context.Background(), 30*time.Second)
	defer done()
	if d.mapped {
		_, _ = blockDiskCommand(ctx, "dmsetup", "resume", d.name)
	}
	if d.mounted {
		if _, err := blockDiskCommand(ctx, "umount", d.StoreDir); err != nil {
			return err
		}
		d.mounted = false
	}
	if d.mapped {
		if _, err := blockDiskCommand(ctx, "dmsetup", "remove", d.name); err != nil {
			return err
		}
		d.mapped = false
	}
	if d.loop != "" {
		if _, err := blockDiskCommand(ctx, "losetup", "--detach", d.loop); err != nil {
			return err
		}
		d.loop = ""
	}
	return os.RemoveAll(d.root)
}
