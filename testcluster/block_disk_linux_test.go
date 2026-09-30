//go:build linux

package testcluster

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestBlockDiskStallBlocksSyncAndCleansUp(t *testing.T) {
	if os.Getenv("WF_BLOCK_DISK") != "1" {
		t.Skip("requires Linux device mapper, loop devices and passwordless sudo; set WF_BLOCK_DISK=1")
	}
	disk, err := NewBlockDisk(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := disk.Close(); err != nil {
			t.Error(err)
		}
	}()
	ctx, done := context.WithTimeout(context.Background(), 30*time.Second)
	defer done()
	proof, err := disk.Stall(ctx, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("block stall proof=%+v", proof)
	if proof.Resumed.Sub(proof.Suspended) < 5*time.Second || proof.SyncReturned.Sub(proof.Suspended) < 5*time.Second {
		t.Fatalf("invalid I/O stall proof: %+v", proof)
	}
	// Cancel while the block fault is active: cleanup must restore the device,
	// allowing the queued sync and a subsequent ordinary write to finish.
	canceled, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	_, err = disk.Stall(canceled, 5*time.Second)
	cancel()
	if err == nil {
		t.Fatal("canceled stall unexpectedly succeeded")
	}
	if err := os.WriteFile(disk.StoreDir+"/after-cancel", []byte("ok"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := disk.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(disk.root); !os.IsNotExist(err) {
		t.Fatalf("private image root remains: %v", err)
	}
}
