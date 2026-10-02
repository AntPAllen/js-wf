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
	canceledProof, err := disk.Stall(canceled, 5*time.Second)
	cancel()
	if err == nil {
		t.Fatal("canceled stall unexpectedly succeeded")
	}
	if canceledProof.Suspended.IsZero() || canceledProof.Resumed.IsZero() {
		t.Fatalf("cancellation did not exercise an active device stall: %+v", canceledProof)
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

func TestBlockDiskDelaySlowsSyncAndRestoresOnCancellation(t *testing.T) {
	if os.Getenv("WF_BLOCK_DELAY") != "1" {
		t.Skip("requires dm-delay, loop devices and passwordless sudo; set WF_BLOCK_DELAY=1")
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	proof, err := disk.Delay(ctx, 2*time.Second, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if proof.Clearing.Sub(proof.Applied) < 2*time.Second || proof.SyncReturned.Sub(proof.SyncStarted) < 100*time.Millisecond || proof.Restored.Before(proof.Clearing) {
		t.Fatalf("unverified delay: %+v", proof)
	}
	t.Logf("delay proof=%+v", proof)
	// Cancel after the real probe reports completion, with the target still
	// applied. Cleanup must leave a usable linear filesystem.
	canceled, stop := context.WithCancel(context.Background())
	go func() { time.Sleep(time.Second); stop() }()
	canceledProof, err := disk.Delay(canceled, 5*time.Second, 100*time.Millisecond)
	stop()
	if err == nil || canceledProof.Applied.IsZero() || canceledProof.Restored.IsZero() {
		t.Fatalf("cancellation did not restore active fault: %+v err=%v", canceledProof, err)
	}
	if err := os.WriteFile(disk.StoreDir+"/after-delay-cancel", []byte("ok"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := disk.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(disk.root); !os.IsNotExist(err) {
		t.Fatalf("private delay image remains: %v", err)
	}
}
