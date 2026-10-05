//go:build linux

package testcluster

import (
	"context"
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"
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
	if proof.ResumeStarted.Sub(proof.Suspended) < 5*time.Second || proof.Resumed.Before(proof.ResumeStarted) || proof.SyncReturned.Before(proof.ResumeStarted) {
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

// Inspect only a copy after cleanup: retain original filesystem bytes and prove
// that ordinary Close releases the devices while preserving a readable image.
func TestBlockDiskRetainsClosedMediaForCopiedReview(t *testing.T) {
	if os.Getenv("WF_BLOCK_DISK") != "1" {
		t.Skip("set WF_BLOCK_DISK=1 for real loop/device-mapper media retention")
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
	disk.RetainMediaOnClose()
	payload := []byte("retained closed block fixture evidence")
	if err := os.WriteFile(filepath.Join(disk.StoreDir, "retained-proof"), payload, 0600); err != nil {
		t.Fatal(err)
	}
	if err := disk.Close(); err != nil {
		t.Fatal(err)
	}
	if disk.mounted || disk.mapped || disk.loop != "" {
		t.Fatal("owned block resources remain after close")
	}
	hash := func(path string) [32]byte {
		t.Helper()
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		h := sha256.New()
		if _, err := io.Copy(h, f); err != nil {
			t.Fatal(err)
		}
		var result [32]byte
		copy(result[:], h.Sum(nil))
		return result
	}
	before := hash(disk.ImagePath())
	copied := filepath.Join(t.TempDir(), "copied.img")
	source, err := os.Open(disk.ImagePath())
	if err != nil {
		t.Fatal(err)
	}
	target, err := os.Create(copied)
	if err != nil {
		source.Close()
		t.Fatal(err)
	}
	_, copyErr := io.Copy(target, source)
	source.Close()
	closeErr := target.Close()
	if copyErr != nil || closeErr != nil {
		t.Fatalf("copy image: %v close=%v", copyErr, closeErr)
	}
	if hash(copied) != before {
		t.Fatal("initial copied image bytes differ")
	}
	mount := filepath.Join(t.TempDir(), "review")
	if err := os.Mkdir(mount, 0700); err != nil {
		t.Fatal(err)
	}
	ctx, done := context.WithTimeout(context.Background(), 30*time.Second)
	defer done()
	if _, err := blockDiskCommand(ctx, "mount", "-o", "loop,ro,noload", copied, mount); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		if _, err := blockDiskCommand(cleanup, "umount", mount); err != nil {
			t.Error(err)
		}
	}()
	got, err := os.ReadFile(filepath.Join(mount, "retained-proof"))
	if err != nil || string(got) != string(payload) {
		t.Fatalf("closed copied proof=%q err=%v", got, err)
	}
	if hash(disk.ImagePath()) != before || hash(copied) != before {
		t.Fatal("read-only copied review changed filesystem image bytes")
	}
	t.Logf("closed image sha256=%x, copied read-only proof verified, original media unchanged", before)
}
