//go:build linux

package testcluster

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// BlockDelayProof records an active dm-delay table and an actual dirty-file
// sync on its filesystem. It makes no claim about NATS's individual syscalls.
type BlockDelayProof struct {
	Applied       time.Time `json:"applied"`
	Clearing      time.Time `json:"clearing"`
	Restored      time.Time `json:"restored"`
	SyncStarted   time.Time `json:"sync_started"`
	SyncReturned  time.Time `json:"sync_returned"`
	DelayNS       int64     `json:"delay_ns"`
	ActiveTable   string    `json:"active_table"`
	RestoredTable string    `json:"restored_table"`
}

// switchTable preserves the same private backing image. Even a canceled reload
// must resume the device; an inactive failed table must not leave it suspended.
func (d *BlockDisk) switchTable(ctx context.Context, table string) (err error) {
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_, resumeErr := blockDiskCommand(cleanup, "dmsetup", "resume", d.name)
		err = errors.Join(err, resumeErr)
	}()
	if _, err = blockDiskCommand(ctx, "dmsetup", "suspend", "--noflush", "--nolockfs", d.name); err != nil {
		return err
	}
	_, err = blockDiskCommand(ctx, "dmsetup", "reload", d.name, "--table", table)
	return err
}

func validBlockDelayTable(table string, delay time.Duration) bool {
	fields := strings.Fields(table)
	return len(fields) == 6 && fields[0] == "0" && fields[1] == "1048576" && fields[2] == "delay" && fields[4] == "0" && fields[5] == strconv.FormatInt(delay.Milliseconds(), 10)
}

// Delay applies a per-request read/write/flush delay for duration. All callers
// must serialize faults on this disk. Cleanup restores the linear table using
// an independent context, including when the supplied context is canceled.
func (d *BlockDisk) Delay(ctx context.Context, duration, delay time.Duration) (proof BlockDelayProof, err error) {
	if !d.mounted || !d.mapped || duration <= 0 || delay < time.Millisecond || delay > time.Second || delay%time.Millisecond != 0 {
		return proof, fmt.Errorf("invalid block disk delay")
	}
	file, err := os.OpenFile(filepath.Join(d.StoreDir, ".delay-proof"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return proof, err
	}
	defer file.Close()
	proof.DelayNS = int64(delay)
	linear := "0 1048576 linear " + d.loop + " 0"
	restored := false
	restore := func() error {
		if restored {
			return nil
		}
		proof.Clearing = time.Now().UTC()
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := d.switchTable(cleanup, linear); err != nil {
			return err
		}
		table, err := blockDiskCommand(cleanup, "dmsetup", "table", d.name)
		proof.RestoredTable = strings.TrimSpace(table)
		if err != nil {
			return err
		}
		fields := strings.Fields(table)
		if len(fields) != 5 || fields[0] != "0" || fields[1] != "1048576" || fields[2] != "linear" || fields[4] != "0" {
			return fmt.Errorf("linear table restoration unconfirmed: %q", table)
		}
		if active := strings.Fields(proof.ActiveTable); len(active) == 6 && active[3] != fields[3] {
			return fmt.Errorf("restored table changed its backing device")
		}
		proof.Restored = time.Now().UTC()
		restored = true
		return nil
	}
	defer func() { err = errors.Join(err, restore()) }()
	if err = d.switchTable(ctx, "0 1048576 delay "+d.loop+" 0 "+strconv.FormatInt(delay.Milliseconds(), 10)); err != nil {
		return proof, err
	}
	proof.Applied = time.Now().UTC()
	table, err := blockDiskCommand(ctx, "dmsetup", "table", d.name)
	proof.ActiveTable = strings.TrimSpace(table)
	if err != nil {
		return proof, err
	}
	if !validBlockDelayTable(table, delay) {
		return proof, fmt.Errorf("active delay table unconfirmed: %q", table)
	}
	type syncResult struct {
		started, returned time.Time
		err               error
	}
	results := make(chan syncResult, 1)
	go func() {
		started := time.Now().UTC()
		_, syncErr := file.WriteAt([]byte(started.Format(time.RFC3339Nano)), 0)
		if syncErr == nil {
			syncErr = file.Sync()
		}
		results <- syncResult{started, time.Now().UTC(), syncErr}
	}()
	joined := false
	defer func() {
		err = errors.Join(err, restore())
		if !joined {
			select {
			case result := <-results:
				proof.SyncStarted, proof.SyncReturned = result.started, result.returned
				err = errors.Join(err, result.err)
			case <-time.After(15 * time.Second):
				err = errors.Join(err, fmt.Errorf("delayed sync did not exit after restore"))
			}
		}
	}()
	select {
	case result := <-results:
		joined = true
		proof.SyncStarted, proof.SyncReturned = result.started, result.returned
		if result.err != nil {
			return proof, result.err
		}
		if result.returned.Sub(result.started) < delay {
			return proof, fmt.Errorf("dirty sync escaped per-request delay: %+v", proof)
		}
	case <-ctx.Done():
		return proof, ctx.Err()
	}
	timer := time.NewTimer(time.Until(proof.Applied.Add(duration)))
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		return proof, ctx.Err()
	}
	err = restore()
	return proof, err
}
