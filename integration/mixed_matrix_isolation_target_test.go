//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"js-wf/lease"
	"js-wf/worker"
)

// A ready marker names a newly acquired delivery that cannot execute or release
// until the parent installs the fault. A sampled active count cannot give that
// guarantee: the release may already have committed while its observer waits.
type matrixIsolationTarget struct {
	Token    string               `json:"token"`
	Delivery worker.DispatchEvent `json:"delivery"`
}

func holdMatrixIsolationTarget(ctx context.Context, base string, delivery worker.DispatchEvent) error {
	armed, err := os.ReadFile(base + "-isolation-arm")
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	data, err := json.Marshal(matrixIsolationTarget{Token: string(armed), Delivery: delivery})
	if err != nil {
		return err
	}
	if err := os.WriteFile(base+"-isolation-ready.tmp", data, 0600); err != nil {
		return err
	}
	if err := os.Rename(base+"-isolation-ready.tmp", base+"-isolation-ready.json"); err != nil {
		return err
	}
	bounded, stop := context.WithTimeout(ctx, 10*time.Second)
	defer stop()
	for {
		current, err := os.ReadFile(base + "-isolation-arm")
		if os.IsNotExist(err) || (err == nil && string(current) != string(armed)) {
			return nil
		}
		if err != nil {
			return err
		}
		select {
		case <-bounded.Done():
			return fmt.Errorf("isolation acquisition barrier: %w", bounded.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func armMatrixIsolationTarget(ctx context.Context, fleet []*matrixProcessWorker, first int) (index int, target matrixIsolationTarget, release func() error, err error) {
	var once sync.Once
	var releaseErr error
	release = func() error {
		once.Do(func() {
			for _, process := range fleet {
				if removeErr := os.Remove(process.base + "-isolation-arm"); removeErr != nil && !os.IsNotExist(removeErr) {
					releaseErr = errors.Join(releaseErr, removeErr)
				}
			}
		})
		return releaseErr
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, release())
		}
	}()
	token := time.Now().UTC().Format(time.RFC3339Nano)
	for _, process := range fleet {
		if err = os.WriteFile(process.base+"-isolation-arm", []byte(token), 0600); err != nil {
			return
		}
	}
	ready, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	for ready.Err() == nil {
		for offset := range fleet {
			index = (first + offset) % len(fleet)
			data, readErr := os.ReadFile(fleet[index].base + "-isolation-ready.json")
			if os.IsNotExist(readErr) {
				continue
			}
			if readErr != nil {
				err = readErr
				return
			}
			if err = json.Unmarshal(data, &target); err != nil {
				return
			}
			if target.Token == token && target.Delivery.Stage == "lease_acquired" && target.Delivery.Worker == fleet[index].id {
				return
			}
		}
		select {
		case <-ready.Done():
		case <-time.After(5 * time.Millisecond):
		}
	}
	err = fmt.Errorf("no acquired delivery available for reply isolation: %w", ready.Err())
	return
}

func TestMatrixIsolationAcquisitionHandoff(t *testing.T) {
	ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	base := filepath.Join(t.TempDir(), "worker")
	process := &matrixProcessWorker{base: base, id: "worker"}
	// A ready file left by the previous fault must never select a released lease.
	stale, _ := json.Marshal(matrixIsolationTarget{Token: "previous", Delivery: worker.DispatchEvent{Worker: "worker", Stage: "lease_acquired", ID: "released"}})
	if err := os.WriteFile(base+"-isolation-ready.json", stale, 0600); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		for {
			if _, err := os.Stat(base + "-isolation-arm"); err == nil {
				break
			}
			select {
			case <-ctx.Done():
				finished <- ctx.Err()
				return
			case <-time.After(time.Millisecond):
			}
		}
		close(entered)
		finished <- holdMatrixIsolationTarget(ctx, base, worker.DispatchEvent{At: time.Now(), Worker: "worker", Stage: "lease_acquired", Type: "short", ID: "current", RunSequence: 17})
	}()
	index, target, release, err := armMatrixIsolationTarget(ctx, []*matrixProcessWorker{process}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	<-entered
	if index != 0 || target.Delivery.ID != "current" || target.Delivery.RunSequence != 17 {
		t.Fatalf("stale target: %+v", target)
	}
	select {
	case err := <-finished:
		t.Fatalf("delivery proceeded before fault installed: %v", err)
	default:
	}
	// The parent installs its reply hold here, then permits execution to start.
	release()
	release()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestMatrixIsolationFailedSelectionDisarmsWorkers(t *testing.T) {
	base := filepath.Join(t.TempDir(), "worker")
	ctx, stop := context.WithCancel(context.Background())
	stop()
	_, _, _, err := armMatrixIsolationTarget(ctx, []*matrixProcessWorker{{base: base, id: "worker"}}, 0)
	if err == nil {
		t.Fatal("canceled selection succeeded")
	}
	if _, err := os.Stat(base + "-isolation-arm"); !os.IsNotExist(err) {
		t.Fatalf("worker left armed: %v", err)
	}
}

func TestMatrixIsolationFencingBelongsToSelectedDelivery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dispatch.jsonl")
	cut := time.Now()
	target := worker.DispatchEvent{At: cut.Add(time.Second), Worker: "worker", Type: "short", ID: "selected", RunSequence: 7, Delivery: 2, Stage: "execution_retry", Error: lease.ErrLost.Error()}
	other := target
	other.ID = "other"
	previous := target
	previous.Delivery = 1
	before := target
	before.At = cut.Add(-time.Second)
	write := func(events ...worker.DispatchEvent) {
		t.Helper()
		file, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		encoder := json.NewEncoder(file)
		for _, event := range events {
			if err := encoder.Encode(event); err != nil {
				t.Fatal(err)
			}
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}
	write(other, previous, before)
	if n, err := matrixWorkerDeliveryFencingEvents(path, cut, &target); err != nil || n != 0 {
		t.Fatalf("unrelated fencing accepted: n=%d err=%v", n, err)
	}
	write(other, previous, before, target)
	if n, err := matrixWorkerDeliveryFencingEvents(path, cut, &target); err != nil || n != 1 {
		t.Fatalf("selected fencing absent: n=%d err=%v", n, err)
	}
}
