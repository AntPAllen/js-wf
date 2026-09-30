//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/testcluster"
)

var matrixWorkerClockOffsets = []time.Duration{5 * time.Second, -5 * time.Second, 0}

type matrixWorkerClockSample struct {
	Worker   string        `json:"worker"`
	WorkerAt time.Time     `json:"worker_at"`
	ServerAt time.Time     `json:"server_at"`
	Sequence uint64        `json:"sequence"`
	Offset   time.Duration `json:"offset_ns"`
}

func buildMatrixClockWorkers(troot string) ([]string, error) {
	binaries := make([]string, 3)
	for i, offset := range matrixWorkerClockOffsets {
		if offset == 0 {
			var err error
			binaries[i], err = os.Executable()
			if err != nil {
				return nil, err
			}
			continue
		}
		root := filepath.Join(troot, fmt.Sprintf("clock-%d", i))
		overlay, err := testcluster.WriteClockOverlay(root, offset)
		if err != nil {
			return nil, err
		}
		binaries[i] = filepath.Join(root, "integration-clock.test")
		// Instrument the worker even when the parent smoke command is not -race.
		command := exec.Command("go", "test", "-c", "-race", "-overlay="+overlay, "-o", binaries[i], ".")
		if output, err := command.CombinedOutput(); err != nil {
			return nil, fmt.Errorf("build %s worker: %w: %s", offset, err, output)
		}
	}
	return binaries, nil
}

func writeMatrixWorkerClock(ctx context.Context, js jetstream.JetStream, stream jetstream.Stream, base, id string) error {
	sample := matrixWorkerClockSample{Worker: id, WorkerAt: time.Now().UTC()}
	payload, _ := json.Marshal(sample)
	ack, err := js.Publish(ctx, "matrix.clock."+id, payload)
	if err != nil {
		return err
	}
	message, err := stream.GetMsg(ctx, ack.Sequence)
	if err != nil {
		return err
	}
	sample.ServerAt, sample.Sequence = message.Time, message.Sequence
	sample.Offset = sample.WorkerAt.Sub(sample.ServerAt)
	payload, _ = json.Marshal(sample)
	if err := os.WriteFile(base+"-clock.tmp", payload, 0600); err != nil {
		return err
	}
	return os.Rename(base+"-clock.tmp", base+"-clock.json")
}

func validateMatrixWorkerClock(sample matrixWorkerClockSample, want time.Duration, now time.Time) error {
	if sample.Worker == "" || sample.Sequence == 0 || sample.ServerAt.IsZero() || sample.WorkerAt.IsZero() {
		return fmt.Errorf("incomplete worker clock proof")
	}
	if now.Sub(sample.ServerAt) > 4*time.Second || sample.ServerAt.After(now.Add(time.Second)) {
		return fmt.Errorf("stale or future server clock sample: %s", sample.ServerAt)
	}
	if sample.Offset != sample.WorkerAt.Sub(sample.ServerAt) || sample.Offset < want-time.Second || sample.Offset > want+time.Second {
		return fmt.Errorf("worker %s measured offset=%s want=%s±1s", sample.Worker, sample.Offset, want)
	}
	return nil
}

func verifyMatrixWorkerClocks(processes []*matrixProcessWorker, scheduled time.Time) (matrixLeaderFault, error) {
	event := matrixLeaderFault{Scheduled: scheduled, Node: -1}
	for i, process := range processes {
		data, err := os.ReadFile(process.base + "-clock.json")
		if err != nil {
			return event, err
		}
		var sample matrixWorkerClockSample
		if err := json.Unmarshal(data, &sample); err != nil {
			return event, err
		}
		if sample.Worker != process.id {
			return event, fmt.Errorf("clock sample belongs to another worker")
		}
		if err := validateMatrixWorkerClock(sample, matrixWorkerClockOffsets[i], time.Now()); err != nil {
			return event, err
		}
		event.ClockSamples = append(event.ClockSamples, sample)
	}
	event.Healed = time.Now()
	return event, nil
}

func TestMatrixWorkerClockProofRejectsUnshiftedAndStaleSamples(t *testing.T) {
	now := time.Now().UTC()
	sample := matrixWorkerClockSample{Worker: "one", WorkerAt: now.Add(5 * time.Second), ServerAt: now, Sequence: 1, Offset: 5 * time.Second}
	if err := validateMatrixWorkerClock(sample, 5*time.Second, now); err != nil {
		t.Fatal(err)
	}
	sample.WorkerAt = now
	sample.Offset = 0
	if err := validateMatrixWorkerClock(sample, 5*time.Second, now); err == nil {
		t.Fatal("unshifted worker accepted")
	}
	sample.WorkerAt = now.Add(5 * time.Second)
	sample.Offset = 5 * time.Second
	if err := validateMatrixWorkerClock(sample, 5*time.Second, now.Add(5*time.Second)); err == nil {
		t.Fatal("stale clock proof accepted")
	}
}
