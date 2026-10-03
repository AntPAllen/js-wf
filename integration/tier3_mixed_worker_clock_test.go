//go:build linux

package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/worker"
)

func TestFiveContainerMixedWorkerClockSkew(t *testing.T) {
	runFiveContainerMixedLeader(t, "worker_clock")
}

type tier3ClockMessage struct {
	Subject  string    `json:"subject"`
	Sequence uint64    `json:"sequence"`
	Time     time.Time `json:"time"`
	Data     []byte    `json:"data"`
}

type tier3WorkerClockProof struct {
	Stage      string                    `json:"stage"`
	Observed   time.Time                 `json:"observed"`
	Samples    []matrixWorkerClockSample `json:"samples"`
	Messages   []tier3ClockMessage       `json:"messages"`
	StreamInfo *jetstream.StreamInfo     `json:"stream_info"`
}

type tier3ClockBinary struct {
	Worker string        `json:"worker"`
	Offset time.Duration `json:"offset_ns"`
	Path   string        `json:"path"`
	SHA256 string        `json:"sha256"`
}

func prepareTier3ClockBinaries(root string) ([]string, error) {
	original, err := os.ReadFile(filepath.Join(runtime.GOROOT(), "src", "time", "time.go"))
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(root, "clock-original-time.go"), original, 0644); err != nil {
		return nil, err
	}
	binaries, err := buildMatrixClockWorkers(root)
	if err != nil {
		return nil, err
	}
	// The unshifted parent executable is outside the fixture. Retain an exact
	// copy alongside shifted executables so all five actual binaries survive.
	data, err := os.ReadFile(binaries[2])
	if err != nil {
		return nil, err
	}
	binaries[2] = filepath.Join(root, "clock-unshifted.test")
	if err := os.WriteFile(binaries[2], data, 0700); err != nil {
		return nil, err
	}
	var proof []tier3ClockBinary
	for slot := 0; slot < 5; slot++ {
		data, err := os.ReadFile(binaries[slot%3])
		if err != nil {
			return nil, err
		}
		sum := sha256.Sum256(data)
		relative, err := filepath.Rel(root, binaries[slot%3])
		if err != nil {
			return nil, err
		}
		proof = append(proof, tier3ClockBinary{fmt.Sprintf("matrix-process-%d-generation-0", slot), matrixWorkerClockOffsets[slot%3], relative, hex.EncodeToString(sum[:])})
	}
	data, err = json.MarshalIndent(proof, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(root, "worker-clock-binaries.json"), data, 0644); err != nil {
		return nil, err
	}
	return binaries, nil
}

func captureTier3WorkerClocks(ctx context.Context, stream jetstream.Stream, processes []*matrixProcessWorker, stage string) (tier3WorkerClockProof, error) {
	proof := tier3WorkerClockProof{Stage: stage}
	if len(processes) != 5 {
		return proof, fmt.Errorf("clock proof requires five processes")
	}
	for slot, process := range processes {
		data, err := os.ReadFile(process.base + "-clock.json")
		if err != nil {
			return proof, err
		}
		var sample matrixWorkerClockSample
		if err := json.Unmarshal(data, &sample); err != nil {
			return proof, err
		}
		if sample.Worker != process.id {
			return proof, fmt.Errorf("clock sample worker mismatch")
		}
		message, err := matrixReadMetadata(ctx, func(attempt context.Context) (*jetstream.RawStreamMsg, error) {
			return stream.GetMsg(attempt, sample.Sequence)
		})
		if err != nil {
			return proof, err
		}
		var published matrixWorkerClockSample
		if err := json.Unmarshal(message.Data, &published); err != nil {
			return proof, err
		}
		if message.Subject != "matrix.clock."+process.id || message.Sequence != sample.Sequence || !message.Time.Equal(sample.ServerAt) || published.Worker != process.id || !published.WorkerAt.Equal(sample.WorkerAt) || published.Sequence != 0 || published.Offset != 0 {
			return proof, fmt.Errorf("worker sample differs from broker message: %s", process.id)
		}
		if err := validateMatrixWorkerClock(sample, matrixWorkerClockOffsets[slot%3], time.Now()); err != nil {
			return proof, err
		}
		proof.Samples = append(proof.Samples, sample)
		proof.Messages = append(proof.Messages, tier3ClockMessage{message.Subject, message.Sequence, message.Time, message.Data})
	}
	var err error
	// PubAck confirms quorum, not that every follower has caught up. Preserve
	// the strict five-current-replica proof by waiting before taking its snapshot.
	readiness, stop := context.WithTimeout(ctx, 2*time.Second)
	defer stop()
	proof.StreamInfo, err = waitTier3WorkerClockReplicas(readiness, stream.Info)
	proof.Observed = time.Now().UTC()
	if err != nil {
		return proof, err
	}
	for slot, sample := range proof.Samples {
		if err := validateMatrixWorkerClock(sample, matrixWorkerClockOffsets[slot%3], proof.Observed); err != nil {
			return proof, err
		}
	}
	return proof, nil
}

func waitTier3WorkerClockReplicas(ctx context.Context, lookup func(context.Context, ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error)) (*jetstream.StreamInfo, error) {
	var last *jetstream.StreamInfo
	for {
		info, err := matrixReadMetadata(ctx, func(attempt context.Context) (*jetstream.StreamInfo, error) {
			return lookup(attempt)
		})
		if err != nil {
			return nil, err
		}
		last = info
		if info == nil || info.Config.Name != "MATRIX_CLOCK" || len(info.Config.Subjects) != 1 || info.Config.Subjects[0] != "matrix.clock.*" || info.Config.Replicas != 5 || info.Config.Storage != jetstream.FileStorage || info.Config.MaxMsgsPerSubject != 16 {
			return nil, errors.New("clock probe stream configuration differs from R5 file contract")
		}
		ready := info.Cluster != nil && info.Cluster.Leader != "" && len(info.Cluster.Replicas) == 4
		if ready {
			names := map[string]bool{info.Cluster.Leader: true}
			for _, replica := range info.Cluster.Replicas {
				if replica == nil || replica.Name == "" || names[replica.Name] || !replica.Current || replica.Offline {
					ready = false
					break
				}
				names[replica.Name] = true
			}
		}
		if ready {
			return info, nil
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("clock probe replicas did not catch up: cluster=%+v: %w", last.Cluster, ctx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func normalizeTier3WorkerClockRecords(steps []worker.DispatchEvent, fences []worker.FencingEvent) ([]worker.DispatchEvent, []worker.FencingEvent, error) {
	offsets := make(map[string]time.Duration)
	for slot := 0; slot < 5; slot++ {
		offsets[fmt.Sprintf("matrix-process-%d-generation-0", slot)] = matrixWorkerClockOffsets[slot%3]
	}
	steps = append([]worker.DispatchEvent(nil), steps...)
	fences = append([]worker.FencingEvent(nil), fences...)
	for i := range steps {
		offset, ok := offsets[steps[i].Worker]
		if !ok {
			return nil, nil, fmt.Errorf("unknown clock dispatch worker")
		}
		steps[i].At = steps[i].At.Add(-offset).UTC()
	}
	for i := range fences {
		offset, ok := offsets[fences[i].Worker]
		if !ok {
			return nil, nil, fmt.Errorf("unknown clock fencing worker")
		}
		fences[i].At = fences[i].At.Add(-offset).UTC()
	}
	return steps, fences, nil
}

func TestTier3WorkerClockNormalizationPreservesRawEvidence(t *testing.T) {
	at := time.Date(2026, 10, 3, 0, 0, 1, 123456789, time.UTC)
	for slot := 0; slot < 5; slot++ {
		id := fmt.Sprintf("matrix-process-%d-generation-0", slot)
		offset := matrixWorkerClockOffsets[slot%3]
		rawSteps := []worker.DispatchEvent{{Worker: id, At: at.Add(offset), Stage: "fetched", Delivery: 2}}
		rawFences := []worker.FencingEvent{{Worker: id, At: at.Add(offset), Epoch: 7, Reason: "lease_cleanup_lost"}}
		steps, fences, err := normalizeTier3WorkerClockRecords(rawSteps, rawFences)
		if err != nil || !steps[0].At.Equal(at) || !fences[0].At.Equal(at) || steps[0].Delivery != 2 || fences[0].Epoch != 7 || fences[0].Reason != "lease_cleanup_lost" {
			t.Fatalf("clock normalization: steps=%+v fences=%+v err=%v", steps, fences, err)
		}
		if !rawSteps[0].At.Equal(at.Add(offset)) || !rawFences[0].At.Equal(at.Add(offset)) {
			t.Fatal("original worker timestamps mutated")
		}
	}
	if _, _, err := normalizeTier3WorkerClockRecords([]worker.DispatchEvent{{Worker: "foreign", At: at}}, nil); err == nil {
		t.Fatal("foreign worker accepted")
	}
}
