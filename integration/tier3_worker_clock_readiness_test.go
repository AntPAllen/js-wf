//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// This is the exact rejected StreamInfo from worker-clock seed 1 in run
// 37157123048: a quorum-confirmed publish with one follower still one entry behind.
func workerClockLagSnapshot(t *testing.T) *jetstream.StreamInfo {
	t.Helper()
	raw, err := os.ReadFile("testdata/worker-clock-lag-one-37157123048.json")
	if err != nil {
		t.Fatal(err)
	}
	var info jetstream.StreamInfo
	if err := json.Unmarshal(raw, &info); err != nil {
		t.Fatal(err)
	}
	return &info
}

func TestTier3WorkerClockReplicaCatchup(t *testing.T) {
	calls := 0
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	info, err := waitTier3WorkerClockReplicas(ctx, func(context.Context, ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
		calls++
		info := workerClockLagSnapshot(t)
		if calls == 2 {
			for _, peer := range info.Cluster.Replicas {
				peer.Current = true
				peer.Lag = 0
			}
		}
		return info, nil
	})
	if err != nil || calls != 2 || info == nil {
		t.Fatalf("catch-up calls=%d info=%+v err=%v", calls, info, err)
	}
	for _, peer := range info.Cluster.Replicas {
		if !peer.Current || peer.Offline {
			t.Fatal("saved the pre-catch-up snapshot")
		}
	}
}

func TestTier3WorkerClockReplicaReadinessRejectsInvalidProof(t *testing.T) {
	for _, mode := range []string{"lag", "offline", "duplicate", "missing_cluster", "wrong_replication", "wrong_storage", "wrong_subject"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			calls := 0
			info, err := waitTier3WorkerClockReplicas(ctx, func(context.Context, ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
				calls++
				x := workerClockLagSnapshot(t)
				for _, peer := range x.Cluster.Replicas {
					peer.Current = true
					peer.Lag = 0
				}
				switch mode {
				case "lag":
					x.Cluster.Replicas[0].Current = false
				case "offline":
					x.Cluster.Replicas[0].Offline = true
				case "duplicate":
					x.Cluster.Replicas[0].Name = x.Cluster.Leader
				case "missing_cluster":
					x.Cluster = nil
				case "wrong_replication":
					x.Config.Replicas = 3
				case "wrong_storage":
					x.Config.Storage = jetstream.MemoryStorage
				case "wrong_subject":
					x.Config.Subjects = []string{"another.*"}
				}
				return x, nil
			})
			if err == nil || info != nil {
				t.Fatalf("accepted invalid %s proof: %+v", mode, info)
			}
			if mode == "wrong_replication" || mode == "wrong_storage" || mode == "wrong_subject" {
				if calls != 1 || errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("configuration failure was retried: calls=%d err=%v", calls, err)
				}
			} else if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("readiness did not stop at deadline: %v", err)
			}
		})
	}
}
