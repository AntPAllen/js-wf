//go:build linux

package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func fiveUpgradeReadyStream() *jetstream.StreamInfo {
	info := &jetstream.StreamInfo{Config: jetstream.StreamConfig{Replicas: 5, Storage: jetstream.FileStorage}, Cluster: &jetstream.ClusterInfo{Leader: "leader"}}
	for i := 0; i < 4; i++ {
		info.Cluster.Replicas = append(info.Cluster.Replicas, &jetstream.PeerInfo{Current: true})
	}
	return info
}

func TestFiveUpgradeReplicaReadinessWaitsForCatchUp(t *testing.T) {
	ctx, stop := context.WithTimeout(context.Background(), 2*time.Second)
	defer stop()
	deadline, _ := ctx.Deadline()
	for _, state := range []string{"stale", "offline", "missing-leader", "missing-peer"} {
		t.Run(state, func(t *testing.T) {
			calls := 0
			var observations []*jetstream.StreamInfo
			info, err := waitFiveUpgradeStreamReadiness(ctx, "WF_RUN", func(operation context.Context) (*jetstream.StreamInfo, error) {
				observedDeadline, ok := operation.Deadline()
				if !ok || observedDeadline != deadline {
					t.Fatal("whole-proof deadline changed")
				}
				calls++
				candidate := fiveUpgradeReadyStream()
				if calls == 1 {
					switch state {
					case "stale":
						candidate.Cluster.Replicas[0].Current = false
					case "offline":
						candidate.Cluster.Replicas[0].Offline = true
					case "missing-leader":
						candidate.Cluster.Leader = ""
					case "missing-peer":
						candidate.Cluster.Replicas = candidate.Cluster.Replicas[:3]
					}
				}
				return candidate, nil
			}, func(info *jetstream.StreamInfo, err error) { observations = append(observations, info) })
			if err != nil || calls != 2 || len(observations) != 2 || info != observations[1] {
				t.Fatalf("calls=%d observations=%d info=%v err=%v", calls, len(observations), info, err)
			}
		})
	}
}

func TestFiveUpgradeReplicaReadinessPreservesDeadlineAndLastObservation(t *testing.T) {
	ctx, stop := context.WithTimeout(context.Background(), 75*time.Millisecond)
	defer stop()
	stale := fiveUpgradeReadyStream()
	stale.Cluster.Replicas[0].Current = false
	calls, observed := 0, 0
	info, err := waitFiveUpgradeStreamReadiness(ctx, "WF_TIMER", func(context.Context) (*jetstream.StreamInfo, error) { calls++; return stale, nil }, func(info *jetstream.StreamInfo, err error) {
		observed++
		if info != stale {
			t.Fatal("lost rejected metadata")
		}
	})
	if !errors.Is(err, context.DeadlineExceeded) || info != stale || calls != 1 || observed != 1 {
		t.Fatalf("calls=%d observed=%d info=%v err=%v", calls, observed, info, err)
	}
}

func TestFiveUpgradeReplicaReadinessRejectsConfigurationWithoutRetry(t *testing.T) {
	for _, bad := range []string{"replicas", "storage", "scheduling"} {
		t.Run(bad, func(t *testing.T) {
			candidate := fiveUpgradeReadyStream()
			switch bad {
			case "replicas":
				candidate.Config.Replicas = 3
			case "storage":
				candidate.Config.Storage = jetstream.MemoryStorage
			case "scheduling":
				candidate.Config.AllowMsgSchedules = true
			}
			calls := 0
			info, err := waitFiveUpgradeStreamReadiness(context.Background(), "WF_RUN", func(context.Context) (*jetstream.StreamInfo, error) { calls++; return candidate, nil }, func(*jetstream.StreamInfo, error) {})
			if err == nil || calls != 1 || info != candidate {
				t.Fatalf("calls=%d info=%v err=%v", calls, info, err)
			}
		})
	}
}

func TestFiveUpgradeReplicaReadinessPreservesCancellationAndMetadataError(t *testing.T) {
	ctx, stop := context.WithCancel(context.Background())
	stop()
	called := false
	_, err := waitFiveUpgradeStreamReadiness(ctx, "WF_RUN", func(context.Context) (*jetstream.StreamInfo, error) { called = true; return nil, nil }, func(*jetstream.StreamInfo, error) {})
	if called || !errors.Is(err, context.Canceled) {
		t.Fatalf("called=%v err=%v", called, err)
	}
	sentinel := errors.New("semantic metadata failure")
	_, err = waitFiveUpgradeStreamReadiness(context.Background(), "WF_RUN", func(context.Context) (*jetstream.StreamInfo, error) { return nil, sentinel }, func(info *jetstream.StreamInfo, err error) {
		if info != nil || err != sentinel {
			t.Fatal("lost metadata error")
		}
	})
	if !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
}
