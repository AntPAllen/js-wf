//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/provision"
	"js-wf/testcluster"
)

func TestFiveContainerMixedRollingServerUpgrade(t *testing.T) {
	runFiveContainerMixedLeader(t, "rolling_upgrade")
}

type fiveUpgradePeer struct {
	Node           int    `json:"node"`
	Version        string `json:"version"`
	ServerID       string `json:"server_id"`
	NativeRejected string `json:"native_rejected"`
}
type fiveUpgradeBackendCheck struct {
	Started  time.Time              `json:"started"`
	Ended    time.Time              `json:"ended"`
	Deadline time.Time              `json:"deadline"`
	Backend  provision.TimerBackend `json:"backend"`
	Error    string                 `json:"error,omitempty"`
}

type fiveUpgradeProof struct {
	Version      int                      `json:"version"`
	Started      time.Time                `json:"started"`
	Complete     bool                     `json:"complete"`
	Phase        string                   `json:"phase"`
	Error        string                   `json:"error,omitempty"`
	BackendCheck *fiveUpgradeBackendCheck `json:"backend_check,omitempty"`
	At           time.Time                `json:"at"`
	Nodes        []fiveUpgradePeer        `json:"nodes"`
	Backend      provision.TimerBackend   `json:"backend"`
	Run          *jetstream.StreamInfo    `json:"run_info"`
	Timer        *jetstream.StreamInfo    `json:"timer_info"`
}

func fiveUpgradeVersions(upgraded []bool) []string {
	versions := make([]string, 5)
	for node := range versions {
		versions[node] = "2.11.17"
		if upgraded[node] {
			versions[node] = server.VERSION
		}
	}
	return versions
}

// Pin every direct endpoint; reconnect/discovery cannot substitute a peer.
// Retained fallback selection must survive each binary change, and explicit
// native provisioning must fail with its semantic version/configuration error.
func proveFiveUpgradeDeployment(ctx context.Context, js jetstream.JetStream, cluster *testcluster.DockerCluster, versions []string, path string) (observed []string, resultErr error) {
	bound, stop := context.WithTimeout(ctx, 60*time.Second)
	defer stop()
	proof := fiveUpgradeProof{Version: 2, Started: time.Now().UTC(), Phase: "peer-native-rejections"}
	// Preserve partial phase/timing evidence on failure as well as success.
	defer func() {
		proof.At = time.Now().UTC()
		if resultErr != nil {
			proof.Error = resultErr.Error()
		}
		data, err := json.MarshalIndent(proof, "", "  ")
		if err == nil {
			err = os.WriteFile(path, data, 0644)
		}
		if err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("upgrade proof artifact: %w", err))
		}
	}()
	seen := map[string]bool{}
	observed = make([]string, 5)
	for node := 0; node < 5; node++ {
		nc, err := nats.Connect(cluster.ClientURL(node), nats.NoReconnect(), nats.IgnoreDiscoveredServers(), nats.Timeout(time.Second))
		if err != nil {
			return nil, err
		}
		peer := fiveUpgradePeer{Node: node, Version: nc.ConnectedServerVersion(), ServerID: nc.ConnectedServerId()}
		if peer.Version != versions[node] || peer.ServerID == "" || seen[peer.ServerID] {
			nc.Close()
			return nil, fmt.Errorf("upgrade node%d version=%s want=%s identity=%s", node, peer.Version, versions[node], peer.ServerID)
		}
		seen[peer.ServerID] = true
		observed[node] = peer.Version
		peerJS, err := jetstream.New(nc)
		if err != nil {
			nc.Close()
			return nil, err
		}
		_, nativeErr := matrixReadMetadata(bound, func(attempt context.Context) (struct{}, error) {
			return struct{}{}, provision.Ensure(attempt, peerJS, 5)
		})
		nc.Close()
		want := "stream WF_RUN configuration mismatch:"
		if peer.Version == "2.11.17" {
			want = "native timers require NATS 2.12+; connected server reports 2.11.17"
		}
		if nativeErr == nil || !strings.HasPrefix(nativeErr.Error(), want) {
			return nil, fmt.Errorf("upgrade node%d native rejection=%v want=%s", node, nativeErr, want)
		}
		peer.NativeRejected = nativeErr.Error()
		proof.Nodes = append(proof.Nodes, peer)
	}
	proof.Phase = "fallback-provisioning"
	check, err := checkFiveUpgradeFallback(bound, func(operation context.Context) (provision.TimerBackend, error) {
		return provision.EnsureAuto(operation, js, 5)
	})
	proof.BackendCheck, proof.Backend = &check, check.Backend
	if err != nil {
		return nil, err
	}
	proof.Phase = "replica-readiness"
	if err := waitFiveReplicaReadiness(bound, js, 0); err != nil {
		return nil, err
	}
	for _, name := range []string{"WF_RUN", "WF_TIMER"} {
		info, err := matrixReadMetadata(bound, func(attempt context.Context) (*jetstream.StreamInfo, error) {
			s, err := js.Stream(attempt, name)
			if err != nil {
				return nil, err
			}
			return s.Info(attempt)
		})
		if err != nil || info == nil || info.Config.Replicas != 5 || info.Config.Storage != jetstream.FileStorage || info.Cluster == nil || info.Cluster.Leader == "" || len(info.Cluster.Replicas) != 4 {
			return nil, fmt.Errorf("upgrade %s missing R5 file readiness: %v", name, err)
		}
		for _, replica := range info.Cluster.Replicas {
			if replica == nil || !replica.Current || replica.Offline {
				return nil, fmt.Errorf("upgrade %s stale replica", name)
			}
		}
		if name == "WF_RUN" {
			if info.Config.AllowMsgSchedules {
				return nil, fmt.Errorf("upgrade unexpectedly enabled native scheduling")
			}
			proof.Run = info
		} else {
			proof.Timer = info
		}
	}
	proof.Phase, proof.Complete = "complete", true
	return observed, nil
}

// EnsureAuto checks several retained stores. Its total budget is the existing
// deployment-proof deadline, not the single metadata-read retry helper's 2s.
func checkFiveUpgradeFallback(ctx context.Context, lookup func(context.Context) (provision.TimerBackend, error)) (fiveUpgradeBackendCheck, error) {
	check := fiveUpgradeBackendCheck{Started: time.Now().UTC()}
	check.Deadline, _ = ctx.Deadline()
	backend, err := lookup(ctx)
	check.Ended, check.Backend = time.Now().UTC(), backend
	if err != nil {
		check.Error = err.Error()
		return check, fmt.Errorf("upgrade fallback provisioning proof: %w", err)
	}
	if backend != provision.FallbackTimers {
		err = fmt.Errorf("upgrade fallback changed backend=%s", backend)
		check.Error = err.Error()
		return check, err
	}
	return check, nil
}

func TestFiveUpgradeProvisioningUsesWholeProofBudget(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	check, err := checkFiveUpgradeFallback(ctx, func(operation context.Context) (provision.TimerBackend, error) {
		// Two sequential stages exceed one metadata-read budget. Restoring the
		// previous helper around this whole operation cancels before stage two.
		for stage := 0; stage < 2; stage++ {
			select {
			case <-operation.Done():
				return "", operation.Err()
			case <-time.After(1100 * time.Millisecond):
			}
		}
		return provision.FallbackTimers, nil
	})
	deadline, _ := ctx.Deadline()
	if err != nil || check.Backend != provision.FallbackTimers || check.Error != "" || !check.Deadline.Equal(deadline) || check.Ended.Sub(check.Started) < 2200*time.Millisecond {
		t.Fatalf("whole-operation proof=%+v err=%v", check, err)
	}
}

func TestFiveUpgradeProvisioningPreservesCancellationAndBackendFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	check, err := checkFiveUpgradeFallback(ctx, func(operation context.Context) (provision.TimerBackend, error) { return "", operation.Err() })
	if !errors.Is(err, context.Canceled) || check.Backend != "" || check.Error != context.Canceled.Error() {
		t.Fatalf("cancellation proof=%+v err=%v", check, err)
	}
	check, err = checkFiveUpgradeFallback(context.Background(), func(context.Context) (provision.TimerBackend, error) { return provision.NativeTimers, nil })
	if err == nil || check.Backend != provision.NativeTimers || check.Error == "" {
		t.Fatalf("changed backend proof=%+v err=%v", check, err)
	}
}

func upgradeFiveMixedServer(ctx context.Context, js jetstream.JetStream, cluster *testcluster.DockerCluster, node int, scheduled time.Time, prefix string, upgraded []bool) (matrixLeaderFault, error) {
	event := matrixLeaderFault{Scheduled: scheduled, Node: node}
	var err error
	event.VersionsBefore, err = proveFiveUpgradeDeployment(ctx, js, cluster, fiveUpgradeVersions(upgraded), prefix+"-upgrade-before.json")
	if err != nil {
		return event, err
	}
	event.Killed = time.Now().UTC()
	if upgraded[node] {
		return event, fmt.Errorf("upgrade node%d selected twice", node)
	}
	if err := cluster.UpgradeNode(node); err != nil {
		return event, err
	}
	upgraded[node] = true
	event.VersionsAfter, err = proveFiveUpgradeDeployment(ctx, js, cluster, fiveUpgradeVersions(upgraded), prefix+"-upgrade-after.json")
	if err != nil {
		return event, err
	}
	event.Healed = time.Now().UTC()
	return event, nil
}
