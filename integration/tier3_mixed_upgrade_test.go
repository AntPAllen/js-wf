//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
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
type fiveUpgradeProof struct {
	At      time.Time              `json:"at"`
	Nodes   []fiveUpgradePeer      `json:"nodes"`
	Backend provision.TimerBackend `json:"backend"`
	Run     *jetstream.StreamInfo  `json:"run_info"`
	Timer   *jetstream.StreamInfo  `json:"timer_info"`
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
func proveFiveUpgradeDeployment(ctx context.Context, js jetstream.JetStream, cluster *testcluster.DockerCluster, versions []string, path string) ([]string, error) {
	bound, stop := context.WithTimeout(ctx, 60*time.Second)
	defer stop()
	proof := fiveUpgradeProof{At: time.Now().UTC()}
	seen := map[string]bool{}
	observed := make([]string, 5)
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
	var err error
	proof.Backend, err = matrixReadMetadata(bound, func(attempt context.Context) (provision.TimerBackend, error) {
		return provision.EnsureAuto(attempt, js, 5)
	})
	if err != nil || proof.Backend != provision.FallbackTimers {
		return nil, fmt.Errorf("upgrade fallback changed backend=%s err=%v", proof.Backend, err)
	}
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
	proof.At = time.Now().UTC()
	data, err := json.MarshalIndent(proof, "", "  ")
	if err == nil {
		err = os.WriteFile(path, data, 0644)
	}
	return observed, err
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
