//go:build linux

package integration_test

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/provision"
	"js-wf/testcluster"
)

func matrixClusterVersions(cluster *testcluster.ProcessCluster) []string {
	versions := make([]string, len(cluster.Clients))
	for node, client := range cluster.Clients {
		versions[node] = client.ConnectedServerVersion()
	}
	return versions
}

func upgradeMatrixServer(ctx context.Context, js jetstream.JetStream, cluster *testcluster.ProcessCluster, node int, scheduled time.Time) (matrixLeaderFault, error) {
	event := matrixLeaderFault{Scheduled: scheduled, Node: node, VersionsBefore: matrixClusterVersions(cluster)}
	if event.VersionsBefore[node] != "2.11.17" {
		return event, fmt.Errorf("upgrade node %d starts at %q, want 2.11.17", node, event.VersionsBefore[node])
	}
	if err := recordMatrixPeerQueues(cluster, fmt.Sprintf("upgrade-%d-%d-before", scheduled.UnixNano(), node)); err != nil {
		return event, fmt.Errorf("upgrade peer queue capture: %w", err)
	}
	bound, done := context.WithTimeout(ctx, 45*time.Second)
	defer done()
	event.Killed = time.Now()
	if err := cluster.KillNode(node); err != nil {
		return event, err
	}
	if err := cluster.UpgradeNode(node); err != nil {
		return event, err
	}
	if _, err := waitMixedVersionReplicaCatchup(bound, js, 3); err != nil {
		return event, err
	}
	event.VersionsAfter = matrixClusterVersions(cluster)
	for peer, before := range event.VersionsBefore {
		want := before
		if peer == node {
			want = server.VERSION
		}
		if event.VersionsAfter[peer] != want {
			return event, fmt.Errorf("upgrade peer %d version=%s want=%s", peer, event.VersionsAfter[peer], want)
		}
	}
	backend, err := matrixReadMetadata(bound, func(attempt context.Context) (provision.TimerBackend, error) {
		return provision.EnsureAuto(attempt, js, 3)
	})
	if err != nil || backend != provision.FallbackTimers {
		return event, fmt.Errorf("upgrade changed fallback deployment: backend=%s err=%v", backend, err)
	}
	event.Healed = time.Now()
	if err := recordMatrixPeerQueues(cluster, fmt.Sprintf("upgrade-%d-%d-healed", scheduled.UnixNano(), node)); err != nil {
		return event, fmt.Errorf("upgrade peer queue capture: %w", err)
	}
	return event, nil
}
