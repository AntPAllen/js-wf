//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/testcluster"
)

// Preserve ordinary delays outside an outage. For an interval overlapping
// quorum loss, recovery begins at its last confirmed route/R5 heal, or at the
// enabling event if later. Completion during an outage has zero recovery delay.
func tier3RouteRecoveryDelay(sample matrixLatencySample, faults []matrixLeaderFault) time.Duration {
	baseline := sample.Enabled
	for _, fault := range faults {
		if !sample.Observed.Before(fault.Killed) && !sample.Enabled.After(fault.Healed) && fault.Healed.After(baseline) {
			baseline = fault.Healed
		}
	}
	if !sample.Observed.After(baseline) {
		return 0
	}
	return sample.Observed.Sub(baseline)
}

func partitionFiveContainerMixedRoute(ctx context.Context, js jetstream.JetStream, cluster *testcluster.DockerCluster, scheduled time.Time, prefix string, rng *rand.Rand, nc *nats.Conn, quorumRemoving bool) (matrixLeaderFault, error) {
	event := matrixLeaderFault{Scheduled: scheduled, Node: -1}
	bound, stop := context.WithTimeout(ctx, 90*time.Second)
	defer stop()
	type observation struct {
		Phase  string
		Node   int
		Routes int
		At     time.Time
	}
	var observations []observation
	write := func() error {
		data, err := json.MarshalIndent(observations, "", "  ")
		if err != nil {
			return err
		}
		return os.WriteFile(prefix+"-route-observations.json", data, 0644)
	}
	defer func() { _ = write() }()
	before, err := js.Publish(bound, "tier3.route.probe", []byte("before"))
	if err != nil {
		return event, err
	}
	nodes := rng.Perm(5)[:3]
	if !quorumRemoving {
		nodes = nil
		for _, node := range rng.Perm(5) {
			if cluster.ClientURL(node) != nc.ConnectedUrl() {
				nodes = []int{node}
				break
			}
		}
		if len(nodes) != 1 {
			return event, fmt.Errorf("no node distinct from majority client's connected server")
		}
	}
	event.Killed = time.Now()
	var disconnected []int
	defer func() {
		for _, node := range disconnected {
			_ = cluster.ConnectNode(node)
		}
	}()
	for _, node := range nodes {
		if err = cluster.DisconnectNode(node); err != nil {
			return event, err
		}
		disconnected = append(disconnected, node)
		event.Nodes = append(event.Nodes, node)
	}
	for _, node := range nodes {
		routes, err := waitRouteCounts(bound, cluster, node, 0, 30*time.Second)
		if err != nil {
			return event, err
		}
		observations = append(observations, observation{"isolated", node, routes, time.Now().UTC()})
	}
	var majorityBefore *jetstream.StreamInfo
	if !quorumRemoving {
		stream, err := matrixReadMetadata(bound, func(attempt context.Context) (jetstream.Stream, error) { return js.Stream(attempt, "WF_JRN") })
		if err != nil {
			return event, err
		}
		majorityBefore, err = matrixReadMetadata(bound, func(attempt context.Context) (*jetstream.StreamInfo, error) { return stream.Info(attempt) })
		if err != nil {
			return event, err
		}
	}
	// Client ports remain reachable; only server-to-server routes are removed.
	attempt, cancel := context.WithTimeout(bound, 3*time.Second)
	during, publishErr := js.Publish(attempt, "tier3.route.probe", []byte("during"))
	cancel()
	if quorumRemoving && publishErr == nil {
		return event, fmt.Errorf("R5 publication acknowledged with three route-isolated peers: %+v", during)
	}
	if !quorumRemoving && publishErr != nil {
		return event, fmt.Errorf("majority probe did not acknowledge: %w", publishErr)
	}
	message := ""
	if publishErr != nil {
		message = publishErr.Error()
	}
	var acknowledged uint64
	if during != nil {
		acknowledged = during.Sequence
	}
	data, err := json.MarshalIndent(struct {
		Before               uint64
		UnacknowledgedError  string
		AcknowledgedSequence uint64
	}{before.Sequence, message, acknowledged}, "", "  ")
	if err != nil {
		return event, err
	}
	if err = os.WriteFile(prefix+"-quorum-probe.json", data, 0644); err != nil {
		return event, err
	}
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-bound.Done():
		return event, bound.Err()
	case <-timer.C:
	}
	if !quorumRemoving {
		stream, err := matrixReadMetadata(bound, func(attempt context.Context) (jetstream.Stream, error) { return js.Stream(attempt, "WF_JRN") })
		if err != nil {
			return event, err
		}
		after, err := matrixReadMetadata(bound, func(attempt context.Context) (*jetstream.StreamInfo, error) { return stream.Info(attempt) })
		if err != nil {
			return event, err
		}
		if after.State.LastSeq <= majorityBefore.State.LastSeq {
			return event, fmt.Errorf("no workflow journal progress on majority during confirmed isolation")
		}
		if nc.ConnectedUrl() == cluster.ClientURL(nodes[0]) {
			return event, fmt.Errorf("client moved onto isolated server")
		}
		proof := struct {
			ClientURL, IsolatedURL string
			Before, After          uint64
			ObservedAt             time.Time
		}{nc.ConnectedUrl(), cluster.ClientURL(nodes[0]), majorityBefore.State.LastSeq, after.State.LastSeq, time.Now().UTC()}
		data, err := json.MarshalIndent(proof, "", "  ")
		if err != nil {
			return event, err
		}
		if err = os.WriteFile(prefix+"-majority-progress.json", data, 0644); err != nil {
			return event, err
		}
		event.MajoritySequence = after.State.LastSeq
	}
	for _, node := range nodes {
		if err = cluster.ConnectNode(node); err != nil {
			return event, err
		}
	}
	disconnected = nil
	for node := 0; node < 5; node++ {
		routes, err := waitRouteCounts(bound, cluster, node, 16, 30*time.Second)
		if err != nil {
			return event, err
		}
		observations = append(observations, observation{"reconnected", node, routes, time.Now().UTC()})
	}
	if err = waitFiveReplicaReadiness(bound, js, 0); err != nil {
		return event, err
	}
	_, err = matrixReadMetadata(bound, func(attempt context.Context) (*jetstream.PubAck, error) {
		return js.Publish(attempt, "tier3.route.probe", []byte("after"))
	})
	if err != nil {
		return event, err
	}
	event.Healed = time.Now()
	if err = write(); err != nil {
		return event, err
	}
	return event, nil
}
