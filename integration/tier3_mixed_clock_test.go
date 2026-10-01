//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/testcluster"
)

// Clock observations come from the actual server monitoring endpoint, bracketed
// by the unshifted controller's clock. Configuration alone is not skew proof.
type matrixServerClockObservation struct {
	Stage            string    `json:"stage"`
	Fault            int       `json:"fault"`
	Node             int       `json:"node"`
	HostBefore       time.Time `json:"host_before"`
	ServerNow        time.Time `json:"server_now"`
	HostAfter        time.Time `json:"host_after"`
	ExpectedOffsetNS int64     `json:"expected_offset_ns"`
}

func matrixServerClockOffset(row string) time.Duration {
	switch row {
	case "server_clock_ahead":
		return time.Minute
	case "server_clock_behind":
		return -time.Minute
	default:
		return 0
	}
}

func observeMatrixServerClocks(ctx context.Context, cluster *testcluster.DockerCluster, row, stage string, fault int) ([]matrixServerClockObservation, error) {
	offset := matrixServerClockOffset(row)
	if offset == 0 {
		return nil, fmt.Errorf("not a server clock row: %s", row)
	}
	var records []matrixServerClockObservation
	for node := 0; node < 5; node++ {
		want := time.Duration(0)
		if node == 4 {
			want = offset
		}
		attempt, stop := context.WithTimeout(ctx, 2*time.Second)
		before := time.Now().UTC()
		serverNow, err := cluster.ServerNow(attempt, node)
		after := time.Now().UTC()
		stop()
		if err != nil {
			return records, fmt.Errorf("%s fault%d node%d clock: %w", stage, fault, node, err)
		}
		records = append(records, matrixServerClockObservation{stage, fault, node, before, serverNow, after, int64(want)})
		if after.Sub(before) > 2*time.Second || serverNow.Before(before.Add(want-2*time.Second)) || serverNow.After(after.Add(want+2*time.Second)) {
			return records, fmt.Errorf("%s fault%d node%d server clock=%s host interval=%s..%s expected offset=%s", stage, fault, node, serverNow, before, after, want)
		}
	}
	return records, nil
}

type matrixClockRoleObservation struct {
	Stage    string                `json:"stage"`
	Fault    int                   `json:"fault"`
	Stream   string                `json:"stream"`
	Observed time.Time             `json:"observed"`
	Info     *jetstream.StreamInfo `json:"info"`
}

func observeMatrixClockRoles(ctx context.Context, js jetstream.JetStream, stage string, fault int, expected func(string) bool) ([]matrixClockRoleObservation, error) {
	var records []matrixClockRoleObservation
	for _, name := range []string{"WF_RUN", "WF_JRN"} {
		info, err := waitMatrixClockRole(ctx, func(attempt context.Context) (*jetstream.StreamInfo, error) {
			stream, err := js.Stream(attempt, name)
			if err != nil {
				return nil, err
			}
			return stream.Info(attempt)
		}, expected)
		if err != nil {
			return records, fmt.Errorf("clock role %s %s: %w", stage, name, err)
		}
		records = append(records, matrixClockRoleObservation{stage, fault, name, time.Now().UTC(), info})
	}
	return records, nil
}

// Election observation uses the fault's deadline. The ordinary metadata helper's
// three-attempt cap can expire while a killed leader's replacement is still being
// elected. Permanent errors still fail immediately; no latency gate is changed.
func waitMatrixClockRole(ctx context.Context, lookup func(context.Context) (*jetstream.StreamInfo, error), expected func(string) bool) (*jetstream.StreamInfo, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		attempt, cancel := context.WithTimeout(ctx, 2*time.Second)
		info, err := lookup(attempt)
		cancel()
		if err != nil && !matrixTransientTransport(err) {
			return nil, err
		}
		if err == nil && info != nil && info.Cluster != nil && expected(info.Cluster.Leader) {
			return info, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func preferMatrixClockLeaders(ctx context.Context, nc *nats.Conn, js jetstream.JetStream, cluster *testcluster.DockerCluster, stage string, fault int) ([]matrixClockRoleObservation, error) {
	bound, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for _, name := range []string{"WF_RUN", "WF_JRN"} {
		stream, err := js.Stream(bound, name)
		if err != nil {
			return nil, err
		}
		info, err := stream.Info(bound)
		if err != nil {
			return nil, err
		}
		if info.Cluster != nil && info.Cluster.Leader == cluster.NodeName(4) {
			continue
		}
		request, _ := json.Marshal(map[string]any{"placement": map[string]string{"preferred": cluster.NodeName(4)}})
		reply, err := nc.RequestWithContext(bound, "$JS.API.STREAM.LEADER.STEPDOWN."+name, request)
		if err != nil {
			return nil, err
		}
		var response struct {
			Success bool            `json:"success"`
			Error   json.RawMessage `json:"error"`
		}
		if err := json.Unmarshal(reply.Data, &response); err != nil || !response.Success {
			return nil, fmt.Errorf("clock role stepdown %s response=%s err=%v", name, reply.Data, err)
		}
	}
	return observeMatrixClockRoles(bound, js, stage, fault, func(leader string) bool { return leader == cluster.NodeName(4) })
}

func killFiveContainerMixedClockLeader(ctx context.Context, js jetstream.JetStream, cluster *testcluster.DockerCluster, scheduled time.Time, prefix string, fault int, record func([]matrixClockRoleObservation)) (matrixLeaderFault, error) {
	event := matrixLeaderFault{Scheduled: scheduled, Node: 4}
	bound, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	stream, err := js.Stream(bound, "WF_JRN")
	if err != nil {
		return event, err
	}
	info, err := stream.Info(bound)
	if err != nil || info.Cluster == nil || info.Cluster.Leader != cluster.NodeName(4) {
		return event, fmt.Errorf("skewed journal leader admission lost: %v", err)
	}
	data, err := json.MarshalIndent(info, "", "  ")
	if err == nil {
		err = os.WriteFile(prefix+"-journal-before.json", data, 0644)
	}
	if err != nil {
		return event, err
	}
	event.Killed = time.Now().UTC()
	if err := cluster.KillNode(4); err != nil {
		return event, err
	}
	removed := time.Now().UTC()
	// Keep the skewed server down until both roles have actually changed to an
	// unshifted peer; an immediate restart could simply re-elect the same clock.
	replacement, err := observeMatrixClockRoles(bound, js, "replacement", fault, func(leader string) bool { return leader != "" && leader != cluster.NodeName(4) })
	record(replacement)
	if err != nil {
		return event, err
	}
	if err := cluster.RestartNode(4); err != nil {
		return event, err
	}
	restarted := time.Now().UTC()
	operations := []struct {
		Node   int       `json:"node"`
		Action string    `json:"action"`
		At     time.Time `json:"at"`
	}{{4, "sigkill_removed", removed}, {4, "restarted", restarted}}
	data, err = json.MarshalIndent(operations, "", "  ")
	if err == nil {
		err = os.WriteFile(prefix+"-journal-operations.json", data, 0644)
	}
	if err != nil {
		return event, err
	}
	if err := waitFiveReplicaReadiness(bound, js, 0); err != nil {
		return event, err
	}
	after, err := observeMatrixClockRoles(bound, js, "after", fault, func(leader string) bool { return leader != "" && leader != cluster.NodeName(4) })
	record(after)
	if err != nil {
		return event, err
	}
	event.Healed = time.Now().UTC()
	return event, nil
}
