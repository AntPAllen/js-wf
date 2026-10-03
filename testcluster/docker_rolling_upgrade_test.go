//go:build linux

package testcluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestFiveDockerRollingUpgradePreservesEveryReplica(t *testing.T) {
	if os.Getenv("WF_DOCKER_ROLLING_UPGRADE") != "1" {
		t.Skip("set WF_DOCKER_ROLLING_UPGRADE=1 with an actual NATS2.11.17 executable")
	}
	root := os.Getenv("WF_DOCKER_UPGRADE_ARTIFACT_ROOT")
	if root == "" {
		root = t.TempDir()
	}
	if err := os.MkdirAll(root, 0755); err != nil {
		t.Fatal(err)
	}
	cluster, err := StartRollingUpgradeDockerCluster(filepath.Join(root, "cluster"), 5, os.Getenv("WF_NATS_SERVER_BIN"))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	defer func() {
		for node := 0; node < 5; node++ {
			logs, err := cluster.Logs(node)
			if err != nil {
				t.Error(err)
				continue
			}
			if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("server-%d.log", node)), []byte(logs), 0644); err != nil {
				t.Error(err)
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	nc, err := nats.Connect(cluster.ClientURL(0), nats.MaxReconnects(-1), nats.ReconnectWait(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	var stream jetstream.Stream
	ready, stopReady := context.WithTimeout(ctx, 45*time.Second)
	for ready.Err() == nil {
		stream, err = js.CreateStream(ready, jetstream.StreamConfig{Name: "ROLLING_PROOF", Subjects: []string{"rolling.proof"}, Storage: jetstream.FileStorage, Replicas: 5})
		if err == nil {
			break
		}
		var api *jetstream.APIError
		if !errors.As(err, &api) || api.ErrorCode != 10005 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	stopReady()
	if err != nil {
		t.Fatal(err)
	}
	var payloads [][]byte
	publish := func() {
		data := []byte(fmt.Sprintf("retained-upgrade-message-%d", len(payloads)+1))
		// A ready client listener can precede the stream's elected leader.
		// Keep a stable message ID across ambiguous replies, then require the
		// exact next sequence so a duplicate append cannot pass this contract.
		deadline, stop := context.WithTimeout(ctx, time.Minute)
		defer stop()
		var ack *jetstream.PubAck
		var err error
		for attempt := 1; deadline.Err() == nil; attempt++ {
			request, cancel := context.WithTimeout(deadline, 2*time.Second)
			ack, err = js.Publish(request, "rolling.proof", data, jetstream.WithMsgID(string(data)))
			cancel()
			if err == nil {
				break
			}
			t.Logf("publish sequence=%d attempt=%d error=%v", len(payloads)+1, attempt, err)
			if !errors.Is(err, jetstream.ErrNoStreamResponse) && !errors.Is(err, nats.ErrNoResponders) && !errors.Is(err, nats.ErrTimeout) && !errors.Is(err, context.DeadlineExceeded) {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if err != nil || ack == nil || ack.Sequence != uint64(len(payloads)+1) {
			t.Fatalf("publish sequence=%d ack=%+v err=%v deadline=%v", len(payloads)+1, ack, err, deadline.Err())
		}
		payloads = append(payloads, data)
	}
	for i := 0; i < 32; i++ {
		publish()
	}
	upgraded := make([]bool, 5)
	versions := map[string][]string{}
	check := func(stage string) {
		deadline, stop := context.WithTimeout(ctx, time.Minute)
		defer stop()
		var lastErr error
		for attempt := 1; deadline.Err() == nil; attempt++ {
			observed := make([]string, 5)
			ids := map[string]bool{}
			lastErr = nil
			for node := 0; node < 5; node++ {
				peer, err := nats.Connect(cluster.ClientURL(node), nats.NoReconnect(), nats.IgnoreDiscoveredServers(), nats.Timeout(time.Second))
				if err != nil {
					lastErr = err
					break
				}
				observed[node] = peer.ConnectedServerVersion()
				id := peer.ConnectedServerId()
				peer.Close()
				want := "2.11.17"
				if upgraded[node] {
					want = server.VERSION
				}
				if observed[node] != want || id == "" || ids[id] {
					t.Fatalf("stage=%s node%d version=%s want=%s identity=%s", stage, node, observed[node], want, id)
				}
				ids[id] = true
				raw, err := cluster.Diagnostic(deadline, node, "jetstream")
				if err != nil {
					lastErr = err
					break
				}
				if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("%s-attempt-%d-node-%d.json", stage, attempt, node)), raw, 0644); err != nil {
					t.Fatal(err)
				}
				var snapshot struct {
					Server   string `json:"server_id"`
					Accounts []struct {
						Streams []struct {
							Name  string `json:"name"`
							State struct {
								Messages *uint64 `json:"messages"`
								Last     *uint64 `json:"last_seq"`
							} `json:"state"`
						} `json:"stream_detail"`
					} `json:"account_details"`
				}
				if err := json.Unmarshal(raw, &snapshot); err != nil {
					t.Fatal(err)
				}
				found := 0
				for _, account := range snapshot.Accounts {
					for _, state := range account.Streams {
						if state.Name != "ROLLING_PROOF" {
							continue
						}
						found++
						if state.State.Messages == nil || state.State.Last == nil || *state.State.Messages != uint64(len(payloads)) || *state.State.Last != uint64(len(payloads)) {
							lastErr = fmt.Errorf("node%d retained count/last mismatch", node)
						}
					}
				}
				if found != 1 || snapshot.Server != id {
					lastErr = fmt.Errorf("node%d missing/duplicate stream or monitoring identity mismatch", node)
				}
				if lastErr != nil {
					break
				}
			}
			if lastErr == nil {
				versions[stage] = observed
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if lastErr != nil || deadline.Err() != nil {
			t.Fatalf("stage=%s err=%v deadline=%v", stage, lastErr, deadline.Err())
		}
		for index, want := range payloads {
			message, err := stream.GetMsg(ctx, uint64(index+1))
			if err != nil || message == nil || string(message.Data) != string(want) {
				t.Fatalf("stage=%s sequence=%d err=%v", stage, index+1, err)
			}
		}
	}
	check("initial")
	order := []int{2, 0, 4, 1, 3}
	for cut, node := range order {
		if err := cluster.UpgradeNode(node); err != nil {
			t.Fatal(err)
		}
		upgraded[node] = true
		publish()
		check(fmt.Sprintf("upgrade-%d", cut+1))
	}
	data, err := json.MarshalIndent(struct {
		Order    []int               `json:"order"`
		Versions map[string][]string `json:"versions"`
		Messages int                 `json:"retained_messages"`
		Replicas int                 `json:"replicas"`
	}{order, versions, len(payloads), 5}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "result.json"), data, 0644); err != nil {
		t.Fatal(err)
	}
	t.Logf("ROLLING_DOCKER_CONTRACT old=2.11.17 current=%s upgrades=5 retained=%d physical_replicas=5", server.VERSION, len(payloads))
}
