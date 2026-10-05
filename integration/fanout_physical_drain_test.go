package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/provision"
	"js-wf/worker"
)

type fanoutDrainPeer struct {
	Queue     *jetstream.StreamInfo     `json:"queue"`
	Consumers []*jetstream.ConsumerInfo `json:"consumers"`
}

// Drain only after the original parent/child workers have been stopped and
// joined. These are production deliveries and acknowledgments, never purges.
// Both the extra 30-second bound and every read stay inside the original case.
func drainFanoutTerminalWakeups(t *testing.T, ctx context.Context, all []jetstream.JetStream, handlers map[string]worker.Handler, root string) {
	t.Helper()
	bound, stop := context.WithTimeout(ctx, 30*time.Second)
	defer stop()
	w, err := worker.New(bound, all[1], "fanout-terminal-drain", handlers)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(bound)
	done := make(chan error, provision.Partitions)
	for part := uint32(0); part < provision.Partitions; part++ {
		go func(part uint32) { done <- w.RunPartition(runCtx, part) }(part)
	}
	joined := false
	join := func() {
		if joined {
			return
		}
		joined = true
		cancel()
		for part := uint32(0); part < provision.Partitions; part++ {
			if err := <-done; err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("terminal drain worker: %v", err)
			}
		}
	}
	defer join()
	began := time.Now()
	var peers []fanoutDrainPeer
	var lastErr error
	for bound.Err() == nil {
		peers, lastErr = observeFanoutPhysicalDrain(bound, all)
		if lastErr == nil {
			break
		}
		select {
		case <-bound.Done():
		case <-time.After(50 * time.Millisecond):
		}
	}
	if lastErr != nil || bound.Err() != nil {
		diagnosis, err := json.MarshalIndent(map[string]any{"all_observed_peers": peers, "last_error": fmt.Sprint(lastErr), "deadline_error": fmt.Sprint(bound.Err()), "elapsed_ns": time.Since(began).Nanoseconds()}, "", "  ")
		if err == nil {
			err = os.WriteFile(filepath.Join(root, "physical-drain-failure.json"), append(diagnosis, '\n'), 0600)
		}
		if err != nil {
			t.Errorf("retain drain failure: %v", err)
		}
		t.Fatalf("fanout physical drain: %v deadline=%v", lastErr, bound.Err())
	}
	join()
	// The persisted witness is after every producer/worker is joined.
	peers, err = observeFanoutPhysicalDrain(bound, all)
	if err != nil {
		t.Fatalf("fanout drain changed after worker joins: %v", err)
	}
	data, err := json.MarshalIndent(struct {
		ElapsedNS     int64             `json:"elapsed_ns"`
		Peers         []fanoutDrainPeer `json:"all_three_peers"`
		WorkersJoined bool              `json:"workers_joined"`
	}{time.Since(began).Nanoseconds(), peers, joined}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "physical-drain.json"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("FANOUT_PHYSICAL_DRAIN peers=3 stream_messages=0 consumers=64 pending=0 ack_pending=0 workers_joined=64")
}

func observeFanoutPhysicalDrain(ctx context.Context, all []jetstream.JetStream) ([]fanoutDrainPeer, error) {
	peers := make([]fanoutDrainPeer, 0, len(all))
	if len(all) != 3 {
		return nil, fmt.Errorf("expected three clients")
	}
	for index, js := range all {
		attempt, stop := context.WithTimeout(ctx, time.Second)
		stream, err := js.Stream(attempt, "WF_RUN")
		var info *jetstream.StreamInfo
		if err == nil {
			info, err = stream.Info(attempt)
		}
		stop()
		if err != nil {
			return peers, err
		}
		peers = append(peers, fanoutDrainPeer{Queue: info})
		if info.State.Msgs != 0 || info.State.Consumers != int(provision.Partitions) {
			return peers, fmt.Errorf("peer%d queue messages=%d consumers=%d", index, info.State.Msgs, info.State.Consumers)
		}
		if info.Cluster == nil || info.Cluster.Leader == "" || len(info.Cluster.Replicas) != 2 {
			return peers, fmt.Errorf("peer%d queue has no current R3 leader", index)
		}
		for _, replica := range info.Cluster.Replicas {
			if !replica.Current || replica.Offline {
				return peers, fmt.Errorf("peer%d replica not current", index)
			}
		}
		peer := fanoutDrainPeer{Queue: info}
		attempt, stop = context.WithTimeout(ctx, time.Second)
		list := stream.ListConsumers(attempt)
		names := map[string]bool{}
		for consumer := range list.Info() {
			if names[consumer.Name] || consumer.NumPending != 0 || consumer.NumAckPending != 0 {
				stop()
				return peers, fmt.Errorf("peer%d duplicate/pending durable %s pending=%d ack=%d", index, consumer.Name, consumer.NumPending, consumer.NumAckPending)
			}
			names[consumer.Name] = true
			peer.Consumers = append(peer.Consumers, consumer)
		}
		err = list.Err()
		stop()
		if err != nil {
			return peers, err
		}
		if len(names) != int(provision.Partitions) {
			return peers, fmt.Errorf("peer%d durable census=%d", index, len(names))
		}
		for part := uint32(0); part < provision.Partitions; part++ {
			if !names[fmt.Sprintf("WF_P_%02d", part)] {
				return peers, fmt.Errorf("peer%d missing partition%d", index, part)
			}
		}
		peers[len(peers)-1] = peer
	}
	if ctx.Err() != nil {
		return peers, ctx.Err()
	}
	return peers, nil
}
