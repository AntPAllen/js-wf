//go:build linux

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

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/testcluster"
	"js-wf/worker"
)

// Characterize broker redelivery, separately from workflow completion. No
// latency or physical-drain gate in a workflow fixture is changed by this test.
func TestFiveContainerConsumerPendingClockTransition(t *testing.T) {
	if os.Getenv("WF_CONSUMER_CLOCK_CONTRACT") != "1" {
		t.Skip("set WF_CONSUMER_CLOCK_CONTRACT=1 for native pending-clock characterization")
	}
	for _, row := range []string{"server_clock_ahead", "server_clock_behind"} {
		t.Run(row, func(t *testing.T) {
			offset := matrixServerClockOffset(row)
			t.Setenv("WF_TIER3_SERVER_SKEW", "4:"+offset.String())
			root := t.TempDir()
			if base := os.Getenv("CONSUMER_CLOCK_CONTRACT_ROOT"); base != "" {
				root = filepath.Join(base, row)
			}
			cluster, err := testcluster.StartDockerCluster(root, 5)
			if err != nil {
				t.Fatal(err)
			}
			defer cluster.Close()
			write := func(name string, value any) {
				data, err := json.MarshalIndent(value, "", "  ")
				if err == nil {
					err = os.WriteFile(filepath.Join(root, name), data, 0644)
				}
				if err != nil {
					t.Error(err)
				}
			}
			defer func() {
				for node := 0; node < 5; node++ {
					if node == 4 {
						continue
					} // The killed container is already removed.
					logs, err := cluster.Logs(node)
					if err == nil {
						err = os.WriteFile(filepath.Join(root, fmt.Sprintf("server-%d.log", node)), []byte(logs), 0644)
					}
					if err != nil {
						t.Error(err)
					}
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			if _, err := waitRouteCounts(ctx, cluster, 0, 16, 30*time.Second); err != nil {
				t.Fatal(err)
			}
			nc, err := nats.Connect(cluster.ClientURL(0), nats.IgnoreDiscoveredServers(), nats.NoReconnect())
			if err != nil {
				t.Fatal(err)
			}
			defer nc.Close()
			js, err := jetstream.New(nc)
			if err != nil {
				t.Fatal(err)
			}
			clocks, err := observeMatrixServerClocks(ctx, cluster, row, "before", 0)
			write("physical-clocks.json", clocks)
			if err != nil {
				t.Fatal(err)
			}
			const streamName = "CLOCK_PENDING"
			var stream jetstream.Stream
			ready, stopReady := context.WithTimeout(ctx, 45*time.Second)
			for ready.Err() == nil {
				attempt, stop := context.WithTimeout(ready, 3*time.Second)
				stream, err = js.CreateOrUpdateStream(attempt, jetstream.StreamConfig{Name: streamName, Subjects: []string{"clock.pending.*"}, Replicas: 5, Storage: jetstream.FileStorage, Retention: jetstream.WorkQueuePolicy})
				stop()
				if err == nil {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			stopReady()
			if err != nil || stream == nil {
				t.Fatalf("create stream: %v", err)
			}
			var consumer jetstream.Consumer
			var before *jetstream.ConsumerInfo
			var subject string
			for candidate := 0; candidate < 64; candidate++ {
				subject = fmt.Sprintf("clock.pending.%d", candidate)
				attempt, stop := context.WithTimeout(ctx, 3*time.Second)
				consumer, err = stream.CreateConsumer(attempt, jetstream.ConsumerConfig{Name: fmt.Sprintf("PENDING_%02d", candidate), Durable: fmt.Sprintf("PENDING_%02d", candidate), FilterSubject: subject, Replicas: 5, AckPolicy: jetstream.AckExplicitPolicy, AckWait: worker.DefaultAckWait, MaxDeliver: -1, MaxAckPending: 16})
				if err == nil {
					before, err = consumer.Info(attempt)
				}
				stop()
				if err != nil {
					t.Fatal(err)
				}
				if before.Cluster != nil && before.Cluster.Leader == cluster.NodeName(4) {
					break
				}
			}
			if before == nil || before.Cluster == nil || before.Cluster.Leader != cluster.NodeName(4) {
				t.Fatal("no confirmed skewed consumer leader")
			}
			write("consumer-selected.json", before)
			type sample struct {
				Mode         string    `json:"mode"`
				Sequence     uint64    `json:"sequence"`
				Received     time.Time `json:"received"`
				StoredAt     time.Time `json:"stored_at"`
				ActionBefore time.Time `json:"action_before"`
				ActionAfter  time.Time `json:"action_after"`
				Redelivered  time.Time `json:"redelivered"`
				Deliveries   uint64    `json:"deliveries"`
			}
			var samples []*sample
			defer func() { write("samples.json", samples) }()
			for _, mode := range []string{"ack_wait", "progress", "nak", "confirmed_ack_control"} {
				attempt, stop := context.WithTimeout(ctx, 3*time.Second)
				_, err = js.Publish(attempt, subject, []byte(mode))
				stop()
				if err != nil {
					t.Fatal(err)
				}
				msg, err := consumer.Next(jetstream.FetchMaxWait(3 * time.Second))
				if err != nil {
					t.Fatal(err)
				}
				meta, err := msg.Metadata()
				if err != nil || meta.NumDelivered != 1 || string(msg.Data()) != mode {
					t.Fatalf("first delivery: %+v %v", meta, err)
				}
				entry := &sample{Mode: mode, Sequence: meta.Sequence.Stream, Received: time.Now().UTC(), StoredAt: meta.Timestamp}
				stored, err := stream.GetMsg(ctx, meta.Sequence.Stream)
				if err != nil || !stored.Time.Equal(meta.Timestamp) || string(stored.Data) != mode {
					t.Fatalf("stored timestamp provenance: %v", err)
				}
				samples = append(samples, entry)
				entry.ActionBefore = time.Now().UTC()
				attempt, stop = context.WithTimeout(ctx, 3*time.Second)
				switch mode {
				case "progress":
					_, err = nc.RequestWithContext(attempt, msg.Reply(), []byte("+WPI"))
				case "nak":
					// Require a processing reply; a plain NAK's local Publish
					// success cannot prove the leader processed the delay.
					_, err = nc.RequestWithContext(attempt, msg.Reply(), []byte(`-NAK {"delay":5000000000}`))
				case "confirmed_ack_control":
					err = msg.DoubleAck(attempt)
				}
				stop()
				entry.ActionAfter = time.Now().UTC()
				if err != nil {
					t.Fatal(err)
				}
			}
			// Admit the cut only while all three selected messages remain
			// pending on the skewed leader and all consumer replicas are current.
			admit, stopAdmit := context.WithTimeout(ctx, 3*time.Second)
			for admit.Err() == nil {
				before, err = consumer.Info(admit)
				current := err == nil && before.Cluster != nil && before.Cluster.Leader == cluster.NodeName(4) && before.NumAckPending == 3 && len(before.Cluster.Replicas) == 4
				if current {
					for _, replica := range before.Cluster.Replicas {
						current = current && replica.Current
					}
				}
				if current {
					break
				}
				time.Sleep(25 * time.Millisecond)
			}
			admitErr := admit.Err()
			stopAdmit()
			if err != nil || admitErr != nil {
				t.Fatalf("pending cut admission: %v %v", err, admitErr)
			}
			write("consumer-before-cut.json", before)
			if logs, err := cluster.Logs(4); err == nil {
				_ = os.WriteFile(filepath.Join(root, "server-4-before-kill.log"), []byte(logs), 0644)
			}
			killed := time.Now().UTC()
			if killed.Sub(samples[2].ActionAfter) >= 5*time.Second {
				t.Fatal("delayed NAK can already be due on its old leader")
			}
			if err := cluster.KillNode(4); err != nil {
				t.Fatal(err)
			}
			removed := time.Now().UTC()
			var replacement *jetstream.ConsumerInfo
			for ctx.Err() == nil {
				attempt, stop := context.WithTimeout(ctx, 2*time.Second)
				replacement, err = consumer.Info(attempt)
				stop()
				if err == nil && replacement.Cluster != nil && replacement.Cluster.Leader != "" && replacement.Cluster.Leader != cluster.NodeName(4) {
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
			if err != nil || ctx.Err() != nil {
				t.Fatalf("replacement: %v %v", err, ctx.Err())
			}
			replacementObserved := time.Now().UTC()
			write("handoff.json", struct {
				Killed, Removed, Observed time.Time
				Info                      *jetstream.ConsumerInfo
			}{killed, removed, replacementObserved, replacement})
			remaining := 3
			for remaining > 0 && ctx.Err() == nil {
				msg, err := consumer.Next(jetstream.FetchMaxWait(time.Second))
				if err != nil {
					if errors.Is(err, nats.ErrTimeout) || matrixTransientTransport(err) {
						continue
					}
					t.Fatal(err)
				}
				observed := time.Now().UTC()
				meta, err := msg.Metadata()
				if err != nil {
					t.Fatal(err)
				}
				var entry *sample
				for _, candidate := range samples {
					if candidate.Sequence == meta.Sequence.Stream {
						entry = candidate
					}
				}
				if entry == nil || entry.Mode == "confirmed_ack_control" || !entry.Redelivered.IsZero() || meta.NumDelivered != 2 {
					t.Fatalf("unexpected redelivery %+v", meta)
				}
				entry.Redelivered, entry.Deliveries = observed, meta.NumDelivered
				// deliverMsg persists the stored message timestamp, whereas
				// progressUpdate/processNak persist the consumer's wall clock.
				// The replacement restores that replicated pending state.
				lower, upper := entry.StoredAt.Add(worker.DefaultAckWait), entry.StoredAt.Add(worker.DefaultAckWait)
				if entry.Mode != "ack_wait" {
					wait := worker.DefaultAckWait
					if entry.Mode == "nak" {
						wait = 5 * time.Second
					}
					lower, upper = entry.ActionBefore.Add(offset+wait), entry.ActionAfter.Add(offset+wait)
				}
				if lower.Before(removed) {
					lower = removed
				}
				if upper.Before(replacementObserved) {
					upper = replacementObserved
				}
				if observed.Before(lower.Add(-2*time.Second)) || observed.After(upper.Add(5*time.Second)) {
					t.Errorf("%s redelivery=%s predicted bracket=%s..%s", entry.Mode, observed, lower, upper)
				}
				attempt, stop := context.WithTimeout(ctx, 3*time.Second)
				err = msg.DoubleAck(attempt)
				stop()
				if err != nil {
					t.Fatal(err)
				}
				remaining--
			}
			if remaining != 0 {
				t.Fatalf("missing redeliveries=%d: %v", remaining, ctx.Err())
			}
			// Consumer ACK and stream removal use separate Raft groups. Wait
			// for physical removal as well as the acknowledged consumer state.
			drain, stopDrain := context.WithTimeout(ctx, 10*time.Second)
			var final *jetstream.ConsumerInfo
			var state *jetstream.StreamInfo
			var stateErr error
			for drain.Err() == nil {
				final, err = consumer.Info(drain)
				state, stateErr = stream.Info(drain)
				if err == nil && stateErr == nil && final.NumAckPending == 0 && final.NumPending == 0 && state.State.Msgs == 0 {
					break
				}
				time.Sleep(25 * time.Millisecond)
			}
			drainErr := drain.Err()
			stopDrain()
			write("consumer-final.json", final)
			write("stream-final.json", state)
			if err != nil || stateErr != nil || drainErr != nil {
				t.Fatalf("physical drain info=%+v err=%v stream=%+v err=%v deadline=%v", final, err, state, stateErr, drainErr)
			}
			t.Logf("CONSUMER_CLOCK_CONTRACT offset=%s samples=3 confirmed_ack_control=true physical_drain=true", offset)
		})
	}
}
