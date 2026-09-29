//go:build linux

package integration_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/testcluster"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// TestFiveContainerAckedPublishesSurviveLeaderKill checks acknowledged file
// stream writes while the five-replica leader is SIGKILLed and later restarted.
func TestFiveContainerAckedPublishesSurviveLeaderKill(t *testing.T) {
	if os.Getenv("WF_TIER3_CONTAINER") != "1" {
		t.Skip("set WF_TIER3_CONTAINER=1 for five-container NATS proof")
	}
	seed, err := testcluster.SeedFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	const writers, perWriter = 4, 250
	killAt := 100 + rand.New(rand.NewSource(seed)).Intn(300)
	t.Logf("FAULT_SEED=%d kill_after_attempt=%d", seed, killAt)
	cluster, err := testcluster.StartDockerCluster(t.TempDir(), 5)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	if _, err := waitRouteCounts(ctx, cluster, 0, 16, 30*time.Second); err != nil {
		t.Fatalf("five-node routes: %v", err)
	}
	clients := make([]*nats.Conn, 5)
	js := make([]jetstream.JetStream, 5)
	for i := range js {
		clients[i], err = nats.Connect(cluster.ClientURL(i), nats.NoReconnect(), nats.IgnoreDiscoveredServers())
		if err != nil {
			t.Fatal(err)
		}
		defer clients[i].Close()
		js[i], err = jetstream.New(clients[i])
		if err != nil {
			t.Fatal(err)
		}
	}
	config := jetstream.StreamConfig{Name: "TIER3_KILL", Subjects: []string{"tier3.kill"}, Storage: jetstream.FileStorage, Replicas: 5, Discard: jetstream.DiscardNew}
	var stream jetstream.Stream
	for until := time.Now().Add(60 * time.Second); time.Now().Before(until) && ctx.Err() == nil; {
		attempt, stop := context.WithTimeout(ctx, 5*time.Second)
		stream, err = js[0].CreateStream(attempt, config)
		stop()
		if err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("create five-replica file stream: %v", err)
	}
	leader := -1
	for until := time.Now().Add(60 * time.Second); time.Now().Before(until) && ctx.Err() == nil; {
		attempt, stop := context.WithTimeout(ctx, 3*time.Second)
		info, infoErr := stream.Info(attempt)
		stop()
		if infoErr == nil && info.Cluster != nil && info.Cluster.Leader != "" && len(info.Cluster.Replicas) == 4 {
			current := true
			for _, replica := range info.Cluster.Replicas {
				if replica == nil || !replica.Current || replica.Offline {
					current = false
				}
			}
			if current {
				for i := 0; i < 5; i++ {
					if cluster.NodeName(i) == info.Cluster.Leader {
						leader = i
					}
				}
				break
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	if leader < 0 {
		t.Fatal("five-replica stream did not acquire a current leader")
	}
	publisher := (leader + 1) % 5
	acked := map[uint64]string{}
	var ackMu sync.Mutex
	var attempts atomic.Int64
	var killed atomic.Bool
	var ackedAfter atomic.Int64
	killNow := make(chan struct{})
	killDone := make(chan error, 1)
	go func() {
		<-killNow
		err := cluster.KillNode(leader)
		if err == nil {
			killed.Store(true)
		}
		killDone <- err
	}()
	var publishers sync.WaitGroup
	conflicts := make(chan error, 1)
	for writer := 0; writer < writers; writer++ {
		publishers.Add(1)
		go func(writer int) {
			defer publishers.Done()
			for n := 0; n < perWriter; n++ {
				if attempts.Add(1) == int64(killAt) {
					close(killNow)
				}
				payload := fmt.Sprintf("%04d", writer*perWriter+n)
				attempt, stop := context.WithTimeout(ctx, 500*time.Millisecond)
				ack, err := js[publisher].Publish(attempt, "tier3.kill", []byte(payload))
				stop()
				if err != nil {
					continue // a publish without an ack has an unknown outcome
				}
				if killed.Load() {
					ackedAfter.Add(1)
				}
				ackMu.Lock()
				if prior, exists := acked[ack.Sequence]; exists && prior != payload {
					select {
					case conflicts <- fmt.Errorf("sequence %d acknowledged as %q and %q", ack.Sequence, prior, payload):
					default:
					}
				}
				acked[ack.Sequence] = payload
				ackMu.Unlock()
			}
		}(writer)
	}
	publishers.Wait()
	if err := <-killDone; err != nil {
		t.Fatalf("kill leader node %d: %v", leader, err)
	}
	select {
	case err := <-conflicts:
		t.Fatal(err)
	default:
	}
	if len(acked) == 0 || ackedAfter.Load() == 0 {
		t.Fatalf("acknowledged=%d after kill=%d", len(acked), ackedAfter.Load())
	}
	var survivor jetstream.Stream
	for until := time.Now().Add(90 * time.Second); time.Now().Before(until) && ctx.Err() == nil; {
		attempt, stop := context.WithTimeout(ctx, 30*time.Second)
		survivor, err = js[publisher].Stream(attempt, config.Name)
		if err == nil {
			err = auditPlainStream(attempt, survivor, acked)
		}
		stop()
		if err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("surviving node lost acknowledged writes: %v", err)
	}
	if err := cluster.RestartNode(leader); err != nil {
		t.Fatal(err)
	}
	restartedConn, err := nats.Connect(cluster.ClientURL(leader), nats.NoReconnect(), nats.IgnoreDiscoveredServers())
	if err != nil {
		t.Fatal(err)
	}
	defer restartedConn.Close()
	restartedJS, err := jetstream.New(restartedConn)
	if err != nil {
		t.Fatal(err)
	}
	for until := time.Now().Add(90 * time.Second); time.Now().Before(until) && ctx.Err() == nil; {
		attempt, stop := context.WithTimeout(ctx, 30*time.Second)
		recovered, getErr := restartedJS.Stream(attempt, config.Name)
		if getErr == nil {
			getErr = auditPlainStream(attempt, recovered, acked)
		}
		stop()
		if getErr == nil {
			t.Logf("five-container leader kill: leader=%d acked=%d after_kill=%d", leader, len(acked), ackedAfter.Load())
			return
		}
		err = getErr
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("restarted leader did not retain acknowledged writes: %v", err)
}
