package integrity

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestStreamingAuditNativeJournalFaults(t *testing.T) {
	if os.Getenv("WF_AUDIT_BATCH_CANDIDATE") != "1" {
		t.Skip("opt-in full streaming audit fault qualification")
	}
	for _, snapshot := range []bool{false, true} {
		for _, fault := range []string{"consumer-leader-loss", "cancellation"} {
			t.Run(fmt.Sprintf("state-%v/%s", snapshot, fault), func(t *testing.T) {
				_, ctx, cluster := batchedAuditClusterWithServers(t)
				var urls []string
				for _, server := range cluster.Servers {
					urls = append(urls, server.ClientURL())
				}
				nc, err := nats.Connect(strings.Join(urls, ","), nats.MaxReconnects(-1), nats.ReconnectWait(20*time.Millisecond))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(nc.Close)
				js, err := jetstream.New(nc)
				if err != nil {
					t.Fatal(err)
				}
				const invocations = 500
				for i := 0; i < invocations; i++ {
					batchAuditPublish(t, ctx, js, fmt.Sprintf("fault-%06d", i), batchAuditEntries())
				}
				want := Report{Invocations: invocations, Journals: invocations, Entries: 4 * invocations, Terminal: invocations}
				baseline, err := CheckWithBatchedReads(ctx, js)
				if err != nil || baseline != want {
					t.Fatalf("baseline=%+v err=%v", baseline, err)
				}
				auditCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
				defer cancel()
				visited, leader, pending, replicas := 0, -1, uint64(0), 0
				consumerName := ""
				started := time.Now()
				report, err := checkUsingOptions(auditCtx, js, nil, func(call context.Context, stream jetstream.Stream, cutoff *uint64, visit func(*jetstream.RawStreamMsg) error) error {
					if stream.CachedInfo().Config.Name != "WF_JRN" {
						return scanBatchThrough(call, stream, cutoff, visit)
					}
					observed := &candidateObservedStream{Stream: stream}
					return scanBatchThrough(call, observed, cutoff, func(msg *jetstream.RawStreamMsg) error {
						visited++
						if visited == 128 {
							if fault == "cancellation" {
								cancel()
							} else {
								info, err := observed.consumer.Info(call)
								if err != nil {
									return err
								}
								if info.Cluster == nil || info.NumPending == 0 || info.Config.Replicas != 3 || !info.Config.MemoryStorage || info.Config.AckPolicy != jetstream.AckNonePolicy {
									return errors.New("missing active replicated consumer identity")
								}
								consumerName, pending, replicas = info.Name, info.NumPending, info.Config.Replicas
								for i, server := range cluster.Servers {
									if server.Name() == info.Cluster.Leader {
										leader = i
									}
								}
								if leader < 0 {
									return errors.New("consumer leader not found")
								}
								cluster.KillNode(leader)
							}
						}
						return visit(msg)
					})
				}, snapshot, true)
				elapsed := time.Since(started)
				if elapsed >= 20*time.Second {
					t.Fatalf("original budget exceeded: %s", elapsed)
				}
				if fault == "cancellation" {
					if !errors.Is(err, context.Canceled) || visited != 128 || report != (Report{Invocations: invocations}) {
						t.Fatalf("cancel report=%+v visited=%d err=%v", report, visited, err)
					}
				} else if err != nil || report != want || visited != 4*invocations || leader < 0 || pending == 0 {
					t.Fatalf("recovery report=%+v visited=%d leader=%d pending=%d err=%v", report, visited, leader, pending, err)
				}
				for _, name := range []string{"WF_INV", "WF_JRN"} {
					stream, err := js.Stream(ctx, name)
					if err != nil {
						t.Fatal(err)
					}
					info, err := stream.Info(ctx)
					if err != nil || info.State.Consumers != 0 {
						t.Fatalf("cleanup stream=%s info=%+v err=%v", name, info, err)
					}
				}
				t.Logf("streaming-fault state_snapshot=%v fault=%s consumer=%s leader=%d pending_at_kill=%d consumer_replicas=%d visited=%d elapsed=%s report=%+v err=%v reconnects=%d", snapshot, fault, consumerName, leader, pending, replicas, visited, elapsed, report, err, nc.Stats().Reconnects)
			})
		}
	}
}
