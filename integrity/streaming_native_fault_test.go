package integrity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func nativeStreamingScanner() retainedScanner {
	return scanByteBoundedThrough
}

func TestStreamingAuditNativeJournalFaults(t *testing.T) {
	if os.Getenv("WF_AUDIT_BATCH_CANDIDATE") != "1" {
		t.Skip("opt-in full streaming audit fault qualification")
	}
	for _, mode := range []struct {
		name                                            string
		snapshot, concurrent, compact, callback, direct bool
	}{
		{"state-false", false, false, false, false, false}, {"state-true", true, false, false, false, false}, {"concurrent-state", true, true, false, false, false}, {"compact-metadata", true, true, true, false, false}, {"callback-delivery", true, true, true, true, false}, {"direct-callback", true, true, true, false, true},
	} {
		snapshot := mode.snapshot
		read := nativeStreamingScanner()
		if mode.compact {
			read = scanCompactByteBoundedThrough
		}
		if mode.callback {
			read = scanConsumeByteBoundedThrough
		}
		if mode.direct {
			read = scanConsumeDirectWindowsThrough
		}
		for _, fault := range []string{"consumer-leader-loss", "cancellation"} {
			t.Run(fmt.Sprintf("%s/%s", mode.name, fault), func(t *testing.T) {
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
				// Cross both the record and production byte windows, keeping a
				// server-side tail after healthy client prefetch at the fault point.
				const invocations = 1500
				for i := 0; i < invocations; i++ {
					entries := batchAuditEntries()
					entries[1].Payload = json.RawMessage(strconv.Quote(strings.Repeat("p", 16<<10)))
					batchAuditPublish(t, ctx, js, fmt.Sprintf("fault-%06d", i), entries)
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
				report, err := checkUsingConcurrentOptions(auditCtx, js, nil, func(call context.Context, stream jetstream.Stream, cutoff *uint64, visit func(*jetstream.RawStreamMsg) error) error {
					if stream.CachedInfo().Config.Name != "WF_JRN" {
						return read(call, stream, cutoff, visit)
					}
					observed := &candidateObservedStream{Stream: stream}
					return read(call, observed, cutoff, func(msg *jetstream.RawStreamMsg) error {
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
				}, snapshot, true, mode.concurrent)
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
				t.Logf("streaming-fault concurrent_state=%v state_snapshot=%v fault=%s consumer=%s leader=%d pending_at_kill=%d consumer_replicas=%d visited=%d elapsed=%s report=%+v err=%v reconnects=%d", mode.concurrent, snapshot, fault, consumerName, leader, pending, replicas, visited, elapsed, report, err, nc.Stats().Reconnects)
			})
		}
	}
}

// A large remaining tail exercises transport recovery beyond the small control.
// Both complete reports must fit the original per-attempt budget.
func TestStreamingAuditNativeLargeJournalFault(t *testing.T) {
	if os.Getenv("WF_AUDIT_BATCH_CANDIDATE") != "1" {
		t.Skip("opt-in full streaming audit large fault qualification")
	}
	_, ctx, want, cluster := batchAuditLargeCohortWithServers(t, nativeAuditProfileCount(t))
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
	baselineCtx, baselineStop := context.WithTimeout(ctx, 20*time.Second)
	baseline, baselineErr := checkUsingOptions(baselineCtx, js, nil, nativeStreamingScanner(), true, true)
	baselineStop()
	if baselineErr != nil || baseline != want {
		t.Fatalf("baseline=%+v want=%+v err=%v", baseline, want, baselineErr)
	}
	attempt, stop := context.WithTimeout(ctx, 20*time.Second)
	defer stop()
	visited, leader, pending := 0, -1, uint64(0)
	consumerName := ""
	started := time.Now()
	report, err := checkUsingOptions(attempt, js, nil, func(call context.Context, stream jetstream.Stream, cutoff *uint64, visit func(*jetstream.RawStreamMsg) error) error {
		if stream.CachedInfo().Config.Name != "WF_JRN" {
			return nativeStreamingScanner()(call, stream, cutoff, visit)
		}
		observed := &candidateObservedStream{Stream: stream}
		return nativeStreamingScanner()(call, observed, cutoff, func(msg *jetstream.RawStreamMsg) error {
			visited++
			if visited == 128 {
				info, err := observed.consumer.Info(call)
				if err != nil {
					return err
				}
				if info.Cluster == nil || info.NumPending == 0 || info.Config.Replicas != 3 || !info.Config.MemoryStorage || info.Config.AckPolicy != jetstream.AckNonePolicy {
					return errors.New("missing active replicated consumer identity")
				}
				consumerName, pending = info.Name, info.NumPending
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
			return visit(msg)
		})
	}, true, true)
	elapsed := time.Since(started)
	t.Logf("streaming-large-fault consumer=%s leader=%d pending_at_kill=%d visited=%d elapsed=%s report=%+v err=%v reconnects=%d", consumerName, leader, pending, visited, elapsed, report, err, nc.Stats().Reconnects)
	if err != nil || report != want || visited != want.Entries || leader < 0 || pending == 0 || elapsed >= 20*time.Second {
		t.Fatalf("recovery report=%+v want=%+v visited=%d leader=%d pending=%d elapsed=%s err=%v", report, want, visited, leader, pending, elapsed, err)
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
}
