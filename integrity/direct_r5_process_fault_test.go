//go:build linux

package integrity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/provision"
	"js-wf/testcluster"
)

// Record actual API consumer identities and positions when the scanner asks
// for replay proof. This supplies evidence without changing replay admission.
type processObservedConsumer struct {
	jetstream.Consumer
	record func(*jetstream.ConsumerInfo, error)
}

func (c processObservedConsumer) Info(ctx context.Context) (*jetstream.ConsumerInfo, error) {
	info, err := c.Consumer.Info(ctx)
	c.record(info, err)
	return info, err
}

type processDeleteObservation struct {
	Name  string    `json:"name"`
	Error string    `json:"error"`
	At    time.Time `json:"at"`
}
type processObservedStream struct {
	*candidateObservedStream
	record  func(*jetstream.ConsumerInfo, error)
	deleted func(processDeleteObservation)
}

func (s processObservedStream) DeleteConsumer(ctx context.Context, name string) error {
	err := s.Stream.DeleteConsumer(ctx, name)
	observation := processDeleteObservation{Name: name, At: time.Now().UTC()}
	if err != nil {
		observation.Error = err.Error()
	}
	s.deleted(observation)
	return err
}

func (s processObservedStream) CreateConsumer(ctx context.Context, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	c, err := s.candidateObservedStream.CreateConsumer(ctx, cfg)
	if err != nil {
		s.record(nil, err)
		return nil, err
	}
	s.record(c.CachedInfo(), nil)
	observed := processObservedConsumer{Consumer: c, record: s.record}
	s.consumer = observed
	return observed, nil
}

// Fresh disposable stores only. The fault targets the actual R1 cursor owner,
// rather than inferring ownership from the R5 source stream's leader.
func TestDirectR1AuditR5ProcessOwnerLoss(t *testing.T) {
	if os.Getenv("WF_AUDIT_R5_PROCESS_FAULT") != "1" {
		t.Skip("opt-in five-container R1 audit cursor process SIGKILL")
	}
	for _, restart := range []bool{false, true} {
		t.Run(fmt.Sprintf("restart-%v", restart), func(t *testing.T) {
			root := candidateNativeRoot(t)
			cluster, err := testcluster.StartDockerCluster(filepath.Join(root, "cluster"), 5)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(cluster.Close)
			killed := -1
			t.Cleanup(func() {
				for n := 0; n < 5; n++ {
					logs, e := cluster.Logs(n)
					if e != nil {
						// KillNodeObserved removes the container; its pre-kill log is retained.
						if n == killed && !restart {
							continue
						}
						t.Error(e)
						continue
					}
					if e := os.WriteFile(filepath.Join(root, fmt.Sprintf("server-%d.log", n)), []byte(logs), 0600); e != nil {
						t.Error(e)
					}
				}
			})
			var urls []string
			for n := 0; n < 5; n++ {
				urls = append(urls, cluster.ClientURL(n))
			}
			nc, err := nats.Connect(strings.Join(urls, ","), nats.IgnoreDiscoveredServers(), nats.MaxReconnects(-1), nats.ReconnectWait(20*time.Millisecond), nats.Timeout(time.Second))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(nc.Close)
			js, err := jetstream.New(nc)
			if err != nil {
				t.Fatal(err)
			}
			ctx, stop := context.WithTimeout(context.Background(), 5*time.Minute)
			defer stop()
			for {
				call, cancel := context.WithTimeout(ctx, 3*time.Second)
				err = provision.Ensure(call, js, 5)
				cancel()
				if err == nil {
					break
				}
				if ctx.Err() != nil {
					t.Fatal(err)
				}
				time.Sleep(50 * time.Millisecond)
			}
			const count = 1500
			for i := 0; i < count; i++ {
				entries := batchAuditEntries()
				entries[1].Payload = json.RawMessage(strconv.Quote(strings.Repeat("p", 16<<10)))
				batchAuditPublish(t, ctx, js, fmt.Sprintf("process-%06d", i), entries)
			}
			want := Report{Invocations: count, Journals: count, Entries: count * 4, Terminal: count}
			for _, name := range []string{"WF_INV", "WF_JRN", "KV_WF_STATE"} {
				stream, e := js.Stream(ctx, name)
				if e != nil {
					t.Fatal(e)
				}
				info := stream.CachedInfo()
				if info.Config.Replicas != 5 || info.Config.Storage != jetstream.FileStorage || info.Cluster == nil || len(info.Cluster.Replicas) != 4 {
					t.Fatalf("expected R5 file source %s: %+v", name, info)
				}
			}
			baselineCtx, baselineStop := context.WithTimeout(ctx, 20*time.Second)
			baseline, err := CheckWithBatchedReads(baselineCtx, js)
			baselineStop()
			if err != nil || baseline != want {
				t.Fatalf("baseline=%+v err=%v", baseline, err)
			}
			attempt, cancel := context.WithTimeout(ctx, 20*time.Second)
			defer cancel()
			visited := 0
			var target *jetstream.ConsumerInfo
			var kill testcluster.DockerKillObservation
			started := time.Now()
			report, failure := checkUsingConcurrentOptions(attempt, js, nil, func(call context.Context, stream jetstream.Stream, cutoff *uint64, visit func(*jetstream.RawStreamMsg) error) error {
				observed := &candidateObservedStream{Stream: stream, cursorReplicas: 1}
				var deleteMu sync.Mutex
				var deletions []processDeleteObservation
				deleted := func(observation processDeleteObservation) {
					deleteMu.Lock()
					defer deleteMu.Unlock()
					deletions = append(deletions, observation)
					t.Logf("cursor deletion stream=%s observation=%+v", stream.CachedInfo().Config.Name, observation)
				}
				defer func() {
					data, e := json.MarshalIndent(deletions, "", "  ")
					if e != nil {
						t.Error(e)
						return
					}
					if e = os.WriteFile(filepath.Join(root, stream.CachedInfo().Config.Name+"-delete-observations.json"), data, 0600); e != nil {
						t.Error(e)
					}
				}()
				var snapshots []struct {
					Info  *jetstream.ConsumerInfo `json:"info"`
					Error string                  `json:"error"`
					At    time.Time               `json:"at"`
				}
				record := func(info *jetstream.ConsumerInfo, failure error) {
					snapshot := struct {
						Info  *jetstream.ConsumerInfo `json:"info"`
						Error string                  `json:"error"`
						At    time.Time               `json:"at"`
					}{Info: info, At: time.Now().UTC()}
					if failure != nil {
						snapshot.Error = failure.Error()
					}
					snapshots = append(snapshots, snapshot)
					t.Logf("cursor API snapshot stream=%s info=%+v err=%v", stream.CachedInfo().Config.Name, info, failure)
				}
				defer func() {
					data, e := json.MarshalIndent(snapshots, "", "  ")
					if e != nil {
						t.Error(e)
						return
					}
					if e = os.WriteFile(filepath.Join(root, stream.CachedInfo().Config.Name+"-cursor-snapshots.json"), data, 0600); e != nil {
						t.Error(e)
					}
				}()
				return scanConsumeDirectWindowsThrough(call, processObservedStream{candidateObservedStream: observed, record: record, deleted: deleted}, cutoff, func(msg *jetstream.RawStreamMsg) error {
					if stream.CachedInfo().Config.Name != "WF_JRN" {
						return visit(msg)
					}
					visited++
					if msg.Sequence != uint64(visited) {
						return fmt.Errorf("duplicate or omitted journal sequence: visited=%d seq=%d", visited, msg.Sequence)
					}
					if visited == 128 {
						target, err = observed.consumer.Info(call)
						if err != nil {
							return err
						}
						if target.Name != observed.name || target.Stream != "WF_JRN" || target.Config.Replicas != 1 || !target.Config.MemoryStorage || target.Config.AckPolicy != jetstream.AckNonePolicy || target.Cluster == nil || target.NumPending == 0 {
							return fmt.Errorf("invalid pending R1 cursor: %+v", target)
						}
						for n := 0; n < 5; n++ {
							if cluster.NodeName(n) == target.Cluster.Leader {
								killed = n
							}
						}
						if killed < 0 {
							return fmt.Errorf("cursor owner %q not found", target.Cluster.Leader)
						}
						logs, e := cluster.Logs(killed)
						if e != nil {
							return e
						}
						if e = os.WriteFile(filepath.Join(root, "owner-before-kill.log"), []byte(logs), 0600); e != nil {
							return e
						}
						kill, e = cluster.KillNodeObserved(killed)
						if e != nil {
							return e
						}
						if kill.SourceStopped.IsZero() || (kill.State != "exited" && kill.State != "dead" && kill.State != "absent") {
							return fmt.Errorf("unconfirmed process exit: %+v", kill)
						}
						if restart {
							if e := cluster.RestartNode(killed); e != nil {
								return e
							}
						}
					}
					return visit(msg)
				})
			}, true, true, true)
			elapsed := time.Since(started)
			proof := struct {
				Target  *jetstream.ConsumerInfo           `json:"target"`
				Kill    testcluster.DockerKillObservation `json:"kill"`
				Restart bool                              `json:"restart"`
				Visited int                               `json:"visited"`
				Elapsed time.Duration                     `json:"elapsed_ns"`
				Report  Report                            `json:"report"`
				Error   string                            `json:"error"`
			}{Target: target, Kill: kill, Restart: restart, Visited: visited, Elapsed: elapsed, Report: report}
			if failure != nil {
				proof.Error = failure.Error()
			}
			data, e := json.MarshalIndent(proof, "", "  ")
			if e != nil {
				t.Fatal(e)
			}
			if e = os.WriteFile(filepath.Join(root, "fault-result.json"), data, 0600); e != nil {
				t.Fatal(e)
			}
			t.Logf("R5 process owner-loss restart=%v target=%+v kill=%+v visited=%d elapsed=%s report=%+v err=%v", restart, target, kill, visited, elapsed, report, failure)
			if failure != nil || report != want || visited != want.Entries || killed < 0 || elapsed >= 20*time.Second {
				t.Fatalf("process recovery failed: %s", data)
			}
			// Observe cleanup through short independent requests within the same
			// original audit deadline. One lost reply must not consume the
			// five-minute population context or hide the failed observation.
			type cleanupObservation struct {
				Stream    string        `json:"stream"`
				Elapsed   time.Duration `json:"elapsed_ns"`
				Consumers int           `json:"consumers"`
				Error     string        `json:"error"`
			}
			var cleanup []cleanupObservation
			defer func() {
				data, e := json.MarshalIndent(cleanup, "", "  ")
				if e != nil {
					t.Error(e)
					return
				}
				if e = os.WriteFile(filepath.Join(root, "cleanup-observations.json"), data, 0600); e != nil {
					t.Error(e)
				}
			}()
			for _, name := range []string{"WF_INV", "WF_JRN"} {
				for {
					call, done := context.WithTimeout(attempt, time.Second)
					stream, e := js.Stream(call, name)
					observation := cleanupObservation{Stream: name, Elapsed: time.Since(started), Consumers: -1}
					if e == nil {
						observation.Consumers = stream.CachedInfo().State.Consumers
					}
					done()
					if e != nil {
						observation.Error = e.Error()
					}
					cleanup = append(cleanup, observation)
					t.Logf("cleanup observation=%+v", observation)
					if e == nil && observation.Consumers == 0 {
						break
					}
					if attempt.Err() != nil {
						t.Fatalf("original audit/cleanup budget exhausted: %v", attempt.Err())
					}
					if e != nil && !errors.Is(e, context.DeadlineExceeded) && !batchReadTransportError(e) {
						t.Fatalf("cleanup %s: %v", name, e)
					}
					select {
					case <-attempt.Done():
						t.Fatalf("original audit/cleanup budget exhausted: %v", attempt.Err())
					case <-time.After(50 * time.Millisecond):
					}
				}
			}
		})
	}
}
