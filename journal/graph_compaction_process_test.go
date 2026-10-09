//go:build !windows

package journal_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/testcluster"
)

// This cuts real OS server processes after acknowledged archive publication.
// It does not simulate filesystem power loss or a worker process crash.
func TestProcessGraphCheckpointArchiveKillAndCollection(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d-domain", replicas), func(t *testing.T) {
			cluster, err := testcluster.StartProcessesWithDomain(t.TempDir(), replicas, "ARCHIVE")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(cluster.Close)
			if evidence := os.Getenv("ARCHIVE_PROCESS_EVIDENCE"); evidence != "" {
				destination := filepath.Join(evidence, fmt.Sprintf("R%d-domain", replicas))
				if err := os.MkdirAll(destination, 0755); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					for i := range cluster.Commands {
						if body, err := os.ReadFile(cluster.LogPath(i)); err == nil {
							if err := os.WriteFile(filepath.Join(destination, fmt.Sprintf("node-%d.log", i)), body, 0644); err != nil {
								t.Error(err)
							}
						} else {
							t.Error(err)
						}
						capture, stop := context.WithTimeout(context.Background(), time.Second)
						body, err := cluster.Diagnostic(capture, i, "jetstream")
						stop()
						if err == nil {
							if err := os.WriteFile(filepath.Join(destination, fmt.Sprintf("node-%d-jsz.json", i)), body, 0644); err != nil {
								t.Error(err)
							}
						} else {
							t.Log("diagnostic unavailable", i, err)
						}
					}
				})
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			ready := func() {
				if replicas == 1 {
					return
				}
				for {
					for i := range cluster.Commands {
						attempt, stop := context.WithTimeout(ctx, time.Second)
						body, err := cluster.Diagnostic(attempt, i, "jetstream")
						stop()
						var info struct {
							Meta *struct {
								Leader   string `json:"leader"`
								Size     int    `json:"cluster_size"`
								Rescue   bool   `json:"rescue"`
								Replicas []struct {
									Current bool `json:"current"`
									Offline bool `json:"offline"`
								} `json:"replicas"`
							} `json:"meta_cluster"`
						}
						if err == nil && json.Unmarshal(body, &info) == nil && info.Meta != nil && info.Meta.Leader != "" && info.Meta.Size == replicas && !info.Meta.Rescue && len(info.Meta.Replicas) == replicas-1 {
							current := true
							for _, peer := range info.Meta.Replicas {
								current = current && peer.Current && !peer.Offline
							}
							if current {
								return
							}
						}
					}
					select {
					case <-ctx.Done():
						t.Fatal("metadata quorum readiness", ctx.Err())
					case <-time.After(20 * time.Millisecond):
					}
				}
			}
			ready()
			nativeCheckpointArchiveFixture(t, ctx, replicas, func() *nats.Conn { return cluster.Clients[0] }, func() {
				oldPIDs := make([]int, replicas)
				for i, cmd := range cluster.Commands {
					oldPIDs[i] = cmd.Process.Pid
					if err := cluster.KillNode(i); err != nil {
						t.Fatal(err)
					}
					state := cmd.ProcessState
					if state == nil {
						t.Fatal("missing process exit", i)
					}
					wait, ok := state.Sys().(syscall.WaitStatus)
					if !ok || !wait.Signaled() || wait.Signal() != syscall.SIGKILL {
						t.Fatal("unexpected process exit", i, state)
					}
					t.Logf("ARCHIVE_PROCESS_KILL node=%d pid=%d signal=%s", i, oldPIDs[i], wait.Signal())
				}
				for i := range cluster.Commands {
					if err := cluster.RestartNode(i); err != nil {
						t.Fatal(err)
					}
					if cluster.Commands[i].Process.Pid == oldPIDs[i] {
						t.Fatal("restart reused process", i)
					}
					t.Logf("ARCHIVE_PROCESS_REOPEN node=%d old_pid=%d new_pid=%d", i, oldPIDs[i], cluster.Commands[i].Process.Pid)
				}
				ready()
				js, err := jetstream.NewWithDomain(cluster.Clients[0], "ARCHIVE")
				if err != nil {
					t.Fatal(err)
				}
				start := time.Now()
				for {
					recovered := true
					for _, name := range []string{"ARCHIVE_AUTH", "OBJ_ARCHIVE_OBJECTS"} {
						attempt, stop := context.WithTimeout(ctx, time.Second)
						stream, err := js.Stream(attempt, name)
						var info *jetstream.StreamInfo
						if err == nil {
							info, err = stream.Info(attempt)
						}
						stop()
						if err != nil || info == nil || info.Config.Replicas != replicas {
							recovered = false
							break
						}
						if replicas > 1 {
							if info.Cluster == nil || info.Cluster.Leader == "" || len(info.Cluster.Replicas) != replicas-1 {
								recovered = false
								break
							}
							for _, peer := range info.Cluster.Replicas {
								if !peer.Current || peer.Offline {
									recovered = false
								}
							}
						}
					}
					if recovered {
						t.Logf("ARCHIVE_ORIGINAL_STREAMS_READY elapsed=%s replicas=%d", time.Since(start), replicas)
						break
					}
					select {
					case <-ctx.Done():
						t.Fatal("original stream recovery readiness", ctx.Err())
					case <-time.After(20 * time.Millisecond):
					}
				}
			})
			t.Log("PROCESS_CHECKPOINT_ARCHIVE successive_compactions=2 all_servers_sigkill_observed=true original_stores_reopened=true old_reader_preserved=true original_receipt_collected=true full_logical_audit=true raw_chunks_after_retirement=0")
		})
	}
}
