//go:build linux

package integrity

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/testcluster"
)

// This diagnostic preserves the entire failed checkpoint8520 cohort and its
// original attempt budget. Only fresh, fully verified restored stores are used.
// Quiescent recovery cannot qualify the original concurrent 24-hour run.
func TestRetainedAuditBulkSoakCheckpoint8520CursorOwnerRestartVerifiedCopy(t *testing.T) {
	const cutoff = 238560
	if os.Getenv("WF_AUDIT_BULK_SOAK_CURSOR_OWNER_RESTART") != "1" {
		t.Skip("opt-in full copied cohort cursor-owner restart")
	}

	if os.Getenv("WF_AUDIT_BULK_SOAK_PARALLEL_DECODE") != "1" {
		t.Fatal("explicit parallel-decode profile required")
	}
	storesRoot, artifact := os.Getenv("WF_AUDIT_BULK_SOAK_STORES"), os.Getenv("WF_AUDIT_BULK_SOAK_ROOT")
	identity := os.Getenv("WF_AUDIT_BULK_SOAK_IDENTITY")
	if storesRoot == "" || artifact == "" {
		t.Skip("opt-in verified original checkpoint8520 copied-store audit")
	}
	if !filepath.IsAbs(storesRoot) || !filepath.IsAbs(artifact) || identity == "" {
		t.Fatal("absolute fresh copied stores, artifact root and original Raft identity required")
	}
	root := filepath.Join(artifact, t.Name())
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	stores := map[int]string{}
	for n := 0; n < 5; n++ {
		stores[n] = filepath.Join(storesRoot, fmt.Sprintf("node-%d", n))
	}
	cluster, err := testcluster.StartDockerClusterWithRestoredIdentity(filepath.Join(root, "cluster"), 5, stores, identity)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	t.Cleanup(func() {
		for n := 0; n < 5; n++ {
			logs, err := cluster.Logs(n)
			if err != nil {
				t.Error(err)
				continue
			}
			if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("server-%d.log", n)), []byte(logs), 0600); err != nil {
				t.Error(err)
			}
		}
	})
	var urls []string
	for n := 0; n < 5; n++ {
		urls = append(urls, cluster.ClientURL(n))
	}
	nc, err := nats.Connect(strings.Join(urls, ","), nats.IgnoreDiscoveredServers(), nats.MaxReconnects(-1), nats.ReconnectWait(100*time.Millisecond), nats.Timeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	startup, stop := context.WithTimeout(context.Background(), 4*time.Minute)
	defer stop()
	readiness := map[string]*jetstream.StreamInfo{}
	for {
		ready := true
		for _, name := range []string{"WF_INV", "WF_JRN", "KV_WF_STATE", "WF_PURGE"} {
			call, cancel := context.WithTimeout(startup, 2*time.Second)
			stream, e := js.Stream(call, name)
			cancel()
			if e != nil {
				ready = false
				break
			}
			info := stream.CachedInfo()
			if info.Config.Replicas != 5 || info.Config.Storage != jetstream.FileStorage {
				t.Fatalf("%s: original full R5 file source required: %+v", name, info)
			}
			if (name == "WF_INV" && info.State.Msgs < cutoff) || (name == "WF_JRN" && info.State.Msgs == 0) {
				t.Fatalf("%s: incomplete full original cohort: %+v", name, info)
			}
			if info.Cluster == nil || info.Cluster.Leader == "" || len(info.Cluster.Replicas) != 4 {
				ready = false
				break
			}
			for _, peer := range info.Cluster.Replicas {
				if !peer.Current || peer.Offline {
					ready = false
				}
			}
			if !ready {
				break
			}
			readiness[name] = info
		}
		if ready {
			break
		}
		if startup.Err() != nil {
			t.Fatal("all four original R5 sources did not heal", startup.Err())
		}
		time.Sleep(50 * time.Millisecond)
	}
	// No healthy journal scan precedes this fault. Admission reads only stream
	// metadata; the original full captured cohort and budgets remain unchanged.
	journalState := readiness["WF_JRN"].State
	if journalState.FirstSeq != 1 || journalState.LastSeq != journalState.Msgs || journalState.NumDeleted != 0 {
		t.Fatal("fixed original hole-free physical journal population required")
	}
	type cursorObservation struct {
		Info          *jetstream.ConsumerInfo `json:"info"`
		JournalVisits uint64                  `json:"journal_visits"`
		Created       bool                    `json:"created"`
	}
	result := struct {
		Cutoff             uint64                            `json:"cutoff"`
		ElapsedNS          int64                             `json:"elapsed_ns"`
		AuditNS            int64                             `json:"audit_ns"`
		Report             Report                            `json:"report"`
		Error              string                            `json:"error"`
		Readiness          map[string]*jetstream.StreamInfo  `json:"readiness"`
		RecoveredReadiness map[string]*jetstream.StreamInfo  `json:"readiness_after"`
		ParallelDecode     bool                              `json:"parallel_decode"`
		CursorRestart      bool                              `json:"cursor_owner_restart"`
		Qualifies24h       bool                              `json:"qualifies_24h"`
		JournalVisits      uint64                            `json:"journal_visits"`
		Target             *jetstream.ConsumerInfo           `json:"target"`
		Kill               testcluster.DockerKillObservation `json:"kill"`
		RestartCompleted   time.Time                         `json:"restart_completed"`
		Cursors            []cursorObservation               `json:"cursors"`
		CursorErrors       []string                          `json:"cursor_errors"`
		Deletions          []processDeleteObservation        `json:"deletions"`
		StateWatches       []capacityWatchTiming             `json:"state_watches"`
		Cleanup            map[string]int                    `json:"cleanup"`
	}{Cutoff: cutoff, Readiness: readiness, ParallelDecode: true, CursorRestart: true, Cleanup: map[string]int{}, RecoveredReadiness: map[string]*jetstream.StreamInfo{}}
	call, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	began := time.Now()
	trace := &capacityStateTrace{origin: &began}
	created := map[string]bool{}
	reader := func(ctx context.Context, stream jetstream.Stream, upper *uint64, visit func(*jetstream.RawStreamMsg) error) error {
		streamName := stream.CachedInfo().Config.Name
		observed := &candidateObservedStream{Stream: stream, cursorReplicas: 1}
		wrapped := processObservedStream{candidateObservedStream: observed, record: func(info *jetstream.ConsumerInfo, err error) {
			if err != nil {
				result.CursorErrors = append(result.CursorErrors, err.Error())
			}
			if info != nil {
				key := info.Stream + "/" + info.Name
				result.Cursors = append(result.Cursors, cursorObservation{Info: info, JournalVisits: result.JournalVisits, Created: !created[key]})
				created[key] = true
			}
		}, deleted: func(deletion processDeleteObservation) {
			result.Deletions = append(result.Deletions, deletion)
		}}
		return scanConsumeChunkedWindowsThrough(ctx, wrapped, upper, func(msg *jetstream.RawStreamMsg) error {
			if streamName != "WF_JRN" {
				return visit(msg)
			}
			result.JournalVisits++
			if msg.Sequence != result.JournalVisits {
				return fmt.Errorf("duplicate/omitted physical journal record: sequence=%d visit=%d", msg.Sequence, result.JournalVisits)
			}
			if result.JournalVisits == 128 {
				target, err := observed.consumer.Info(ctx)
				if err != nil {
					return err
				}
				result.Target = target
				if target.Name != observed.name || target.Stream != "WF_JRN" || target.Config.Replicas != 1 || !target.Config.MemoryStorage || target.Config.AckPolicy != jetstream.AckNonePolicy || target.Cluster == nil || target.NumPending == 0 {
					return fmt.Errorf("active R1 cursor identity/config/pending mismatch: %+v", target)
				}
				owner := -1
				for node := 0; node < 5; node++ {
					if cluster.NodeName(node) == target.Cluster.Leader {
						owner = node
					}
				}
				if owner < 0 {
					return fmt.Errorf("unknown actual cursor owner %s", target.Cluster.Leader)
				}
				logs, err := cluster.Logs(owner)
				if err != nil {
					return err
				}
				if err := os.WriteFile(filepath.Join(root, "cursor-owner-before-kill.log"), []byte(logs), 0600); err != nil {
					return err
				}
				result.Kill, err = cluster.KillNodeObserved(owner)
				if err != nil {
					return err
				}
				if result.Kill.SourceStopped.IsZero() {
					return fmt.Errorf("actual cursor owner exit unconfirmed")
				}
				if err := cluster.RestartNode(owner); err != nil {
					return err
				}
				result.RestartCompleted = time.Now().UTC()
			}
			return visit(msg)
		})
	}
	cutoffValue := uint64(cutoff)
	report, failure := checkUsingJournalDecodeOptions(call, capacityStateTraceJS{JetStream: js, trace: trace}, &cutoffValue, reader, true, true, true, true)
	result.AuditNS = int64(time.Since(began))
	result.Report = report
	if failure == nil {
		for _, name := range []string{"WF_INV", "WF_JRN"} {
			stream, err := js.Stream(call, name)
			if err != nil {
				failure = err
				break
			}
			result.Cleanup[name] = stream.CachedInfo().State.Consumers
			if result.Cleanup[name] != 0 {
				failure = fmt.Errorf("%s has %d residual audit cursors", name, result.Cleanup[name])
				break
			}
		}
	}
	if failure == nil {
		for {
			ready := true
			for _, name := range []string{"WF_INV", "WF_JRN", "KV_WF_STATE", "WF_PURGE"} {
				probe, stop := context.WithTimeout(call, time.Second)
				stream, err := js.Stream(probe, name)
				stop()
				if err != nil {
					ready = false
					break
				}
				info := stream.CachedInfo()
				if info.Cluster == nil || info.Cluster.Leader == "" || len(info.Cluster.Replicas) != 4 {
					ready = false
					break
				}
				for _, peer := range info.Cluster.Replicas {
					if !peer.Current || peer.Offline {
						ready = false
					}
				}
				result.RecoveredReadiness[name] = info
			}
			if ready {
				break
			}
			if call.Err() != nil {
				failure = call.Err()
				break
			}
			select {
			case <-call.Done():
				failure = call.Err()
			case <-time.After(50 * time.Millisecond):
			}
			if failure != nil {
				break
			}
		}
	}
	result.ElapsedNS = int64(time.Since(began))
	result.StateWatches = trace.snapshot()
	result.Error = fmt.Sprint(failure)
	want := Report{Invocations: cutoff, Journals: cutoff, Entries: 2630779, Terminal: cutoff}
	qualification := failure == nil && report == want && result.JournalVisits == journalState.Msgs && result.ElapsedNS < int64(20*time.Second) && !result.RestartCompleted.IsZero()
	var journalCursors []cursorObservation
	for _, cursor := range result.Cursors {
		if cursor.Created && cursor.Info.Stream == "WF_JRN" {
			journalCursors = append(journalCursors, cursor)
		}
	}
	qualification = qualification && len(journalCursors) == 2 && journalCursors[1].Info.Name != journalCursors[0].Info.Name && journalCursors[1].Info.Config.OptStartSeq == journalCursors[1].JournalVisits+1
	qualification = qualification && len(result.StateWatches) == 1 && result.StateWatches[0].CreationError == "<nil>" && result.StateWatches[0].StopError == "<nil>" && result.StateWatches[0].StopFinishedNS > 0
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "copied-checkpoint-audit.json"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("full original checkpoint8520 copied audit cutoff=%d elapsed=%s report=%+v err=%v raw_visits=%d cursor_owner_restart=true", cutoff, time.Duration(result.ElapsedNS), report, failure, result.JournalVisits)
	if !qualification {
		t.Fatalf("original-budget full cursor-owner restart failed: report=%+v raw_visits=%d cursors=%d failure=%v", report, result.JournalVisits, len(journalCursors), failure)
	}
}
