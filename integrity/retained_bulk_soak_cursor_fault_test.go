//go:build linux

package integrity

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
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
	retainedBulkSoakCursorOwnerRestart(t, 8520, 238560, 2630779)
}

// Checkpoint4160 preserves the latest failed full cohort. Its original guard
// requires complete invocation/journal/terminal counts; no fixed entry count
// was specified by the concurrent producer. Entries still undergo the same
// complete journal validation, and the entire physical journal is visited.
func TestRetainedAuditBulkSoakCheckpoint4160CursorOwnerRestartVerifiedCopy(t *testing.T) {
	retainedBulkSoakCursorOwnerRestart(t, 4160, 116480, 0)
}

func retainedBulkSoakCursorOwnerRestart(t *testing.T, checkpoint, cutoff, expectedEntries int) {
	t.Helper()
	if os.Getenv("WF_AUDIT_BULK_SOAK_CURSOR_OWNER_RESTART") != "1" {
		t.Skip("opt-in full copied cohort cursor-owner restart")
	}

	if os.Getenv("WF_AUDIT_BULK_SOAK_PARALLEL_DECODE") != "1" {
		t.Fatal("explicit parallel-decode profile required")
	}
	storesRoot, artifact := os.Getenv("WF_AUDIT_BULK_SOAK_STORES"), os.Getenv("WF_AUDIT_BULK_SOAK_ROOT")
	identity := os.Getenv("WF_AUDIT_BULK_SOAK_IDENTITY")
	if storesRoot == "" || artifact == "" {
		t.Skip("opt-in verified original full checkpoint copied-store audit")
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
			if (name == "WF_INV" && info.State.Msgs < uint64(cutoff)) || (name == "WF_JRN" && info.State.Msgs == 0) {
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
	// The failed latest producer retained one old ephemeral audit cursor per
	// source. Remove only these positively identified inherited consumers from
	// this fresh copy, before any trial cursor exists. Never reset stream data.
	readinessBeforeCleanup := map[string]*jetstream.StreamInfo{}
	for name, info := range readiness {
		readinessBeforeCleanup[name] = info
	}
	var inheritedCursors []*jetstream.ConsumerInfo
	var inheritedDeletions []processDeleteObservation
	if checkpoint == 4160 {
		originalFinished := time.Date(2026, 10, 7, 18, 1, 14, 0, time.UTC)
		for _, name := range []string{"WF_INV", "WF_JRN"} {
			stream, err := js.Stream(startup, name)
			if err != nil {
				t.Fatal(err)
			}
			listed := stream.ListConsumers(startup)
			var infos []*jetstream.ConsumerInfo
			for info := range listed.Info() {
				infos = append(infos, info)
			}
			if listed.Err() != nil || len(infos) != 1 || readiness[name].State.Consumers != 1 {
				t.Fatalf("%s: expected exactly one inherited original audit cursor: count=%d err=%v", name, len(infos), listed.Err())
			}
			info := infos[0]
			if info.Stream != name || !strings.HasPrefix(info.Name, "wf-audit-") || info.Config.Replicas != 1 || !info.Config.MemoryStorage || info.Config.AckPolicy != jetstream.AckNonePolicy || info.Created.IsZero() || !info.Created.Before(originalFinished) {
				t.Fatalf("%s: refusing to remove unknown inherited consumer: %+v", name, info)
			}
			inheritedCursors = append(inheritedCursors, info)
			err = stream.DeleteConsumer(startup, info.Name)
			inheritedDeletions = append(inheritedDeletions, processDeleteObservation{Name: info.Name, At: time.Now()})
			if err != nil {
				t.Fatal("delete inherited original audit cursor", err)
			}
			after, err := stream.Info(startup)
			if err != nil {
				t.Fatal(err)
			}
			beforeState := readiness[name].State
			beforeState.Consumers = 0
			if !reflect.DeepEqual(beforeState, after.State) || !reflect.DeepEqual(readiness[name].Config, after.Config) {
				t.Fatal("inherited cursor preparation changed source data/config or left a consumer", name)
			}
			readiness[name] = after
		}
	}
	var creationFault *copiedCreationState
	if os.Getenv("WF_AUDIT_BULK_SOAK_STATE_CREATION_STALL") == "1" {
		if checkpoint != 4160 || os.Getenv("WF_AUDIT_BULK_SOAK_STATE_CONNECTION_LOSS") == "1" || os.Getenv("WF_AUDIT_BULK_SOAK_STATE_PEER_OUTAGE") == "1" {
			t.Fatal("creation stall requires latest full cohort and no other watch fault")
		}
		creationFault = prepareCopiedCreation(t, startup, js, cluster, root)
	}
	inheritedCleanupFinished := time.Now()
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
		ReadinessBeforeCleanup   map[string]*jetstream.StreamInfo  `json:"readiness_before_cleanup,omitempty"`
		InheritedCursors         []*jetstream.ConsumerInfo         `json:"inherited_cursors,omitempty"`
		InheritedDeletions       []processDeleteObservation        `json:"inherited_deletions,omitempty"`
		InheritedCleanupFinished time.Time                         `json:"inherited_cleanup_finished"`
		Cutoff                   uint64                            `json:"cutoff"`
		ElapsedNS                int64                             `json:"elapsed_ns"`
		AuditNS                  int64                             `json:"audit_ns"`
		Report                   Report                            `json:"report"`
		Error                    string                            `json:"error"`
		Readiness                map[string]*jetstream.StreamInfo  `json:"readiness"`
		RecoveredReadiness       map[string]*jetstream.StreamInfo  `json:"readiness_after"`
		ParallelDecode           bool                              `json:"parallel_decode"`
		CursorRestart            bool                              `json:"cursor_owner_restart"`
		Qualifies24h             bool                              `json:"qualifies_24h"`
		JournalVisits            uint64                            `json:"journal_visits"`
		Target                   *jetstream.ConsumerInfo           `json:"target"`
		Kill                     testcluster.DockerKillObservation `json:"kill"`
		RestartCompleted         time.Time                         `json:"restart_completed"`
		Cursors                  []cursorObservation               `json:"cursors"`
		CursorErrors             []string                          `json:"cursor_errors"`
		Deletions                []processDeleteObservation        `json:"deletions"`
		StateCreationStall       map[string]any                    `json:"state_creation_stall,omitempty"`
		StateConnectionLoss      *copiedWatchConnectionProof       `json:"state_connection_loss,omitempty"`
		StateWatches             []capacityWatchTiming             `json:"state_watches"`
		Cleanup                  map[string]int                    `json:"cleanup"`
	}{ReadinessBeforeCleanup: readinessBeforeCleanup, InheritedCursors: inheritedCursors, InheritedDeletions: inheritedDeletions, InheritedCleanupFinished: inheritedCleanupFinished, Cutoff: uint64(cutoff), Readiness: readiness, ParallelDecode: true, CursorRestart: true, Cleanup: map[string]int{}, RecoveredReadiness: map[string]*jetstream.StreamInfo{}}
	call, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	began := time.Now()
	trace := &capacityStateTrace{origin: &began}
	var watchFault *copiedWatchConnectionLoss
	var creationFrames []StateSnapshotObservation
	if creationFault != nil {
		call = WithStateSnapshotObserver(call, func(frame StateSnapshotObservation) { creationFrames = append(creationFrames, frame) })
	}
	if os.Getenv("WF_AUDIT_BULK_SOAK_STATE_CONNECTION_LOSS") == "1" || os.Getenv("WF_AUDIT_BULK_SOAK_STATE_PEER_OUTAGE") == "1" {
		watchFault = newCopiedWatchConnectionLoss()
		watchFault.peerOutage = os.Getenv("WF_AUDIT_BULK_SOAK_STATE_PEER_OUTAGE") == "1"
		if watchFault.peerOutage && os.Getenv("WF_AUDIT_BULK_SOAK_STATE_CONNECTION_LOSS") == "1" {
			t.Fatal("watch fault profiles are mutually exclusive")
		}
		call = WithStateSnapshotObserver(call, watchFault.observe)
	}
	created := map[string]bool{}
	ownerSelected := false
	reader := func(ctx context.Context, stream jetstream.Stream, upper *uint64, visit func(*jetstream.RawStreamMsg) error) error {
		streamName := stream.CachedInfo().Config.Name
		observed := &candidateObservedStream{Stream: stream, cursorReplicas: 1}
		wrapped := processObservedStream{candidateObservedStream: observed, record: func(info *jetstream.ConsumerInfo, err error) {
			if err != nil {
				result.CursorErrors = append(result.CursorErrors, err.Error())
			}
			if info != nil {
				if watchFault != nil && info.Stream == "WF_JRN" && !ownerSelected {
					for n := 0; n < 5; n++ {
						if info.Cluster != nil && cluster.NodeName(n) == info.Cluster.Leader {
							ownerSelected = true
							watchFault.owner = n
							close(watchFault.ownerReady)
						}
					}
				}
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
				if creationFault != nil {
					// Confirm watchdog cancellation and transport join before killing
					// the cursor owner, which may be the proxy upstream itself.
					select {
					case <-creationFault.joined:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
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
				if watchFault != nil {
					if watchFault.owner != owner {
						return fmt.Errorf("watch connection owner changed before cut")
					}
					if watchFault.peerOutage {
						if err := watchFault.waitFirst(ctx); err != nil {
							return err
						}
					} else if err := watchFault.closeFirst(ctx); err != nil {
						return err
					}
				}
				result.Kill, err = cluster.KillNodeObserved(owner)
				if err != nil {
					return err
				}
				if result.Kill.SourceStopped.IsZero() {
					return fmt.Errorf("actual cursor owner exit unconfirmed")
				}
				if watchFault != nil && watchFault.peerOutage {
					if err := watchFault.exposeDisconnected(ctx); err != nil {
						return err
					}
					// A real three-second offline interval exceeds the candidate's two-second
					// idle bound. The original caller deadline remains the total limit.
					hold := time.NewTimer(3 * time.Second)
					select {
					case <-hold.C:
					case <-ctx.Done():
						hold.Stop()
						return ctx.Err()
					}
					watchFault.mu.Lock()
					watchFault.proof.RestartStarted = time.Now().UTC()
					watchFault.proof.OfflineHoldNS = int64(watchFault.proof.RestartStarted.Sub(result.Kill.SourceStopped))
					watchFault.mu.Unlock()
				}
				if err := cluster.RestartNode(owner); err != nil {
					return err
				}
				result.RestartCompleted = time.Now().UTC()
				if watchFault != nil {
					close(watchFault.restarted)
				}
			}
			return visit(msg)
		})
	}
	cutoffValue := uint64(cutoff)
	var stateJS jetstream.JetStream = js
	if creationFault != nil {
		stateJS = copiedCreationJS{JetStream: js, state: creationFault}
	}
	if watchFault != nil {
		stateJS = copiedWatchFaultJS{JetStream: js, fault: watchFault, cluster: cluster}
	}
	report, failure := checkUsingJournalDecodeOptions(call, capacityStateTraceJS{JetStream: stateJS, trace: trace}, &cutoffValue, reader, true, true, true, true)
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
	if watchFault != nil {
		proof := watchFault.snapshot()
		result.StateConnectionLoss = &proof
	}
	if creationFault != nil {
		creationFault.proof["attempts"] = creationFault.calls
		creationFault.proof["frames"] = creationFrames
		result.StateCreationStall = creationFault.proof
	}
	result.Error = fmt.Sprint(failure)
	want := Report{Invocations: cutoff, Journals: cutoff, Entries: report.Entries, Terminal: cutoff}
	entryCountMatches := report.Entries > 0 && (expectedEntries == 0 || report.Entries == expectedEntries)
	qualification := failure == nil && report == want && entryCountMatches && result.JournalVisits == journalState.Msgs && result.ElapsedNS < int64(20*time.Second) && !result.RestartCompleted.IsZero()
	if creationFault != nil {
		qualification = qualification && creationFault.calls >= 2 && creationFault.calls <= 3 && result.RecoveredReadiness["KV_WF_STATE"] != nil && result.RecoveredReadiness["KV_WF_STATE"].State.Consumers == 0
	}
	var journalCursors []cursorObservation
	for _, cursor := range result.Cursors {
		if cursor.Created && cursor.Info.Stream == "WF_JRN" {
			journalCursors = append(journalCursors, cursor)
		}
	}
	qualification = qualification && len(journalCursors) == 2 && journalCursors[1].Info.Name != journalCursors[0].Info.Name && journalCursors[1].Info.Config.OptStartSeq == journalCursors[1].JournalVisits+1
	qualification = qualification && len(result.StateWatches) >= 1 && len(result.StateWatches) <= 3
	for _, watch := range result.StateWatches {
		if watch.CreationError == "<nil>" {
			qualification = qualification && watch.StopFinishedNS > 0
		} else {
			qualification = qualification && watch.StopFinishedNS == 0
		}
	}
	if len(result.StateWatches) > 0 {
		last := result.StateWatches[len(result.StateWatches)-1]
		qualification = qualification && last.CreationError == "<nil>" && last.StopError == "<nil>"
	}
	if watchFault != nil {
		proof := result.StateConnectionLoss
		qualification = qualification && proof.Attempts >= 2 && proof.Attempts <= 3 && proof.StatusAfterClose == "CLOSED" && proof.ServerID != "" && proof.ServerName == cluster.NodeName(proof.Owner) && proof.Owner == result.Kill.Node
		var failedAttempts, completedAttempts int
		for _, frame := range proof.Frames {
			if frame.Event == "attempt_return" {
				if frame.InitialComplete && frame.Error == "" {
					completedAttempts++
				} else if !frame.InitialComplete && frame.Error != "" {
					failedAttempts++
				}
			}
		}
		qualification = qualification && failedAttempts >= 1 && completedAttempts == 1
		if watchFault.peerOutage {
			qualification = qualification && proof.OfflineHoldNS >= int64(3*time.Second) && proof.ExposedStatus == "RECONNECTING"
			var idleTimeout bool
			for _, frame := range proof.Frames {
				if frame.Event == "watch_idle_timeout" && !frame.InitialComplete && strings.Contains(frame.Error, "made no progress") && frame.Time.Before(proof.RestartStarted) {
					idleTimeout = true
				}
			}
			qualification = qualification && idleTimeout
		}
	}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "copied-checkpoint-audit.json"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("full original checkpoint%d copied audit cutoff=%d elapsed=%s report=%+v err=%v raw_visits=%d cursor_owner_restart=true", checkpoint, cutoff, time.Duration(result.ElapsedNS), report, failure, result.JournalVisits)
	if !qualification {
		t.Fatalf("original-budget full cursor-owner restart failed: report=%+v raw_visits=%d cursors=%d failure=%v", report, result.JournalVisits, len(journalCursors), failure)
	}
}
