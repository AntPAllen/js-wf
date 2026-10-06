//go:build linux

package integration_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/provision"
	"js-wf/testcluster"
)

func TestContinuationRetirementReuseWithManifestLossAndServerSIGKILL(t *testing.T) {
	for _, full := range []bool{false, true} {
		t.Run(fmt.Sprintf("all_servers=%t", full), func(t *testing.T) {
			runContinuationRetirementProcessFault(t, full, false, "")
		})
	}
}

func TestContinuationRetirementReuseWithManifestLossAndLeaseExpiryAcrossServerSIGKILL(t *testing.T) {
	runContinuationRetirementProcessFault(t, true, true, "")
}

func TestContinuationRetirementReuseInJetStreamDomainWithManifestLossAndAllServerSIGKILL(t *testing.T) {
	runContinuationRetirementProcessFault(t, true, false, "WFRETIRE")
}

func TestContinuationRetirementReuseInJetStreamDomainWithManifestLossAndLeaseExpiryAcrossAllServerSIGKILL(t *testing.T) {
	runContinuationRetirementProcessFault(t, true, true, "WFRETIRE")
}

func TestContinuationRetirementReuseInJetStreamDomainWithManifestLossWeakFrameAbsenceAndAllServerSIGKILL(t *testing.T) {
	runContinuationRetirementProcessFault(t, true, false, "WFRETIRE", true)
}

func TestContinuationRetirementReuseInJetStreamDomainWithManifestLossWeakFrameAbsenceAndLeaseExpiryAcrossAllServerSIGKILL(t *testing.T) {
	runContinuationRetirementProcessFault(t, true, true, "WFRETIRE", true)
}

func TestContinuationRetirementReuseInLegacyJetStreamDomainWithManifestLossWeakFrameAbsenceAndLeaseExpiryAcrossAllServerSIGKILL(t *testing.T) {
	binary := os.Getenv("WF_NATS_SERVER_BIN")
	if binary == "" {
		t.Skip("requires NATS 2.11.17 through WF_NATS_SERVER_BIN")
	}
	runContinuationRetirementProcessFaultWithBinary(t, true, true, "WFRETIRE", true, binary)
}

func TestContinuationRetirementReuseInJetStreamDomainWithCommittedManifestAckLossWeakFrameAbsenceAndLeaseExpiryAcrossAllServerSIGKILL(t *testing.T) {
	runContinuationRetirementProcessFaultWithBinary(t, true, true, "WFRETIRE", true, "", true)
}

// Only the selected fresh frame's first weak Object Store response is injected.
// The frame upload, administrative metadata confirmation and retried payload
// read use real domain servers and original retained stores after SIGKILL.
type retirementWeakFrameJS struct {
	jetstream.JetStream
	object  atomic.Value
	armed   atomic.Bool
	drops   atomic.Int64
	reads   atomic.Int64
	leaders atomic.Int64
	direct  atomic.Int64
}

func (j *retirementWeakFrameJS) ObjectStore(ctx context.Context, bucket string) (jetstream.ObjectStore, error) {
	store, err := j.JetStream.ObjectStore(ctx, bucket)
	if err != nil || bucket != "WF_BLOB" {
		return store, err
	}
	return &retirementWeakFrameStore{ObjectStore: store, owner: j}, nil
}

type retirementWeakFrameStore struct {
	jetstream.ObjectStore
	owner *retirementWeakFrameJS
}

func (s *retirementWeakFrameStore) GetBytes(ctx context.Context, name string, opts ...jetstream.GetObjectOpt) ([]byte, error) {
	if object, ok := s.owner.object.Load().(string); ok && name == object {
		s.owner.reads.Add(1)
		if s.owner.armed.Swap(false) {
			s.owner.drops.Add(1)
			return nil, jetstream.ErrObjectNotFound
		}
	}
	return s.ObjectStore.GetBytes(ctx, name, opts...)
}

func runContinuationRetirementProcessFault(t *testing.T, full, expireLease bool, domain string, weakFrame ...bool) {
	t.Helper()
	if len(weakFrame) > 1 {
		t.Fatal("at most one weak frame control")
	}
	runContinuationRetirementProcessFaultWithBinary(t, full, expireLease, domain, len(weakFrame) == 1 && weakFrame[0], "")
}

func runContinuationRetirementProcessFaultWithBinary(t *testing.T, full, expireLease bool, domain string, forcedAbsence bool, legacyBinary string, commitBeforeDrop ...bool) {
	t.Helper()
	if len(commitBeforeDrop) > 1 {
		t.Fatal("at most one committed manifest control")
	}
	committed := len(commitBeforeDrop) == 1 && commitBeforeDrop[0]
	if committed && (!full || !expireLease || domain == "" || !forcedAbsence) {
		t.Fatal("committed manifest control requires full domain/expiry/weak-frame cut")
	}
	if expireLease && !full {
		t.Fatal("lease expiry requires every server stopped")
	}
	if forcedAbsence && (!full || domain == "") {
		t.Fatal("weak frame control requires a full domain server cut")
	}
	root := t.TempDir()
	if retained := os.Getenv("WF_CONTINUATION_RETIREMENT_PROCESS_ROOT"); retained != "" {
		root = filepath.Join(retained, fmt.Sprintf("all-servers-%t-lease-expiry-%t", full, expireLease))
		if domain != "" {
			root += "-domain-" + domain
		}
		if forcedAbsence {
			root += "-weak-frame-absence"
		}
		if legacyBinary != "" {
			root += "-legacy-2.11.17"
		}
		if committed {
			root += "-committed-manifest-ack-loss"
		}
		if err := os.Mkdir(root, 0700); err != nil {
			t.Fatal(err)
		}
	}
	var cluster *testcluster.ProcessCluster
	var err error
	if legacyBinary != "" {
		if domain == "" || !full {
			t.Fatal("legacy control requires a full domain server cut")
		}
		data, readErr := os.ReadFile(legacyBinary)
		if readErr != nil {
			t.Fatal(readErr)
		}
		retained := filepath.Join(root, "legacy", "nats-server")
		if err = os.MkdirAll(filepath.Dir(retained), 0700); err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(retained, data, 0700); err != nil {
			t.Fatal(err)
		}
		cluster, err = testcluster.StartMixedVersionProcessesWithDomain(root, []string{retained, retained, retained}, domain)
	} else if domain == "" {
		cluster, err = testcluster.StartProcesses(root, 3)
	} else {
		cluster, err = testcluster.StartProcessesWithDomain(root, 3, domain)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	all := make([]jetstream.JetStream, 3)
	for node := range all {
		options := []nats.Option{nats.MaxReconnects(-1), nats.ReconnectWait(25 * time.Millisecond)}
		if domain != "" {
			options = append(options, nats.IgnoreDiscoveredServers())
		}
		nc, err := nats.Connect(cluster.ClientURL(node), options...)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(nc.Close)
		if domain == "" {
			all[node], err = jetstream.New(nc)
		} else {
			all[node], err = jetstream.NewWithDomain(nc, domain)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	ready, stop := context.WithTimeout(context.Background(), 30*time.Second)
	defer stop()
	var provisionErr error
	provisioned := false
	for ready.Err() == nil {
		attempt, finish := context.WithTimeout(ready, 4*time.Second)
		if legacyBinary == "" {
			provisionErr = provision.Ensure(attempt, all[0], 3)
		} else {
			var backend provision.TimerBackend
			backend, provisionErr = provision.EnsureAuto(attempt, all[0], 3)
			if provisionErr == nil && backend != provision.FallbackTimers {
				provisionErr = fmt.Errorf("legacy control backend=%s want fallback", backend)
			}
		}
		finish()
		if provisionErr == nil {
			provisioned = true
			break
		}
		select {
		case <-ready.Done():
		case <-time.After(50 * time.Millisecond):
		}
	}
	if !provisioned {
		t.Fatalf("provision startup: last_error=%v deadline=%v", provisionErr, ready.Err())
	}
	if err := waitMatrixWorkflowReplicas(ready, all[0]); err != nil {
		t.Fatal(err)
	}
	if domain != "" {
		for node := range all {
			if legacyBinary != "" && all[node].Conn().ConnectedServerVersion() != "2.11.17" {
				t.Fatalf("legacy node%d version=%s want2.11.17", node, all[node].Conn().ConnectedServerVersion())
			}
			info, err := all[node].AccountInfo(ready)
			if err != nil || info.Domain != domain || all[node].Conn().ConnectedDomain() != domain {
				t.Fatalf("native node%d domain admission: %+v / %v", node, info, err)
			}
			t.Logf("native domain admitted node=%d pid=%d domain=%s server_id=%s", node, cluster.Commands[node].Process.Pid, info.Domain, all[node].Conn().ConnectedServerId())
			if legacyBinary != "" {
				t.Logf("legacy domain admitted node=%d version=%s timer_backend=fallback", node, all[node].Conn().ConnectedServerVersion())
			}
		}
	}
	var previousEpoch uint64
	var leaseTTL time.Duration
	var weak *retirementWeakFrameJS
	var configure []func(*retirementManifestPort)
	type committedRecord struct {
		key  string
		data []byte
	}
	var publication atomic.Value
	if committed {
		configure = append(configure, func(port *retirementManifestPort) {
			port.commitFresh = true
			port.afterCommit = func(key string, data []byte) {
				publication.Store(committedRecord{key: key, data: data})
			}
		})
	}
	if forcedAbsence {
		weak = &retirementWeakFrameJS{}
		prefix := "$JS." + domain + ".API"
		traced, err := jetstream.NewWithDomain(all[1].Conn(), domain, jetstream.WithClientTrace(&jetstream.ClientTrace{RequestSent: func(subject string, data []byte) {
			object, ok := weak.object.Load().(string)
			if !ok {
				return
			}
			var request struct {
				LastBySubject string `json:"last_by_subj"`
			}
			if json.Unmarshal(data, &request) != nil || request.LastBySubject != "$O.WF_BLOB.M."+base64.URLEncoding.EncodeToString([]byte(object)) {
				return
			}
			if subject == prefix+".STREAM.MSG.GET.OBJ_WF_BLOB" {
				weak.leaders.Add(1)
			}
			if strings.HasPrefix(subject, prefix+".DIRECT.GET.OBJ_WF_BLOB") {
				weak.direct.Add(1)
			}
		}}))
		if err != nil {
			t.Fatal(err)
		}
		weak.JetStream = traced
		configure = append(configure, func(port *retirementManifestPort) {
			port.SnapshotWritePort = journal.NewSnapshotPort(weak)
			port.afterDrop = func(object string) {
				weak.object.Store(object)
				weak.armed.Store(true)
				t.Logf("retirement weak frame armed after domain heal: object=%s", object)
			}
		})
	}
	runContinuationRetirementOnCluster(t, all, true, func(ctx context.Context) error {
		cutStarted := time.Now()
		originalPIDs, originalIDs := map[int]int{}, map[int]string{}
		stream, err := all[0].Stream(ctx, "KV_WF_STATE")
		if err != nil {
			return err
		}
		if committed {
			record, ok := publication.Load().(committedRecord)
			if !ok {
				return fmt.Errorf("fresh manifest publication not recorded")
			}
			message, readErr := stream.GetLastMsgForSubject(ctx, "$KV.WF_STATE."+record.key)
			if readErr != nil || message == nil || !bytes.Equal(message.Data, record.data) || message.Sequence == 0 {
				return fmt.Errorf("committed manifest readback mismatch: %v", readErr)
			}
			var snap journal.Snapshot
			if err := json.Unmarshal(message.Data, &snap); err != nil || snap.Runtime == nil {
				return fmt.Errorf("committed runtime manifest absent: %v", err)
			}
			t.Logf("retirement manifest committed before SIGKILL: generation=%d object=%s sequence=%d", snap.Runtime.InvSeq, snap.Runtime.Object, message.Sequence)
		}
		info, err := stream.Info(ctx)
		if err != nil || info.Cluster == nil || info.Cluster.Leader == "" {
			return fmt.Errorf("state leader unconfirmed: info=%+v err=%v", info, err)
		}
		node, err := strconv.Atoi(strings.TrimPrefix(info.Cluster.Leader, "wf-process-"))
		if err != nil || node < 0 || node >= 3 {
			return fmt.Errorf("invalid state leader %q", info.Cluster.Leader)
		}
		nodes := []int{node}
		if full {
			nodes = []int{0, 1, 2}
		}
		if expireLease {
			kv, err := all[0].KeyValue(ctx, "WF_LEASE")
			if err != nil {
				return err
			}
			status, err := kv.Status(ctx)
			if err != nil {
				return err
			}
			leaseTTL = status.TTL()
			if leaseTTL != provision.LeaseTTL {
				return fmt.Errorf("lease TTL=%s want=%s", leaseTTL, provision.LeaseTTL)
			}
			entry, err := kv.Get(ctx, identity.Key("checkpoint-retire", "reused"))
			if err != nil {
				return err
			}
			var held lease.Value
			if err := json.Unmarshal(entry.Value(), &held); err != nil {
				return err
			}
			if held.Epoch == 0 || held.Worker != "checkpoint-retirement" {
				return fmt.Errorf("fresh owner unconfirmed: %+v", held)
			}
			previousEpoch = held.Epoch
			t.Logf("retirement lease before outage: epoch=%d ttl=%s revision=%d created=%s", previousEpoch, leaseTTL, entry.Revision(), entry.Created().UTC().Format(time.RFC3339Nano))
		}
		// Reap and verify every SIGKILL before starting any replacement.
		for _, node := range nodes {
			pid := cluster.Commands[node].Process.Pid
			originalPIDs[node], originalIDs[node] = pid, all[node].Conn().ConnectedServerId()
			if err := cluster.KillNode(node); err != nil {
				return err
			}
			state := cluster.Commands[node].ProcessState
			if state == nil {
				return fmt.Errorf("node%d has no terminal process state", node)
			}
			status, ok := state.Sys().(syscall.WaitStatus)
			if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
				return fmt.Errorf("node%d did not exit via SIGKILL: %v", node, state)
			}
			t.Logf("retirement fresh manifest cut: state_leader=%s killed_node=%d pid=%d signal=%s", info.Cluster.Leader, node, pid, status.Signal())
		}
		if expireLease {
			// This is the fault controller: even when lease loss cancels the
			// SDK operation, it must hold the outage then heal every server.
			stopped := time.Now()
			time.Sleep(leaseTTL + time.Second)
			if time.Since(stopped) <= leaseTTL {
				return fmt.Errorf("outage did not exceed lease TTL")
			}
			t.Logf("retirement all-server outage: held=%s ttl=%s", time.Since(stopped), leaseTTL)
		}
		for _, node := range nodes {
			if err := cluster.RestartNode(node); err != nil {
				return err
			}
			if domain != "" && cluster.Commands[node].Process.Pid == originalPIDs[node] {
				return fmt.Errorf("domain node%d replacement PID unchanged", node)
			}
			t.Logf("retirement restarted node=%d pid=%d", node, cluster.Commands[node].Process.Pid)
		}
		if domain != "" {
			// Reconnect alone does not prove domain metadata leadership. Bound
			// all peer API recovery by the original 30-second whole-cut target.
			observe, stopObserve := context.WithDeadline(context.Background(), cutStarted.Add(30*time.Second))
			defer stopObserve()
			for node := range all {
				for {
					attempt, stopAttempt := context.WithTimeout(observe, time.Second)
					info, err := all[node].AccountInfo(attempt)
					stopAttempt()
					if err == nil && legacyBinary != "" {
						if all[node].Conn().ConnectedServerVersion() != "2.11.17" {
							return fmt.Errorf("legacy restarted node%d wrong version", node)
						}
						attempt, stopAttempt := context.WithTimeout(observe, time.Second)
						run, readErr := all[node].Stream(attempt, "WF_RUN")
						if readErr == nil {
							var streamInfo *jetstream.StreamInfo
							streamInfo, readErr = run.Info(attempt)
							if readErr == nil && streamInfo.Config.AllowMsgSchedules {
								readErr = fmt.Errorf("legacy domain restart changed fallback backend")
							}
						}
						stopAttempt()
						err = readErr
						if err == nil {
							t.Logf("legacy domain healed node=%d version=%s timer_backend=fallback", node, all[node].Conn().ConnectedServerVersion())
						}
					}
					if err == nil {
						if info.Domain != domain || all[node].Conn().ConnectedDomain() != domain || all[node].Conn().ConnectedServerId() == originalIDs[node] {
							return fmt.Errorf("restarted node%d wrong domain %q", node, info.Domain)
						}
						t.Logf("native domain healed node=%d pid=%d domain=%s server_id=%s elapsed=%s", node, cluster.Commands[node].Process.Pid, info.Domain, all[node].Conn().ConnectedServerId(), time.Since(cutStarted))
						break
					}
					select {
					case <-observe.Done():
						return fmt.Errorf("domain node%d recovery: %w", node, err)
					case <-time.After(50 * time.Millisecond):
					}
				}
			}
		}
		return nil
	}, configure...)
	if forcedAbsence {
		read, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		state, err := all[0].KeyValue(read, "WF_STATE")
		if err != nil {
			t.Fatal(err)
		}
		manifest, err := state.Get(read, "snap."+identity.Key("checkpoint-retire", "reused"))
		if err != nil {
			t.Fatal(err)
		}
		var snapshot journal.Snapshot
		if err := json.Unmarshal(manifest.Value(), &snapshot); err != nil {
			t.Fatal(err)
		}
		object, _ := weak.object.Load().(string)
		if snapshot.Runtime == nil || snapshot.Runtime.Object != object || weak.drops.Load() != 1 || weak.reads.Load() < 2 || weak.armed.Load() || weak.leaders.Load() != 1 || weak.direct.Load() != 0 {
			t.Fatalf("combined weak frame proof: runtime=%+v object=%s drops=%d reads=%d armed=%t leader=%d direct=%d", snapshot.Runtime, object, weak.drops.Load(), weak.reads.Load(), weak.armed.Load(), weak.leaders.Load(), weak.direct.Load())
		}
		t.Logf("retirement weak frame confirmed: object=%s generation=%d drops=1 reads=%d leader=1 direct=0 route=$JS.%s.API.STREAM.MSG.GET.OBJ_WF_BLOB", object, snapshot.Runtime.InvSeq, weak.reads.Load(), domain)
	}
	if expireLease {
		read, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		records, _, err := journal.New(all[0]).Read(read, "checkpoint-retire", "reused")
		if err != nil || len(records) == 0 {
			t.Fatalf("post-outage journal: records=%d err=%v", len(records), err)
		}
		terminal := records[len(records)-1]
		if terminal.Kind != journal.Completed || terminal.Epoch <= previousEpoch {
			t.Fatalf("terminal epoch=%d kind=%s prior_epoch=%d", terminal.Epoch, terminal.Kind, previousEpoch)
		}
		t.Logf("retirement lease-expiry recovery: prior_epoch=%d terminal_epoch=%d records=%d", previousEpoch, terminal.Epoch, len(records))
	}
}
