package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/provision"
	"js-wf/testcluster"
)

// Run the original strict retirement/reuse scenario through a real leaf whose
// local JetStream domain differs from the three-node storage hub's domain.
func TestContinuationRetirementReuseThroughLeafWithHubRestart(t *testing.T) {
	runContinuationRetirementLeaf(t, false, false)
}

func TestContinuationRetirementReuseThroughLeafSIGKILLWithHubRestart(t *testing.T) {
	runContinuationRetirementLeaf(t, true, false)
}

func TestContinuationRetirementReuseThroughLeafSIGKILLAndLeaseExpiryWithHubRestart(t *testing.T) {
	runContinuationRetirementLeaf(t, true, true)
}

func runContinuationRetirementLeaf(t *testing.T, processLeaf, expireLease bool) {
	root := os.Getenv("WF_CONTINUATION_DOMAIN_ROOT")
	if root == "" {
		t.Skip("set WF_CONTINUATION_DOMAIN_ROOT to a fresh absolute directory")
	}
	if !filepath.IsAbs(root) {
		t.Fatal("artifact directory must be absolute")
	}
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	const hubDomain, leafDomain = "WFRETIRE", "WFEDGE"
	hub, err := testcluster.StartWithLeafDomain(filepath.Join(root, "hub"), 3, hubDomain)
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Close()
	var leaf *server.Server
	var leafProcess *testcluster.ProcessCluster
	if processLeaf {
		leafProcess, err = testcluster.StartLeafProcess(filepath.Join(root, "leaf-process"), leafDomain, hub.LeafURLs())
		if err != nil {
			t.Fatal(err)
		}
		defer leafProcess.Close()
	} else {
		leaf, err = server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, ServerName: "wf-edge", NoLog: true, NoSigs: true,
			JetStream: true, JetStreamDomain: leafDomain, StoreDir: filepath.Join(root, "leaf"),
			LeafNode: server.LeafNodeOpts{ReconnectInterval: 25 * time.Millisecond, Remotes: []*server.RemoteLeafOpts{{URLs: hub.LeafURLs(), NoRandomize: true}}}})
		if err != nil {
			t.Fatal(err)
		}
		go leaf.Start()
		defer func() { leaf.Shutdown(); leaf.WaitForShutdown() }()
		if !leaf.ReadyForConnections(5 * time.Second) {
			t.Fatal("leaf not ready")
		}
	}
	leafID := func() string {
		if processLeaf {
			return leafProcess.Clients[0].ConnectedServerId()
		}
		return leaf.ID()
	}
	leafURL := func() string {
		if processLeaf {
			return leafProcess.ClientURL(0)
		}
		return leaf.ClientURL()
	}
	leafSnapshot := func(ctx context.Context) (*server.Leafz, error) {
		if !processLeaf {
			return leaf.Leafz(nil)
		}
		data, e := leafProcess.Diagnostic(ctx, 0, "leaf")
		if e != nil {
			return nil, e
		}
		var value server.Leafz
		e = json.Unmarshal(data, &value)
		return &value, e
	}
	ready, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	all := make([]jetstream.JetStream, 3)
	var connections []*nats.Conn
	for i := range all {
		nc, err := nats.Connect(leafURL(), nats.IgnoreDiscoveredServers(), nats.MaxReconnects(-1), nats.ReconnectWait(25*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		defer nc.Close()
		connections = append(connections, nc)
		all[i], err = jetstream.NewWithDomain(nc, hubDomain)
		if err != nil {
			t.Fatal(err)
		}
		if nc.ConnectedServerId() != leafID() {
			t.Fatal("runtime client bypassed leaf")
		}
	}
	local, err := jetstream.New(connections[0])
	if err != nil {
		t.Fatal(err)
	}
	localInfo, err := local.AccountInfo(ready)
	if err != nil || localInfo.Domain != leafDomain {
		t.Fatalf("local domain=%+v err=%v", localInfo, err)
	}
	// A listening client socket or connected leaf does not admit R3 placement.
	// Require an elected hub metadata leader with fresh stats for all three peers.
	var metadataLeader string
	var metadataPeers []string
	for {
		current := true
		for _, s := range hub.Servers {
			current = current && s.JetStreamIsCurrent()
		}
		for _, s := range hub.Servers {
			if !s.JetStreamIsLeader() {
				continue
			}
			peers := s.JetStreamClusterPeers()
			sort.Strings(peers)
			if current && len(peers) == 3 && peers[0] == "wf-test-0" && peers[1] == "wf-test-1" && peers[2] == "wf-test-2" {
				metadataLeader = s.Name()
				metadataPeers = peers
			}
		}
		if metadataLeader != "" {
			break
		}
		if ready.Err() != nil {
			t.Fatalf("hub metadata placement admission: %v", ready.Err())
		}
		time.Sleep(25 * time.Millisecond)
	}
	metadata, err := json.MarshalIndent(map[string]any{"leader": metadataLeader, "peers": metadataPeers, "all_hubs_current": true, "observed_at": time.Now().UTC()}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "hub-metadata-ready.json"), metadata, 0600); err != nil {
		t.Fatal(err)
	}
	for {
		attempt, done := context.WithTimeout(ready, time.Second)
		err = provision.Ensure(attempt, all[0], 3)
		done()
		if err == nil {
			break
		}
		if ready.Err() != nil || !matrixTransientTransport(err) {
			t.Fatal(err)
		}
		time.Sleep(25 * time.Millisecond)
	}
	if err := waitMatrixWorkflowReplicas(ready, all[0]); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	subjects := map[string]int{}
	trace, err := connections[0].Subscribe("$JS.>", func(m *nats.Msg) { mu.Lock(); subjects[m.Subject]++; mu.Unlock() })
	if err != nil {
		t.Fatal(err)
	}
	defer trace.Unsubscribe()
	if err := connections[0].FlushWithContext(ready); err != nil {
		t.Fatal(err)
	}
	type peer struct {
		Name   string `json:"name"`
		ID     string `json:"id"`
		Domain string `json:"domain"`
	}
	type evidence struct {
		LeaseTTLSeconds float64   `json:"lease_ttl_seconds,omitempty"`
		PriorEpoch      uint64    `json:"prior_epoch,omitempty"`
		TerminalEpoch   uint64    `json:"terminal_epoch,omitempty"`
		LeaseRevision   uint64    `json:"lease_revision,omitempty"`
		OutageStart     time.Time `json:"outage_start,omitempty"`
		OutageEnd       time.Time `json:"outage_end,omitempty"`
		JournalRecords  int       `json:"journal_records,omitempty"`

		FaultProfile      string `json:"fault_profile"`
		LeafOriginalID    string `json:"leaf_original_id,omitempty"`
		LeafPIDBefore     int    `json:"leaf_pid_before,omitempty"`
		LeafPIDAfter      int    `json:"leaf_pid_after,omitempty"`
		LeafSignal        string `json:"leaf_signal,omitempty"`
		LeafExitObserved  bool   `json:"leaf_exit_observed,omitempty"`
		ClientDisconnects []bool `json:"client_disconnects,omitempty"`

		LocalStreamsBefore int            `json:"local_streams_before"`
		LocalStreamsAfter  int            `json:"local_streams_after"`
		LeafBefore         *server.Leafz  `json:"leaf_before"`
		LeafAfter          *server.Leafz  `json:"leaf_after"`
		LeafID             string         `json:"leaf_id"`
		LeafURL            string         `json:"leaf_url"`
		LocalDomain        string         `json:"local_domain"`
		RemoteDomain       string         `json:"remote_domain"`
		Before             []peer         `json:"before"`
		After              []peer         `json:"after"`
		Stopped            []string       `json:"stopped"`
		Disconnected       bool           `json:"leaf_disconnected"`
		Reconnected        bool           `json:"leaf_reconnected"`
		CutStart           time.Time      `json:"cut_start"`
		CutEnd             time.Time      `json:"cut_end"`
		RuntimeServerIDs   []string       `json:"runtime_server_ids"`
		Subjects           map[string]int `json:"subjects"`
		Passed             bool           `json:"scenario_passed"`
	}
	if localInfo.Streams != 0 {
		t.Fatal("leaf unexpectedly owns streams")
	}
	leafBefore, err := leafSnapshot(ready)
	if err != nil || leafBefore.NumLeafs != 1 {
		t.Fatalf("initial leaf topology=%+v err=%v", leafBefore, err)
	}
	proof := evidence{LocalStreamsBefore: localInfo.Streams, LeafBefore: leafBefore, LeafID: leafID(), LeafURL: leafURL(), LocalDomain: localInfo.Domain, RemoteDomain: hubDomain}
	proof.FaultProfile = "hub-restart"
	if processLeaf {
		proof.FaultProfile = "leaf-sigkill-hub-restart"
		proof.LeafOriginalID = proof.LeafID
		proof.LeafPIDBefore = leafProcess.Commands[0].Process.Pid
	}
	if expireLease {
		proof.FaultProfile = "leaf-sigkill-lease-expiry-hub-restart"
	}
	save := func() {
		mu.Lock()
		proof.Subjects = make(map[string]int, len(subjects))
		for k, v := range subjects {
			proof.Subjects[k] = v
		}
		mu.Unlock()
		data, e := json.MarshalIndent(proof, "", "  ")
		if e != nil {
			t.Error(e)
			return
		}
		if e = os.WriteFile(filepath.Join(root, "leaf-domain-proof.json"), data, 0600); e != nil {
			t.Error(e)
		}
	}
	defer save()
	for i, s := range hub.Servers {
		js, e := jetstream.NewWithDomain(hub.Clients[i], hubDomain)
		if e != nil {
			t.Fatal(e)
		}
		info, e := js.AccountInfo(ready)
		if e != nil || info.Domain != hubDomain {
			t.Fatalf("hub%d domain=%+v err=%v", i, info, e)
		}
		proof.Before = append(proof.Before, peer{s.Name(), s.ID(), info.Domain})
	}
	onDrop := func(ctx context.Context) error {
		proof.CutStart = time.Now().UTC()
		// The worker may cancel its operation after losing its leaf connection.
		// Fault recovery is an external observer, bounded by the original cut
		// deadline rather than the operation whose failure we are injecting.
		observe, stop := context.WithDeadline(context.Background(), proof.CutStart.Add(30*time.Second))
		defer stop()
		if expireLease {
			kv, e := all[0].KeyValue(observe, "WF_LEASE")
			if e != nil {
				return e
			}
			status, e := kv.Status(observe)
			if e != nil {
				return e
			}
			if status.TTL() != provision.LeaseTTL {
				return fmt.Errorf("lease TTL mismatch: %s", status.TTL())
			}
			proof.LeaseTTLSeconds = status.TTL().Seconds()
			entry, e := kv.Get(observe, identity.Key("checkpoint-retire", "reused"))
			if e != nil {
				return e
			}
			var held lease.Value
			if e = json.Unmarshal(entry.Value(), &held); e != nil {
				return e
			}
			if held.Epoch == 0 || held.Worker != "checkpoint-retirement" {
				return fmt.Errorf("fresh owner unconfirmed: %+v", held)
			}
			proof.PriorEpoch = held.Epoch
			proof.LeaseRevision = entry.Revision()
		}
		for i := range hub.Servers {
			hub.KillNode(i)
			if hub.Servers[i].Running() {
				return fmt.Errorf("hub%d not stopped", i)
			}
			proof.Stopped = append(proof.Stopped, hub.Servers[i].ID())
		}
		if processLeaf {
			old := leafProcess.Commands[0]
			if e := leafProcess.KillNode(0); e != nil {
				return e
			}
			status, ok := old.ProcessState.Sys().(syscall.WaitStatus)
			if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
				return fmt.Errorf("leaf did not exit with SIGKILL")
			}
			proof.LeafExitObserved = true
			proof.LeafSignal = "killed"
			for _, nc := range connections {
				for nc.Status() != nats.RECONNECTING {
					if observe.Err() != nil {
						return observe.Err()
					}
					time.Sleep(10 * time.Millisecond)
				}
				proof.ClientDisconnects = append(proof.ClientDisconnects, true)
			}
		} else {
			for leaf.NumLeafNodes() != 0 {
				if observe.Err() != nil {
					return observe.Err()
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
		proof.Disconnected = true
		if expireLease {
			proof.OutageStart = time.Now().UTC()
			timer := time.NewTimer(provision.LeaseTTL + time.Second)
			select {
			case <-observe.Done():
				timer.Stop()
				return observe.Err()
			case <-timer.C:
			}
			proof.OutageEnd = time.Now().UTC()
			if proof.OutageEnd.Sub(proof.OutageStart) <= provision.LeaseTTL {
				return fmt.Errorf("outage did not exceed TTL")
			}
			t.Logf("leaf lease outage: held=%s ttl=%s prior_epoch=%d", proof.OutageEnd.Sub(proof.OutageStart), provision.LeaseTTL, proof.PriorEpoch)
		}
		for i := range hub.Servers {
			if e := hub.RestartNode(i); e != nil {
				return e
			}
		}
		if processLeaf {
			if e := leafProcess.RestartNode(0); e != nil {
				return e
			}
			proof.LeafPIDAfter = leafProcess.Commands[0].Process.Pid
			proof.LeafID = leafID()
			if proof.LeafPIDAfter == proof.LeafPIDBefore || proof.LeafID == proof.LeafOriginalID {
				return fmt.Errorf("leaf replacement identity unchanged")
			}
		}
		for {
			attempt, done := context.WithTimeout(observe, 250*time.Millisecond)
			info, e := all[0].AccountInfo(attempt)
			done()
			if e == nil {
				if info.Domain != hubDomain {
					return fmt.Errorf("wrong remote domain %q", info.Domain)
				}
				break
			}
			if observe.Err() != nil || !matrixTransientTransport(e) {
				return fmt.Errorf("remote domain recovery: %w", e)
			}
			time.Sleep(25 * time.Millisecond)
		}
		proof.LeafAfter, err = leafSnapshot(observe)
		if err != nil {
			return err
		}
		if proof.LeafAfter.NumLeafs != 1 {
			return fmt.Errorf("leaf reconnect count=%d", proof.LeafAfter.NumLeafs)
		}
		proof.Reconnected = true
		if e := waitMatrixWorkflowReplicas(observe, all[0]); e != nil {
			return e
		}
		for i, s := range hub.Servers {
			js, e := jetstream.NewWithDomain(hub.Clients[i], hubDomain)
			if e != nil {
				return e
			}
			info, e := js.AccountInfo(observe)
			if e != nil {
				return e
			}
			if info.Domain != hubDomain {
				return fmt.Errorf("replacement hub wrong domain")
			}
			if s.ID() == proof.Before[i].ID {
				return fmt.Errorf("hub%d identity unchanged", i)
			}
			proof.After = append(proof.After, peer{s.Name(), s.ID(), info.Domain})
		}
		for _, nc := range connections {
			if nc.ConnectedServerId() != leafID() {
				return fmt.Errorf("runtime client bypassed leaf after cut")
			}
			proof.RuntimeServerIDs = append(proof.RuntimeServerIDs, nc.ConnectedServerId())
		}
		proof.CutEnd = time.Now().UTC()
		if processLeaf {
			t.Logf("leaf SIGKILL confirmed: old_pid=%d new_pid=%d old_id=%s new_id=%s client_disconnects=3", proof.LeafPIDBefore, proof.LeafPIDAfter, proof.LeafOriginalID, proof.LeafID)
		}
		t.Logf("leaf domain hub cut: disconnected=true reconnected=true hub_peers=3 leaf_id=%s elapsed=%s", leafID(), proof.CutEnd.Sub(proof.CutStart))
		return nil
	}
	runContinuationRetirementOnCluster(t, all, true, onDrop)
	if expireLease && !t.Failed() {
		read, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		records, _, e := journal.New(all[0]).Read(read, "checkpoint-retire", "reused")
		if e != nil || len(records) == 0 {
			t.Fatalf("post-outage journal: records=%d err=%v", len(records), e)
		}
		terminal := records[len(records)-1]
		if terminal.Kind != journal.Completed || terminal.Epoch <= proof.PriorEpoch {
			t.Fatalf("terminal failed to fence old owner: %+v prior=%d", terminal, proof.PriorEpoch)
		}
		proof.TerminalEpoch = terminal.Epoch
		proof.JournalRecords = len(records)
		t.Logf("leaf lease successor: prior_epoch=%d terminal_epoch=%d records=%d", proof.PriorEpoch, proof.TerminalEpoch, len(records))
	}
	if err := trace.Unsubscribe(); err != nil {
		t.Fatal(err)
	}
	final, stopFinal := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopFinal()
	if err := connections[0].FlushWithContext(final); err != nil {
		t.Fatal(err)
	}
	localAfter, err := local.AccountInfo(final)
	if err != nil || localAfter.Domain != leafDomain || localAfter.Streams != 0 {
		t.Fatalf("local leaf after=%+v err=%v", localAfter, err)
	}
	proof.LocalStreamsAfter = localAfter.Streams
	proof.Passed = !t.Failed()
	if proof.Passed {
		t.Logf("leaf domain strict scenario passed local=%s remote=%s runtime_clients=3", leafDomain, hubDomain)
	}
}
