//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
			runContinuationRetirementProcessFault(t, full, false)
		})
	}
}

func TestContinuationRetirementReuseWithManifestLossAndLeaseExpiryAcrossServerSIGKILL(t *testing.T) {
	runContinuationRetirementProcessFault(t, true, true)
}

func runContinuationRetirementProcessFault(t *testing.T, full, expireLease bool) {
	t.Helper()
	if expireLease && !full {
		t.Fatal("lease expiry requires every server stopped")
	}
	root := t.TempDir()
	if retained := os.Getenv("WF_CONTINUATION_RETIREMENT_PROCESS_ROOT"); retained != "" {
		root = filepath.Join(retained, fmt.Sprintf("all-servers-%t-lease-expiry-%t", full, expireLease))
		if err := os.Mkdir(root, 0700); err != nil {
			t.Fatal(err)
		}
	}
	cluster, err := testcluster.StartProcesses(root, 3)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	all := make([]jetstream.JetStream, 3)
	for node := range all {
		nc, err := nats.Connect(cluster.ClientURL(node), nats.MaxReconnects(-1), nats.ReconnectWait(25*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(nc.Close)
		all[node], err = jetstream.New(nc)
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
		provisionErr = provision.Ensure(attempt, all[0], 3)
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
	var previousEpoch uint64
	var leaseTTL time.Duration
	runContinuationRetirementOnCluster(t, all, true, func(ctx context.Context) error {
		stream, err := all[0].Stream(ctx, "KV_WF_STATE")
		if err != nil {
			return err
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
			t.Logf("retirement restarted node=%d pid=%d", node, cluster.Commands[node].Process.Pid)
		}
		return nil
	})
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
