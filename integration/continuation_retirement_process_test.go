//go:build linux

package integration_test

import (
	"context"
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
	"js-wf/provision"
	"js-wf/testcluster"
)

func TestContinuationRetirementReuseWithManifestLossAndServerSIGKILL(t *testing.T) {
	for _, full := range []bool{false, true} {
		t.Run(fmt.Sprintf("all_servers=%t", full), func(t *testing.T) {
			root := t.TempDir()
			if retained := os.Getenv("WF_CONTINUATION_RETIREMENT_PROCESS_ROOT"); retained != "" {
				root = filepath.Join(retained, fmt.Sprintf("all-servers-%t", full))
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
				for _, node := range nodes {
					if err := cluster.RestartNode(node); err != nil {
						return err
					}
					t.Logf("retirement restarted node=%d pid=%d", node, cluster.Commands[node].Process.Pid)
				}
				return nil
			})
		})
	}
}
