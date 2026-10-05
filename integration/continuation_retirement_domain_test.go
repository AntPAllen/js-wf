package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/provision"
	"js-wf/testcluster"
)

// Real domain routing, not a substituted Options() value on a default server.
// Reuse the strict retirement/reuse scenario including lost fresh manifest,
// old generation rejection, quiescent collection and shared object survival.
func TestContinuationRetirementReuseInJetStreamDomain(t *testing.T) {
	runContinuationRetirementDomain(t, false)
}

func TestContinuationRetirementReuseInJetStreamDomainWithAllServerRestart(t *testing.T) {
	runContinuationRetirementDomain(t, true)
}

func runContinuationRetirementDomain(t *testing.T, restart bool) {
	t.Helper()
	root := os.Getenv("WF_CONTINUATION_DOMAIN_ROOT")
	if root == "" {
		t.Skip("set WF_CONTINUATION_DOMAIN_ROOT to a fresh absolute artifact directory")
	}
	if !filepath.IsAbs(root) {
		t.Fatal("artifact directory must be absolute")
	}
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	const domain = "WFRETIRE"
	cluster, err := testcluster.StartWithDomain(filepath.Join(root, "cluster"), 3, domain)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	all := make([]jetstream.JetStream, 3)
	for node := range all {
		connection := cluster.Clients[node]
		if restart {
			connection, err = nats.Connect(cluster.Servers[node].ClientURL(), nats.MaxReconnects(-1), nats.ReconnectWait(25*time.Millisecond), nats.IgnoreDiscoveredServers())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(connection.Close)
		}
		all[node], err = jetstream.NewWithDomain(connection, domain)
		if err != nil {
			t.Fatal(err)
		}
	}
	ready, stop := context.WithTimeout(context.Background(), 30*time.Second)
	defer stop()
	for ready.Err() == nil {
		attempt, finish := context.WithTimeout(ready, 4*time.Second)
		err = provision.Ensure(attempt, all[0], 3)
		finish()
		if err == nil {
			break
		}
		select {
		case <-ready.Done():
		case <-time.After(50 * time.Millisecond):
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := waitMatrixWorkflowReplicas(ready, all[0]); err != nil {
		t.Fatal(err)
	}
	// Query each pinned server, requiring the server's actual domain in INFO.
	var identities []map[string]string
	for node := range all {
		info, err := all[node].AccountInfo(ready)
		if err != nil || info.Domain != domain {
			t.Fatalf("node%d actual domain=%+v err=%v", node, info, err)
		}
		identities = append(identities, map[string]string{"name": cluster.Servers[node].Name(), "id": cluster.Servers[node].ID(), "domain": info.Domain})
	}
	data, err := json.MarshalIndent(identities, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "domain-admission.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	var onDrop func(context.Context) error
	if restart {
		onDrop = func(ctx context.Context) error {
			stream, err := all[0].Stream(ctx, "KV_WF_STATE")
			if err != nil {
				return err
			}
			info, err := stream.Info(ctx)
			if err != nil || info.Cluster == nil || info.Cluster.Leader == "" {
				return fmt.Errorf("domain state leader unconfirmed: info=%+v err=%v", info, err)
			}
			leader, err := strconv.Atoi(strings.TrimPrefix(info.Cluster.Leader, "wf-test-"))
			if err != nil || leader < 0 || leader >= 3 {
				return fmt.Errorf("invalid domain state leader %q", info.Cluster.Leader)
			}
			cut := struct {
				Leader   string              `json:"state_leader"`
				Before   []map[string]string `json:"before"`
				Stopped  []string            `json:"stopped"`
				Healed   []map[string]string `json:"healed"`
				Started  time.Time           `json:"started"`
				Finished time.Time           `json:"finished"`
				Scope    string              `json:"scope"`
			}{Leader: info.Cluster.Leader, Before: identities, Started: time.Now().UTC(), Scope: "All three actual library servers shutdown and restarted; not SIGKILL or lease-expiry admission"}
			for node := 0; node < 3; node++ {
				cluster.KillNode(node)
				if cluster.Servers[node].Running() {
					return fmt.Errorf("domain node%d remained running", node)
				}
				cut.Stopped = append(cut.Stopped, cluster.Servers[node].ID())
			}
			// Every original must be stopped before any replacement begins.
			for node := 0; node < 3; node++ {
				if err := cluster.RestartNode(node); err != nil {
					return err
				}
			}
			observe, stopObserve := context.WithTimeout(context.Background(), 5*time.Second)
			defer stopObserve()
			for node := 0; node < 3; node++ {
				account, err := all[node].AccountInfo(observe)
				if err != nil || account.Domain != domain {
					return fmt.Errorf("restarted domain node%d: info=%+v err=%v", node, account, err)
				}
				cut.Healed = append(cut.Healed, map[string]string{"name": cluster.Servers[node].Name(), "id": cluster.Servers[node].ID(), "domain": account.Domain})
			}
			cut.Finished = time.Now().UTC()
			data, err := json.MarshalIndent(cut, "", "  ")
			if err != nil {
				return err
			}
			if err := os.WriteFile(filepath.Join(root, "domain-all-server-restart.json"), data, 0600); err != nil {
				return err
			}
			t.Logf("domain fresh manifest response lost; state_leader=%s all_three_stopped=true restart=%s", cut.Leader, cut.Finished.Sub(cut.Started))
			return nil
		}
	}
	runContinuationRetirementOnCluster(t, all, true, onDrop)
	if t.Failed() {
		return
	}
	if err := os.WriteFile(filepath.Join(root, "scenario-passed.txt"), []byte("Original strict manifest-loss retirement/reuse scenario passed in WFRETIRE domain\n"), 0600); err != nil {
		t.Fatal(err)
	}
}
