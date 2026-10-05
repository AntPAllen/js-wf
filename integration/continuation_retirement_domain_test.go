package integration_test

import (
	"context"
	"encoding/json"
	"errors"
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
		onDrop = func(ctx context.Context) (result error) {
			cut := struct {
				Leader       string              `json:"state_leader"`
				Before       []map[string]string `json:"before"`
				Stopped      []string            `json:"stopped"`
				Restarted    []string            `json:"restarted"`
				Healed       []map[string]string `json:"healed"`
				Started      time.Time           `json:"started"`
				Finished     time.Time           `json:"finished"`
				Stage        string              `json:"stage"`
				Error        string              `json:"error,omitempty"`
				FailureState []map[string]string `json:"failure_state,omitempty"`
				Scope        string              `json:"scope"`
			}{Before: identities, Started: time.Now().UTC(), Stage: "state-leader", Scope: "All three actual library servers shutdown and restarted; not SIGKILL or lease-expiry admission"}
			defer func() {
				cut.Finished = time.Now().UTC()
				if result != nil {
					cut.Error = result.Error()
					for node := 0; node < 3; node++ {
						configured := ""
						if config := cluster.Servers[node].JetStreamConfig(); config != nil {
							configured = config.Domain
						}
						connection := all[node].Conn()
						cut.FailureState = append(cut.FailureState, map[string]string{
							"name": cluster.Servers[node].Name(), "id": cluster.Servers[node].ID(),
							"running":           strconv.FormatBool(cluster.Servers[node].Running()),
							"meta_leader":       strconv.FormatBool(cluster.Servers[node].JetStreamIsLeader()),
							"configured_domain": configured, "connection_status": connection.Status().String(), "connected_url": connection.ConnectedUrl(),
						})
					}
				}
				data, err := json.MarshalIndent(cut, "", "  ")
				if err == nil {
					err = os.WriteFile(filepath.Join(root, "domain-all-server-restart.json"), data, 0600)
				}
				result = errors.Join(result, err)
				t.Logf("domain manifest cut: stage=%s state_leader=%s stopped=%d restarted=%d healed=%d elapsed=%s error=%v", cut.Stage, cut.Leader, len(cut.Stopped), len(cut.Restarted), len(cut.Healed), cut.Finished.Sub(cut.Started), result)
			}()
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
			cut.Leader, cut.Stage = info.Cluster.Leader, "stopping"
			for node := 0; node < 3; node++ {
				cluster.KillNode(node)
				if cluster.Servers[node].Running() {
					return fmt.Errorf("domain node%d remained running", node)
				}
				cut.Stopped = append(cut.Stopped, cluster.Servers[node].ID())
			}
			// Every original must be stopped before any replacement begins.
			cut.Stage = "restarting"
			for node := 0; node < 3; node++ {
				if err := cluster.RestartNode(node); err != nil {
					return err
				}
				cut.Restarted = append(cut.Restarted, cluster.Servers[node].ID())
			}
			cut.Stage = "checking-domain"
			observe, stopObserve := context.WithTimeout(context.Background(), 5*time.Second)
			defer stopObserve()
			for node := 0; node < 3; node++ {
				// One request issued during reconnect may exhaust its context.
				// Retry this read only within the original shared5s observation
				// budget; a successful wrong-domain response is immediately fatal.
				for {
					attempt, finish := context.WithTimeout(observe, 250*time.Millisecond)
					account, err := all[node].AccountInfo(attempt)
					finish()
					if err == nil {
						if account.Domain != domain {
							return fmt.Errorf("restarted domain node%d: wrong domain %+v", node, account)
						}
						cut.Healed = append(cut.Healed, map[string]string{"name": cluster.Servers[node].Name(), "id": cluster.Servers[node].ID(), "domain": account.Domain})
						break
					}
					if observe.Err() != nil || !matrixTransientTransport(err) {
						return fmt.Errorf("restarted domain node%d: err=%v observation=%v", node, err, observe.Err())
					}
					select {
					case <-observe.Done():
					case <-time.After(25 * time.Millisecond):
					}
				}
			}
			cut.Stage = "healed"
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
