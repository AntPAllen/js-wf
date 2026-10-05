package integration_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/provision"
	"js-wf/testcluster"
)

// Real domain routing, not a substituted Options() value on a default server.
// Reuse the strict retirement/reuse scenario including lost fresh manifest,
// old generation rejection, quiescent collection and shared object survival.
func TestContinuationRetirementReuseInJetStreamDomain(t *testing.T) {
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
		all[node], err = jetstream.NewWithDomain(cluster.Clients[node], domain)
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
	runContinuationRetirementOnCluster(t, all, true, nil)
	if t.Failed() {
		return
	}
	if err := os.WriteFile(filepath.Join(root, "scenario-passed.txt"), []byte("Original strict manifest-loss retirement/reuse scenario passed in WFRETIRE domain\n"), 0600); err != nil {
		t.Fatal(err)
	}
}
