//go:build linux

package integrity

import (
	"context"
	"encoding/json"
	"errors"
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

// Diagnostic only: callers supply verified disposable copies of closed stores.
// It compares the existing per-request retry wrapper with the original overall
// audit deadline for one fresh state snapshot, without changing production.
func stateSnapshotCopiedFixture(t *testing.T) (context.Context, jetstream.JetStream, jetstream.KeyValue, *testcluster.DockerCluster, string) {
	t.Helper()
	storesRoot := os.Getenv("WF_AUDIT_STATE_PROFILE_STORES")
	if storesRoot == "" {
		t.Skip("opt-in verified copied-store state snapshot diagnostic")
	}
	if !filepath.IsAbs(storesRoot) {
		t.Fatal("copied-store root must be absolute")
	}
	root := candidateNativeRoot(t)
	stores := map[int]string{}
	for i := 0; i < 5; i++ {
		stores[i] = filepath.Join(storesRoot, fmt.Sprintf("node-%d", i))
	}
	cluster, err := testcluster.StartDockerClusterWithRestoredIdentity(filepath.Join(root, "cluster"), 5, stores, os.Getenv("WF_AUDIT_STATE_PROFILE_IDENTITY"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	t.Cleanup(func() {
		for i := 0; i < 5; i++ {
			logs, err := cluster.Logs(i)
			if err != nil {
				t.Error(err)
				continue
			}
			if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("server-%d.log", i)), []byte(logs), 0644); err != nil {
				t.Error(err)
			}
		}
	})
	urls := []string{}
	for i := 0; i < 5; i++ {
		urls = append(urls, cluster.ClientURL(i))
	}
	nc, err := nats.Connect(strings.Join(urls, ","), nats.MaxReconnects(-1), nats.ReconnectWait(20*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	var kv jetstream.KeyValue
	for {
		call, stop := context.WithTimeout(ctx, 2*time.Second)
		kv, err = js.KeyValue(call, "WF_STATE")
		if err == nil {
			var stream jetstream.Stream
			stream, err = js.Stream(call, "KV_WF_STATE")
			if err == nil {
				info := stream.CachedInfo()
				if info.Config.Replicas != 5 || info.Cluster == nil || info.Cluster.Leader == "" {
					err = errors.New("R5 state not ready")
				}
			}
		}
		stop()
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("state readiness: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	return ctx, js, kv, cluster, root
}

func TestRetainedStateSnapshotCopiedStoreDiagnostic(t *testing.T) {
	ctx, _, kv, _, root := stateSnapshotCopiedFixture(t)
	var err error
	type result struct {
		Label         string `json:"label"`
		ElapsedNS     int64  `json:"elapsed_ns"`
		Values        int    `json:"values"`
		Error         string `json:"error"`
		DeadlineError bool   `json:"deadline_error"`
	}
	results := []result{}
	for _, label := range []string{"existing-2s-three-attempts", "overall-20s-diagnostic", "existing-2s-recheck"} {
		call, stop := context.WithTimeout(ctx, 20*time.Second)
		began := time.Now()
		read := func(attempt context.Context) (jetstream.KeyValue, error) {
			return initialAuditState(attempt, kv, func(string) bool { return true })
		}
		var value jetstream.KeyValue
		if label == "overall-20s-diagnostic" {
			value, err = read(call)
		} else {
			value, err = auditRead(call, read)
		}
		r := result{Label: label, ElapsedNS: time.Since(began).Nanoseconds(), Error: fmt.Sprint(err), DeadlineError: errors.Is(err, context.DeadlineExceeded)}
		stop()
		if err == nil {
			snapshot, ok := value.(*auditStateSnapshot)
			if !ok {
				t.Fatal("unexpected state result")
			}
			r.Values = len(snapshot.values)
		}
		results = append(results, r)
		data, _ := json.MarshalIndent(results, "", "  ")
		if writeErr := os.WriteFile(filepath.Join(root, "state-snapshot-comparisons.json"), append(data, '\n'), 0644); writeErr != nil {
			t.Fatal(writeErr)
		}
		t.Logf("state-snapshot diagnostic=%s elapsed=%s values=%d err=%v", label, time.Duration(r.ElapsedNS), r.Values, err)
		if err != nil && !r.DeadlineError {
			t.Fatal(err)
		}
	}
	// A diagnostic PASS means observations were produced, not that an audit or
	// fault-recovery gate passed. Each recorded variant has its own verdict.
}
