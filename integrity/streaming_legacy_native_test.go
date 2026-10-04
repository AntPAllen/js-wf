//go:build linux

package integrity

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"js-wf/identity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/testcluster"

	"github.com/nats-io/nats.go/jetstream"
)

func TestStreamingAuditNativeLegacy211(t *testing.T) {
	if os.Getenv("WF_AUDIT_BATCH_CANDIDATE") != "1" {
		t.Skip("opt-in full native legacy audit qualification")
	}
	input := os.Getenv("WF_AUDIT_BATCH_LEGACY_BINARY")
	if input == "" {
		t.Skip("set WF_AUDIT_BATCH_LEGACY_BINARY to NATS2.11.17")
	}
	root := candidateNativeRoot(t)
	data, err := os.ReadFile(input)
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(root, "nats-server-2.11.17")
	if err := os.WriteFile(binary, data, 0755); err != nil {
		t.Fatal(err)
	}
	cluster, err := testcluster.StartMixedVersionProcesses(filepath.Join(root, "stores"), []string{binary, binary, binary})
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	for i, nc := range cluster.Clients {
		if nc.ConnectedServerVersion() != "2.11.17" {
			t.Fatalf("node%d version=%s", i, nc.ConnectedServerVersion())
		}
		pid := cluster.Commands[i].Process.Pid
		actual, err := os.ReadFile(fmt.Sprintf("/proc/%d/exe", pid))
		if err != nil {
			t.Fatal(err)
		}
		if sha256.Sum256(actual) != sha256.Sum256(data) {
			t.Fatalf("node%d actual executable differs", i)
		}
		t.Logf("legacy-node node=%d pid=%d version=%s actual_sha256=%x", i, pid, nc.ConnectedServerVersion(), sha256.Sum256(actual))
	}
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(context.Background(), 3*time.Minute)
	defer stop()
	for {
		attempt, done := context.WithTimeout(ctx, 3*time.Second)
		err := provision.EnsureFallback(attempt, js, 3)
		done()
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	batchAuditPublish(t, ctx, js, "legacy-first", batchAuditEntries())
	cutoff := batchAuditPublish(t, ctx, js, "legacy-second", batchAuditEntries())
	snap, err := journal.New(js).SnapshotPrefix(ctx, "audit", "legacy-first", 1)
	if err != nil {
		t.Fatal(err)
	}
	want := Report{Invocations: 2, Journals: 2, Entries: 8, Terminal: 2}
	compareFullAudits(t, ctx, js, nil, want, "")
	// Exercise both newly exposed APIs against the original oracle as well.
	publicCtx, publicStop := context.WithTimeout(ctx, 20*time.Second)
	full, err := CheckWithStreamingStateReads(publicCtx, js)
	publicStop()
	if err != nil || full != want {
		t.Fatalf("public full=%+v err=%v", full, err)
	}
	batchAuditPublish(t, ctx, js, "legacy-later", batchAuditEntries())
	stream, err := js.Stream(ctx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := js.Publish(ctx, identity.JournalSubject("audit", "legacy-later"), []byte("invalid-json")); err != nil {
		t.Fatal(err)
	}
	compareFullAudits(t, ctx, js, &cutoff, want, "")
	publicCtx, publicStop = context.WithTimeout(ctx, 20*time.Second)
	cohort, err := CheckThroughInvocationSequenceWithStreamingStateReads(publicCtx, js, cutoff)
	publicStop()
	if err != nil || cohort != want {
		t.Fatalf("public cohort=%+v err=%v", cohort, err)
	}
	compareFullAudits(t, ctx, js, nil, Report{}, "invalid")
	state, err := js.KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	key := identity.Key("audit", "legacy-first")
	if _, err := state.Put(ctx, key, []byte(`"changed"`)); err != nil {
		t.Fatal(err)
	}
	compareFullAudits(t, ctx, js, &cutoff, Report{}, "terminal state differs")
	if _, err := state.Put(ctx, key, []byte(`"ok"`)); err != nil {
		t.Fatal(err)
	}
	if err := state.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	compareFullAudits(t, ctx, js, &cutoff, Report{}, "terminal state missing")
	if _, err := state.Put(ctx, key, []byte(`"ok"`)); err != nil {
		t.Fatal(err)
	}
	objects, err := js.ObjectStore(ctx, "WF_BLOB")
	if err != nil {
		t.Fatal(err)
	}
	original, err := objects.GetBytes(ctx, snap.Object)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := objects.PutBytes(ctx, snap.Object, []byte("corrupt-snapshot")); err != nil {
		t.Fatal(err)
	}
	compareFullAudits(t, ctx, js, &cutoff, Report{}, "journal gap")
	if _, err := objects.PutBytes(ctx, snap.Object, original); err != nil {
		t.Fatal(err)
	}
	compareFullAudits(t, ctx, js, &cutoff, want, "")
	if err := stream.Purge(ctx, jetstream.WithPurgeSubject(identity.JournalSubject("audit", "legacy-later"))); err != nil {
		t.Fatal(err)
	}
	entry, _ := json.Marshal(batchAuditEntries()[0])
	if _, err := js.Publish(ctx, identity.JournalSubject("audit", "legacy-orphan"), entry); err != nil {
		t.Fatal(err)
	}
	compareFullAudits(t, ctx, js, &cutoff, want, "")
	compareFullAudits(t, ctx, js, nil, Report{}, "journal wf.jrn.audit.legacy-orphan has no invocation")
	t.Log("legacy-full-audit version=2.11.17 replicas=3 compaction/cohort/fresh-terminal/tombstone/snapshot-corruption/orphan/public-apis matched")
}
