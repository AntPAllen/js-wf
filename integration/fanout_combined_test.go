package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/journal"
	"js-wf/testcluster"
)

// Full boundary positions and the original 500-child/five-minute contract.
// Library journal leader restart is combined with an actual parent SIGKILL.
func TestFiveHundredChildFanoutCombinedParentAndJournalBoundaryMatrix(t *testing.T) {
	if os.Getenv("WF_FANOUT_COMBINED_ROOT") == "" {
		t.Skip("opt-in retained combined full 500-child boundary matrix")
	}
	for _, phase := range []string{"create", "results"} {
		for _, position := range []string{"first", "interior", "last"} {
			t.Run(phase+"/"+position, func(t *testing.T) {
				cut := ""
				if position == "first" {
					cut = "0"
				}
				if position == "last" {
					cut = "499"
				}
				t.Setenv("WF_FANOUT_CHILD_CUT", "")
				t.Setenv("WF_FANOUT_RESULT_CUT", "")
				if phase == "create" {
					t.Setenv("WF_FANOUT_CHILD_CUT", cut)
				} else {
					t.Setenv("WF_FANOUT_RESULT_CUT", cut)
				}
				runFiveHundredChildFanout(t, true, phase == "create", phase == "results")
			})
		}
	}
}
func captureFanoutParentSDK(t *testing.T, cmd *exec.Cmd, root string, prefix []journal.Record) {
	t.Helper()
	live := fmt.Sprintf("/proc/%d/exe", cmd.Process.Pid)
	file, err := os.Open(live)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		t.Fatal(err)
	}
	parent, err := os.Open(os.Args[0])
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	expected := sha256.New()
	if _, err := io.Copy(expected, parent); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%x", digest.Sum(nil)) != fmt.Sprintf("%x", expected.Sum(nil)) {
		t.Fatal("fanout child SDK differs from parent")
	}
	build, err := exec.Command("go", "version", "-m", live).Output()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.MarshalIndent(map[string]any{"pid": cmd.Process.Pid, "actual_sdk_sha256": fmt.Sprintf("%x", digest.Sum(nil)), "actual_sdk_build_info": string(build), "observed_before_kill": time.Now().UTC(), "durable_prefix": prefix}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "actual-parent-sdk.json"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}
func restartFanoutJournalAtBoundary(t *testing.T, ctx context.Context, all []jetstream.JetStream, cluster *testcluster.Cluster, root, phase string, prefix []journal.Record) {
	t.Helper()
	stream, err := all[0].Stream(ctx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	before, err := stream.Info(ctx)
	if err != nil || before.Cluster == nil {
		t.Fatal("journal leader unavailable", err)
	}
	node := -1
	for i, server := range cluster.Servers {
		if server.Name() == before.Cluster.Leader {
			node = i
		}
	}
	if node < 0 {
		t.Fatal("unknown journal leader", before.Cluster.Leader)
	}
	if len(prefix) == 0 || before.State.LastSeq < prefix[len(prefix)-1].Sequence {
		t.Fatal("journal does not cover captured parent prefix")
	}
	oldID := cluster.Servers[node].ID()
	began := time.Now().UTC()
	nodes := [3]jetstream.JetStream{all[0], all[1], all[2]}
	if err := restartJournalLeader(ctx, &nodes, cluster, before.State.Msgs); err != nil {
		t.Fatal(err)
	}
	copy(all, nodes[:])
	if cluster.Servers[node].ID() == oldID || !cluster.Servers[node].Running() {
		t.Fatal("journal library restart not confirmed")
	}
	afterStream, err := all[0].Stream(ctx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	after, err := afterStream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after.State.Msgs != before.State.Msgs || after.State.LastSeq != before.State.LastSeq {
		t.Fatal("journal changed across quiescent boundary restart")
	}
	data, err := json.MarshalIndent(map[string]any{"phase": phase, "node": node, "old_server_id": oldID, "new_server_id": cluster.Servers[node].ID(), "started": began, "healed": time.Now().UTC(), "before": before, "after": after, "library_restart_not_server_sigkill": true, "captured_prefix_tail_sequence": prefix[len(prefix)-1].Sequence}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "journal-restart.json"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("combined journal restart phase=%s node=%d messages=%d tail_seq=%d prefix_tail_seq=%d", phase, node, before.State.Msgs, before.State.LastSeq, prefix[len(prefix)-1].Sequence)
}
