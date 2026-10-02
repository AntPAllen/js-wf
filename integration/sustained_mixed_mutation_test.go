//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/integrity"
	"js-wf/testcluster"
)

func TestSustainedMixedMutationAfterJournalLeaderKills(t *testing.T) {
	mode := os.Getenv("WF_SUSTAINED_MUTATION")
	if mode == "" {
		t.Skip("set WF_SUSTAINED_MUTATION for a sustained production mutation challenge")
	}
	switch mode {
	case "determinism", "leases", "cas", "enqueue", "start-repair", "purge":
	default:
		t.Fatalf("unknown sustained mutation mode %q", mode)
	}
	runMixedMatrixLeaderWithChallenge(t, "journal_leader", mode)
}

type sustainedMutationAdmission struct {
	Mode             string           `json:"mode"`
	Seed             int64            `json:"seed"`
	Duration         time.Duration    `json:"duration_ns"`
	Elapsed          time.Duration    `json:"elapsed_ns"`
	ReleaseDuration  bool             `json:"release_duration"`
	Batches          int              `json:"batches"`
	Faults           int              `json:"faults"`
	InvocationCutoff uint64           `json:"invocation_cutoff"`
	Retained         integrity.Report `json:"retained"`
	SameStores       bool             `json:"same_stores"`
}

// The selected source mutation has been active throughout the original mixed
// row, including its workers, scanners, actual faults, checkpoints, histories,
// p99 gates and physical queue drain. Add the controlled live counterexample
// without replacing the processes, file stores or completed retained workload.
func challengeSustainedMixedMutation(t *testing.T, ctx context.Context, cluster *testcluster.ProcessCluster, js jetstream.JetStream, mode string, seed int64, duration, elapsed time.Duration, batches, faults int, cutoff uint64, before integrity.Report) {
	t.Helper()
	if elapsed < duration || batches == 0 || faults == 0 || before.Invocations != batches*28 || before.Terminal != before.Invocations {
		t.Fatal("incomplete sustained row cannot admit a mutation challenge")
	}
	admission := sustainedMutationAdmission{mode, seed, duration, elapsed, duration == 10*time.Minute, batches, faults, cutoff, before, true}
	encoded, err := json.Marshal(admission)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("MIXED_SUSTAINED_ADMISSION %s", encoded)
	// Even a semantically rejected mutant must leave the original completed
	// cohort intact. The high-water audit excludes newly admitted guard IDs;
	// no records are deleted or repaired to make this audit pass.
	defer func() {
		auditCtx, stop := context.WithTimeout(ctx, 30*time.Second)
		defer stop()
		after, err := matrixRetainedAuditUsing(auditCtx, func(attempt context.Context) (integrity.Report, error) {
			return integrity.CheckThroughInvocationSequence(attempt, js, cutoff)
		})
		if err != nil || after != before {
			t.Errorf("original sustained retained cohort changed: before=%+v after=%+v err=%v", before, after, err)
			return
		}
		t.Logf("MIXED_SUSTAINED_PRESERVED cutoff=%d report=%+v", cutoff, after)
	}()
	nodes := make([]jetstream.JetStream, len(cluster.Clients))
	for index, connection := range cluster.Clients {
		nodes[index], err = jetstream.New(connection)
		if err != nil {
			t.Fatal(err)
		}
	}
	runMixedGuardChallengeOnCluster(t, ctx, cluster, nodes, mode, before.Invocations)
}
