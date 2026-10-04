package integrity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"js-wf/identity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/testcluster"

	"github.com/nats-io/nats.go/jetstream"
)

func batchedAuditCluster(t *testing.T) (jetstream.JetStream, context.Context) {
	t.Helper()
	cluster, err := testcluster.Start(candidateNativeRoot(t), 3)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	js, err := jetstream.New(cluster.Clients[0], jetstream.WithPublishAsyncMaxPending(512))
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(stop)
	for {
		attempt, done := context.WithTimeout(ctx, 3*time.Second)
		err := provision.Ensure(attempt, js, 3)
		done()
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	return js, ctx
}
func batchAuditEntries() []journal.Entry {
	return []journal.Entry{
		{Kind: journal.Started, Epoch: 1, Index: 0, WorkerID: "audit-proof"},
		{Kind: journal.StepRequested, Epoch: 1, Index: 1, WorkerID: "audit-proof"},
		{Kind: journal.StepCompleted, Epoch: 1, Index: 2, WorkerID: "audit-proof"},
		{Kind: journal.Completed, Epoch: 1, Index: 3, WorkerID: "audit-proof", Payload: json.RawMessage(`"ok"`)},
	}
}
func batchAuditPublish(t *testing.T, ctx context.Context, js jetstream.JetStream, id string, entries []journal.Entry) uint64 {
	t.Helper()
	inv, err := js.Publish(ctx, identity.InvocationSubject("audit", id), []byte("input"))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		data, err := json.Marshal(entry)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := js.Publish(ctx, identity.JournalSubject("audit", id), data); err != nil {
			t.Fatal(err)
		}
	}
	state, err := js.KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.Put(ctx, identity.Key("audit", id), []byte(`"ok"`)); err != nil {
		t.Fatal(err)
	}
	return inv.Sequence
}
func compareFullAudits(t *testing.T, ctx context.Context, js jetstream.JetStream, cutoff *uint64, want Report, wantError string) {
	t.Helper()
	var point, bulk Report
	var pointErr, bulkErr error
	pointCtx, stop := context.WithTimeout(ctx, 20*time.Second)
	if cutoff == nil {
		point, pointErr = Check(pointCtx, js)
	} else {
		point, pointErr = CheckThroughInvocationSequence(pointCtx, js, *cutoff)
	}
	stop()
	bulkCtx, stop := context.WithTimeout(ctx, 20*time.Second)
	if cutoff == nil {
		bulk, bulkErr = CheckWithBatchedReads(bulkCtx, js)
	} else {
		bulk, bulkErr = CheckThroughInvocationSequenceWithBatchedReads(bulkCtx, js, *cutoff)
	}
	stop()
	if point != bulk {
		t.Fatalf("report mismatch: point=%+v bulk=%+v point_err=%v bulk_err=%v", point, bulk, pointErr, bulkErr)
	}
	if wantError == "" {
		if pointErr != nil || bulkErr != nil || bulk != want {
			t.Fatalf("valid cohort: point=%+v bulk=%+v errors=%v / %v want=%+v", point, bulk, pointErr, bulkErr, want)
		}
	} else if pointErr == nil || bulkErr == nil || !strings.Contains(pointErr.Error(), wantError) || pointErr.Error() != bulkErr.Error() {
		t.Fatalf("different/weakened invariant: point=%v bulk=%v want=%q", pointErr, bulkErr, wantError)
	}
	t.Logf("cohort=%v report=%+v matched_error=%v", cutoff != nil, bulk, bulkErr)
}
func TestBatchedInvariantAuditNativeCompactionCohortAndFreshCorruption(t *testing.T) {
	if os.Getenv("WF_AUDIT_BATCH_CANDIDATE") != "1" {
		t.Skip("opt-in full native batched invariant audit")
	}
	js, ctx := batchedAuditCluster(t)
	batchAuditPublish(t, ctx, js, "first", batchAuditEntries())
	cutoff := batchAuditPublish(t, ctx, js, "second", batchAuditEntries())
	store := journal.New(js)
	snap, err := store.SnapshotPrefix(ctx, "audit", "first", 1)
	if err != nil {
		t.Fatal(err)
	}
	want := Report{Invocations: 2, Journals: 2, Entries: 8, Terminal: 2}
	compareFullAudits(t, ctx, js, nil, want, "")
	// A later malformed journal must be excluded before decoding during cohort
	// audit, while a final full audit must still detect it.
	if _, err := js.Publish(ctx, identity.InvocationSubject("audit", "later"), []byte("later")); err != nil {
		t.Fatal(err)
	}
	if _, err := js.Publish(ctx, identity.JournalSubject("audit", "later"), []byte("invalid-json")); err != nil {
		t.Fatal(err)
	}
	compareFullAudits(t, ctx, js, &cutoff, want, "")
	compareFullAudits(t, ctx, js, nil, Report{}, "invalid")
	state, err := js.KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.Put(ctx, "audit.first", []byte(`"changed"`)); err != nil {
		t.Fatal(err)
	}
	compareFullAudits(t, ctx, js, &cutoff, Report{}, "terminal state differs")
	if _, err := state.Put(ctx, "audit.first", []byte(`"ok"`)); err != nil {
		t.Fatal(err)
	}
	if err := state.Delete(ctx, "audit.second"); err != nil {
		t.Fatal(err)
	}
	compareFullAudits(t, ctx, js, &cutoff, Report{}, "terminal state missing")
	if _, err := state.Put(ctx, "audit.second", []byte(`"ok"`)); err != nil {
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
	// The full final audit, rather than a cohort-only check, covers orphans.
	stream, err := js.Stream(ctx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Purge(ctx, jetstream.WithPurgeSubject(identity.JournalSubject("audit", "later"))); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(batchAuditEntries()[0])
	if _, err := js.Publish(ctx, identity.JournalSubject("audit", "orphan"), data); err != nil {
		t.Fatal(err)
	}
	compareFullAudits(t, ctx, js, &cutoff, want, "")
	compareFullAudits(t, ctx, js, nil, Report{}, "has no invocation")
	for _, name := range []string{"WF_INV", "WF_JRN"} {
		stream, err := js.Stream(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		info, err := stream.Info(ctx)
		if err != nil || info.State.Consumers != 0 {
			t.Fatalf("cleanup %s err=%v info=%+v", name, err, info)
		}
	}
}
func TestBatchedInvariantAuditNativeJournalCorruption(t *testing.T) {
	if os.Getenv("WF_AUDIT_BATCH_CANDIDATE") != "1" {
		t.Skip("opt-in full native batched invariant audit")
	}
	js, ctx := batchedAuditCluster(t)
	for _, tc := range []struct {
		name, want string
		mutate     func([]journal.Entry)
	}{
		{"epoch-owner", "used by workers", func(e []journal.Entry) { e[2].WorkerID = "other-owner" }},
		{"index-gap", "index 9 at position 2", func(e []journal.Entry) { e[2].Index = 9 }},
		{"descending-epoch", "epoch or Started violation", func(e []journal.Entry) { e[2].Epoch = 0 }},
		{"unresolved-terminal", "successful terminal with unresolved request", func(e []journal.Entry) {
			e[2].Kind = journal.Attempt
			e[2].Payload = json.RawMessage(`{"count":1,"error":"panic"}`)
		}},
		{"completion-without-request", "completion without request", func(e []journal.Entry) { e[1].Kind = journal.StepCompleted }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entries := batchAuditEntries()
			tc.mutate(entries)
			batchAuditPublish(t, ctx, js, tc.name, entries)
			compareFullAudits(t, ctx, js, nil, Report{}, tc.want)
			inv, _ := js.Stream(ctx, "WF_INV")
			jrn, _ := js.Stream(ctx, "WF_JRN")
			if err := inv.Purge(ctx, jetstream.WithPurgeSubject(identity.InvocationSubject("audit", tc.name))); err != nil {
				t.Fatal(err)
			}
			if err := jrn.Purge(ctx, jetstream.WithPurgeSubject(identity.JournalSubject("audit", tc.name))); err != nil {
				t.Fatal(err)
			}
		})
	}
	inv, err := js.Stream(ctx, "WF_INV")
	if err != nil {
		t.Fatal(err)
	}
	info, err := inv.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cfg := info.Config
	cfg.MaxMsgsPerSubject = -1
	cfg.DiscardNewPerSubject = false
	if _, err := js.UpdateStream(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	batchAuditPublish(t, ctx, js, "duplicate", batchAuditEntries())
	if _, err := js.Publish(ctx, identity.InvocationSubject("audit", "duplicate"), []byte("duplicate-input")); err != nil {
		t.Fatal(err)
	}
	compareFullAudits(t, ctx, js, nil, Report{}, "duplicate invocation")
}
func TestBatchedInvariantAuditNativeThreeThousandCohort(t *testing.T) {
	runBatchedInvariantAuditNativeCohort(t, 3000)
}

func TestBatchedInvariantAuditNativeTwelveThousandCohort(t *testing.T) {
	runBatchedInvariantAuditNativeCohort(t, 12000)
}

func runBatchedInvariantAuditNativeCohort(t *testing.T, count int) {
	t.Helper()
	if os.Getenv("WF_AUDIT_BATCH_CANDIDATE") != "1" {
		t.Skip("opt-in full native batched invariant audit")
	}
	js, ctx := batchedAuditCluster(t)
	futures := make([]jetstream.PubAckFuture, 0, count*13)
	state, err := js.KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("large-%06d", i)
		future, err := js.PublishAsync(identity.InvocationSubject("audit", id), []byte("input"))
		if err != nil {
			t.Fatal(err)
		}
		futures = append(futures, future)
		entries := []journal.Entry{{Index: 0, Epoch: 1, Kind: journal.Started, WorkerID: "audit-proof"}}
		for n := uint64(0); n < 5; n++ {
			entries = append(entries, journal.Entry{Index: 1 + 2*n, Epoch: 1, Kind: journal.StepRequested, WorkerID: "audit-proof"}, journal.Entry{Index: 2 + 2*n, Epoch: 1, Kind: journal.StepCompleted, WorkerID: "audit-proof"})
		}
		entries = append(entries, journal.Entry{Index: 11, Epoch: 1, Kind: journal.Completed, WorkerID: "audit-proof", Payload: json.RawMessage(`"ok"`)})
		for _, entry := range entries {
			data, _ := json.Marshal(entry)
			future, err := js.PublishAsync(identity.JournalSubject("audit", id), data)
			if err != nil {
				t.Fatal(err)
			}
			futures = append(futures, future)
		}
		if _, err := state.Put(ctx, identity.Key("audit", id), []byte(`"ok"`)); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	for _, future := range futures {
		select {
		case <-future.Ok():
		case err := <-future.Err():
			t.Fatal(err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	want := Report{Invocations: count, Journals: count, Entries: count * 12, Terminal: count}
	started := time.Now()
	pointCtx, done := context.WithTimeout(ctx, 20*time.Second)
	point, pointErr := Check(pointCtx, js)
	done()
	pointElapsed := time.Since(started)
	started = time.Now()
	previousCtx, done := context.WithTimeout(ctx, 20*time.Second)
	previous, previousErr := checkUsing(previousCtx, js, nil, func(ctx context.Context, stream jetstream.Stream, cutoff *uint64, visit func(*jetstream.RawStreamMsg) error) error {
		return scanBatchThroughWithSize(ctx, stream, cutoff, visit, 512)
	})
	done()
	previousElapsed := time.Since(started)
	if previousErr == nil {
		if previous != want {
			t.Fatalf("previous bulk report=%+v", previous)
		}
	} else if !errors.Is(previousErr, context.DeadlineExceeded) {
		t.Fatalf("unexpected previous bulk failure %v", previousErr)
	}
	started = time.Now()
	bulkCtx, done := context.WithTimeout(ctx, 20*time.Second)
	bulk, bulkErr := CheckWithBatchedReads(bulkCtx, js)
	done()
	bulkElapsed := time.Since(started)
	if bulkErr != nil || bulk != want {
		t.Fatalf("full bulk report=%+v err=%v", bulk, bulkErr)
	}
	if pointErr == nil {
		if point != want {
			t.Fatalf("point report=%+v", point)
		}
	} else if !errors.Is(pointErr, context.DeadlineExceeded) {
		t.Fatalf("unexpected point failure %v", pointErr)
	}
	t.Logf("full-audit invocations=%d entries=%d terminal=%d point_elapsed=%s point_error=%v previous_batch512_elapsed=%s previous_batch512_error=%v bulk_batch4096_elapsed=%s bulk_error=%v", bulk.Invocations, bulk.Entries, bulk.Terminal, pointElapsed, pointErr, previousElapsed, previousErr, bulkElapsed, bulkErr)
}
