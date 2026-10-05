package integrity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
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
	js, ctx, _ := batchedAuditClusterWithServers(t)
	return js, ctx
}

func batchedAuditClusterWithServers(t *testing.T) (jetstream.JetStream, context.Context, *testcluster.Cluster) {
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
	return js, ctx, cluster
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
	stateCtx, stateStop := context.WithTimeout(ctx, 20*time.Second)
	withState, stateErr := checkUsingState(stateCtx, js, cutoff, scanBatchThrough, true)
	stateStop()
	if withState != point || (stateErr == nil) != (pointErr == nil) || stateErr != nil && stateErr.Error() != pointErr.Error() {
		t.Fatalf("state snapshot mismatch: point=%+v state=%+v errors=%v / %v", point, withState, pointErr, stateErr)
	}
	for _, snapshot := range []bool{false, true} {
		streamCtx, streamStop := context.WithTimeout(ctx, 20*time.Second)
		var streamed Report
		var streamErr error
		if cutoff == nil {
			if snapshot {
				streamed, streamErr = CheckWithStreamingStateReads(streamCtx, js)
			} else {
				streamed, streamErr = CheckWithStreamingReads(streamCtx, js)
			}
		} else if snapshot {
			streamed, streamErr = CheckThroughInvocationSequenceWithStreamingStateReads(streamCtx, js, *cutoff)
		} else {
			streamed, streamErr = CheckThroughInvocationSequenceWithStreamingReads(streamCtx, js, *cutoff)
		}
		streamStop()
		if streamed != point || (streamErr == nil) != (pointErr == nil) || streamErr != nil && streamErr.Error() != pointErr.Error() {
			t.Fatalf("streamed snapshot=%v mismatch: point=%+v streamed=%+v errors=%v / %v", snapshot, point, streamed, pointErr, streamErr)
		}
	}
	concurrentCtx, concurrentStop := context.WithTimeout(ctx, 20*time.Second)
	concurrent, concurrentErr := checkUsingConcurrentOptions(concurrentCtx, js, cutoff, scanByteBoundedThrough, true, true, true)
	concurrentStop()
	if concurrent != point || (concurrentErr == nil) != (pointErr == nil) || concurrentErr != nil && concurrentErr.Error() != pointErr.Error() {
		t.Fatalf("concurrent state mismatch: point=%+v concurrent=%+v errors=%v / %v", point, concurrent, pointErr, concurrentErr)
	}
	compactCtx, compactStop := context.WithTimeout(ctx, 20*time.Second)
	compact, compactErr := checkUsingConcurrentOptions(compactCtx, js, cutoff, scanCompactByteBoundedThrough, true, true, true)
	compactStop()
	if compact != point || (compactErr == nil) != (pointErr == nil) || compactErr != nil && compactErr.Error() != pointErr.Error() {
		t.Fatalf("compact metadata mismatch: point=%+v compact=%+v errors=%v / %v", point, compact, pointErr, compactErr)
	}
	callbackCtx, callbackStop := context.WithTimeout(ctx, 20*time.Second)
	callback, callbackErr := checkUsingConcurrentOptions(callbackCtx, js, cutoff, scanConsumeByteBoundedThrough, true, true, true)
	callbackStop()
	if callback != point || (callbackErr == nil) != (pointErr == nil) || callbackErr != nil && callbackErr.Error() != pointErr.Error() {
		t.Fatalf("callback delivery mismatch: point=%+v callback=%+v errors=%v / %v", point, callback, pointErr, callbackErr)
	}
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

func batchAuditLargeCohort(t *testing.T, count int) (jetstream.JetStream, context.Context, Report) {
	t.Helper()
	js, ctx, want, _ := batchAuditLargeCohortWithServers(t, count)
	return js, ctx, want
}

func batchAuditLargeCohortWithServers(t *testing.T, count int) (jetstream.JetStream, context.Context, Report, *testcluster.Cluster) {
	t.Helper()
	if os.Getenv("WF_AUDIT_BATCH_CANDIDATE") != "1" {
		t.Skip("opt-in full native batched invariant audit")
	}
	js, ctx, cluster := batchedAuditClusterWithServers(t)
	return js, ctx, batchAuditPopulateLargeCohort(t, js, ctx, count), cluster
}

func batchAuditPopulateLargeCohort(t *testing.T, js jetstream.JetStream, ctx context.Context, count int) Report {
	t.Helper()
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
	return want
}

func runBatchedInvariantAuditNativeCohort(t *testing.T, count int) {
	t.Helper()
	js, ctx, want := batchAuditLargeCohort(t, count)
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
	bulk, bulkErr := checkUsing(bulkCtx, js, nil, func(ctx context.Context, stream jetstream.Stream, cutoff *uint64, visit func(*jetstream.RawStreamMsg) error) error {
		return scanBatchThroughWithSize(ctx, stream, cutoff, visit, 4096)
	})
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

// Test-only override: source streams retain all three replicas. The temporary
// audit consumer's delivery state is the sole difference in this comparison.
// This does not qualify single-replica consumer recovery after leader loss.
type nativeSingleReplicaAuditStream struct {
	jetstream.Stream
	observed *[]string
}

func (s nativeSingleReplicaAuditStream) CreateConsumer(ctx context.Context, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	if cfg.Replicas != 3 || !cfg.MemoryStorage || cfg.AckPolicy != jetstream.AckNonePolicy {
		return nil, fmt.Errorf("unexpected baseline audit consumer config: %+v", cfg)
	}
	cfg.Replicas = 1
	consumer, err := s.Stream.CreateConsumer(ctx, cfg)
	if err != nil {
		return nil, err
	}
	info, err := consumer.Info(ctx)
	if err != nil {
		return nil, err
	}
	if info.Config.Replicas != 1 || !info.Config.MemoryStorage || info.Config.AckPolicy != jetstream.AckNonePolicy {
		return nil, fmt.Errorf("unexpected actual candidate consumer config: %+v", info.Config)
	}
	*s.observed = append(*s.observed, info.Stream)
	return consumer, nil
}

func TestBatchedInvariantAuditNativeConsumerReplication(t *testing.T) {
	js, ctx, want := batchAuditLargeCohort(t, 12000)
	audit := func(singleReplica bool) (Report, error, time.Duration, []string) {
		started := time.Now()
		attempt, stop := context.WithTimeout(ctx, 20*time.Second)
		defer stop()
		var observed []string
		result, err := checkUsing(attempt, js, nil, func(ctx context.Context, stream jetstream.Stream, cutoff *uint64, visit func(*jetstream.RawStreamMsg) error) error {
			if singleReplica {
				stream = nativeSingleReplicaAuditStream{Stream: stream, observed: &observed}
			}
			return scanBatchThrough(ctx, stream, cutoff, visit)
		})
		return result, err, time.Since(started), observed
	}
	baseline, baselineErr, baselineElapsed, _ := audit(false)
	if baselineErr == nil {
		if baseline != want {
			t.Fatalf("replicated report=%+v", baseline)
		}
	} else if !errors.Is(baselineErr, context.DeadlineExceeded) {
		t.Fatalf("unexpected replicated audit failure: %v", baselineErr)
	}
	candidate, candidateErr, candidateElapsed, observed := audit(true)
	if candidateErr != nil || candidate != want || strings.Join(observed, ",") != "WF_INV,WF_JRN" {
		t.Fatalf("single-replica report=%+v observed=%v err=%v", candidate, observed, candidateErr)
	}
	// Recheck the replicated reader after the candidate to expose an ordering
	// or warm-cache effect rather than interpreting a single pair as speedup.
	recheck, recheckErr, recheckElapsed, _ := audit(false)
	if recheckErr == nil {
		if recheck != want {
			t.Fatalf("replicated recheck report=%+v", recheck)
		}
	} else if !errors.Is(recheckErr, context.DeadlineExceeded) {
		t.Fatalf("unexpected replicated recheck failure: %v", recheckErr)
	}
	for _, name := range []string{"WF_INV", "WF_JRN"} {
		stream, err := js.Stream(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		info, err := stream.Info(ctx)
		if err != nil || info.Config.Replicas != 3 || info.State.Consumers != 0 {
			t.Fatalf("source stream changed or audit consumer leaked: %s info=%+v err=%v", name, info, err)
		}
	}
	t.Logf("replication-audit invocations=%d entries=%d terminal=%d stream_replicas=3 replicated_consumer_seconds=%s replicated_error=%v single_consumer_seconds=%s single_error=%v replicated_recheck_seconds=%s replicated_recheck_error=%v actual_single_consumers=%s consumers=0", candidate.Invocations, candidate.Entries, candidate.Terminal, baselineElapsed, baselineErr, candidateElapsed, candidateErr, recheckElapsed, recheckErr, strings.Join(observed, ","))
}

func TestBatchedInvariantAuditNativePhaseProfile(t *testing.T) {
	profileRoot := os.Getenv("WF_AUDIT_BATCH_PROFILE_ROOT")
	if profileRoot == "" {
		t.Skip("set WF_AUDIT_BATCH_PROFILE_ROOT for the native CPU/phase profile")
	}
	count := nativeAuditProfileCount(t)
	js, ctx, want := batchAuditLargeCohort(t, count)
	if !filepath.IsAbs(profileRoot) {
		t.Fatal("profile root must be absolute")
	}
	if err := os.Mkdir(profileRoot, 0755); err != nil {
		t.Fatal(err)
	}
	profile, err := os.Create(filepath.Join(profileRoot, "audit-cpu.pprof"))
	if err != nil {
		t.Fatal(err)
	}
	defer profile.Close()
	if err := pprof.StartCPUProfile(profile); err != nil {
		t.Fatal(err)
	}
	type phase struct {
		Stream       string        `json:"stream"`
		Records      int           `json:"records"`
		Bytes        int           `json:"bytes"`
		ScanElapsed  time.Duration `json:"scan_ns"`
		VisitElapsed time.Duration `json:"visit_ns"`
	}
	var phases []phase
	attempt, stop := context.WithTimeout(ctx, 20*time.Second)
	started := time.Now()
	report, auditErr := checkUsingState(attempt, js, nil, func(ctx context.Context, stream jetstream.Stream, cutoff *uint64, visit func(*jetstream.RawStreamMsg) error) error {
		p := phase{Stream: stream.CachedInfo().Config.Name}
		began := time.Now()
		var failure error
		pprof.Do(ctx, pprof.Labels("audit_phase", p.Stream), func(ctx context.Context) {
			failure = scanBatchThrough(ctx, stream, cutoff, func(msg *jetstream.RawStreamMsg) error {
				p.Records++
				p.Bytes += len(msg.Data)
				entered := time.Now()
				err := visit(msg)
				p.VisitElapsed += time.Since(entered)
				return err
			})
		})
		p.ScanElapsed = time.Since(began)
		phases = append(phases, p)
		return failure
	}, os.Getenv("WF_AUDIT_BATCH_STATE_SNAPSHOT") == "1")
	elapsed := time.Since(started)
	stop()
	pprof.StopCPUProfile()
	data, err := json.MarshalIndent(struct {
		Phases  []phase       `json:"phases"`
		Report  Report        `json:"report"`
		Elapsed time.Duration `json:"elapsed_ns"`
		Error   string        `json:"error"`
	}{Phases: phases, Report: report, Elapsed: elapsed, Error: fmt.Sprint(auditErr)}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(profileRoot, "audit-phases.json"), data, 0644); err != nil {
		t.Fatal(err)
	}
	if auditErr != nil || report != want || len(phases) != 2 || phases[0].Records != count || phases[1].Records != count*12 {
		t.Fatalf("profiled audit report=%+v phases=%+v err=%v", report, phases, auditErr)
	}
	t.Logf("profiled-audit invocations=%d entries=%d terminal=%d elapsed=%s phases=%+v cpu_profile=%s", report.Invocations, report.Entries, report.Terminal, elapsed, phases, filepath.Join(profileRoot, "audit-cpu.pprof"))
}

func nativeAuditProfileCount(t *testing.T) int {
	t.Helper()
	count := 12000
	if value := os.Getenv("WF_AUDIT_BATCH_PROFILE_INVOCATIONS"); value != "" {
		var err error
		count, err = strconv.Atoi(value)
		if err != nil || count < 1 || count > 100000 {
			t.Fatal("WF_AUDIT_BATCH_PROFILE_INVOCATIONS must be between 1 and 100000")
		}
	}
	return count
}

func TestBatchedInvariantAuditNativeStateSnapshotComparison(t *testing.T) {
	if os.Getenv("WF_AUDIT_BATCH_CANDIDATE") != "1" {
		t.Skip("opt-in native same-store state read comparison")
	}
	js, ctx, want := batchAuditLargeCohort(t, nativeAuditProfileCount(t))
	audit := func(snapshot bool) (Report, error, time.Duration) {
		attempt, stop := context.WithTimeout(ctx, 20*time.Second)
		defer stop()
		began := time.Now()
		report, err := checkUsingState(attempt, js, nil, scanBatchThrough, snapshot)
		return report, err, time.Since(began)
	}
	before, beforeErr, beforeElapsed := audit(false)
	state, stateErr, stateElapsed := audit(true)
	after, afterErr, afterElapsed := audit(false)
	if beforeErr != nil || stateErr != nil || afterErr != nil || before != want || state != want || after != want {
		t.Fatalf("state comparison before=%+v/%v state=%+v/%v after=%+v/%v", before, beforeErr, state, stateErr, after, afterErr)
	}
	t.Logf("state-snapshot-audit invocations=%d entries=%d terminal=%d point_state_before=%s snapshot_state=%s point_state_after=%s errors=<nil>,<nil>,<nil>", state.Invocations, state.Entries, state.Terminal, beforeElapsed, stateElapsed, afterElapsed)
}

func TestBatchedInvariantAuditNativeStreamingComparison(t *testing.T) {
	if os.Getenv("WF_AUDIT_BATCH_CANDIDATE") != "1" {
		t.Skip("opt-in native same-store streaming comparison")
	}
	js, ctx, want := batchAuditLargeCohort(t, nativeAuditProfileCount(t))
	for _, mode := range []struct {
		name                string
		streaming, snapshot bool
	}{
		{"baseline", false, false},
		{"streaming", true, false},
		{"streaming_state", true, true},
		{"baseline_recheck", false, false},
	} {
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		attempt, stop := context.WithTimeout(ctx, 20*time.Second)
		started := time.Now()
		report, err := checkUsingOptions(attempt, js, nil, scanBatchThrough, mode.snapshot, mode.streaming)
		elapsed := time.Since(started)
		stop()
		runtime.ReadMemStats(&after)
		t.Logf("streaming-audit reader=%s elapsed=%s report=%+v err=%v allocated_bytes=%d gc_cycles=%d", mode.name, elapsed, report, err, after.TotalAlloc-before.TotalAlloc, after.NumGC-before.NumGC)
		if err == nil {
			if report != want {
				t.Fatalf("%s report=%+v want=%+v", mode.name, report, want)
			}
		} else if mode.streaming || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("%s audit error: %v", mode.name, err)
		}
	}
}

func TestBatchedInvariantAuditNativeStateValues(t *testing.T) {
	if os.Getenv("WF_AUDIT_BATCH_CANDIDATE") != "1" {
		t.Skip("opt-in native state snapshot comparison")
	}
	js, ctx := batchedAuditCluster(t)
	cutoff := batchAuditPublish(t, ctx, js, "state-first", batchAuditEntries())
	batchAuditPublish(t, ctx, js, "state-later", batchAuditEntries())
	state, err := js.KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	want := Report{Invocations: 1, Journals: 1, Entries: 4, Terminal: 1}
	compareFullAudits(t, ctx, js, &cutoff, want, "")
	key := identity.Key("audit", "state-first")
	if _, err := state.Put(ctx, key, []byte(`"corrupt"`)); err != nil {
		t.Fatal(err)
	}
	compareFullAudits(t, ctx, js, &cutoff, Report{}, "terminal state differs")
	for _, remove := range []func(context.Context, string, ...jetstream.KVDeleteOpt) error{state.Delete, state.Purge} {
		if _, err := state.Put(ctx, key, []byte(`"ok"`)); err != nil {
			t.Fatal(err)
		}
		if err := remove(ctx, key); err != nil {
			t.Fatal(err)
		}
		compareFullAudits(t, ctx, js, &cutoff, Report{}, "terminal state missing")
	}
	if _, err := state.Put(ctx, key, []byte(`"ok"`)); err != nil {
		t.Fatal(err)
	}
	compareFullAudits(t, ctx, js, &cutoff, want, "")
	compareFullAudits(t, ctx, js, nil, Report{Invocations: 2, Journals: 2, Entries: 8, Terminal: 2}, "")
}
