//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/testcluster"
)

const matrixLatencyCapacityCount = 400000

// This oracle applies only to the documented synthetic activity population. It
// collects public message timestamps independently of the bulk projection and
// reducer. Frozen point checks separately witness the transport and semantics.
type matrixCapacityTimes struct {
	ID                          string
	Enabled, Started, Completed time.Time
	Entries                     uint64
}
type matrixCapacityOracle struct {
	BySubject map[string]*matrixCapacityTimes
	Ordered   []*matrixCapacityTimes
}

func (o *matrixCapacityOracle) invocation(msg *jetstream.RawStreamMsg) error {
	parts := strings.Split(msg.Subject, ".")
	if len(parts) != 4 || parts[0] != "wf" || parts[1] != "inv" || parts[2] != "audit" || identity.Validate(parts[2], parts[3]) != nil || msg.Time.IsZero() {
		return fmt.Errorf("invalid capacity invocation")
	}
	number, err := strconv.Atoi(strings.TrimPrefix(parts[3], "capacity-"))
	if err != nil || number < 0 || number >= matrixLatencyCapacityCount || parts[3] != fmt.Sprintf("capacity-%06d", number) {
		return fmt.Errorf("capacity invocation outside fixed population")
	}
	subject := identity.JournalSubject(parts[2], parts[3])
	if o.BySubject[subject] != nil {
		return fmt.Errorf("duplicate capacity invocation")
	}
	record := &matrixCapacityTimes{ID: parts[3], Enabled: msg.Time.UTC()}
	o.BySubject[subject] = record
	o.Ordered = append(o.Ordered, record)
	return nil
}
func (o *matrixCapacityOracle) entry(msg *jetstream.RawStreamMsg) error {
	r := o.BySubject[msg.Subject]
	if r == nil || msg.Time.IsZero() {
		return fmt.Errorf("capacity journal outside invocation census")
	}
	var entry journal.Entry
	if err := journal.UnmarshalEntry(msg.Data, &entry); err != nil {
		return err
	}
	if entry.Index != r.Entries || entry.Epoch != 1 || r.Entries >= 12 {
		return fmt.Errorf("capacity journal index/epoch mismatch")
	}
	want := journal.StepCompleted
	switch {
	case r.Entries == 0:
		want = journal.Started
	case r.Entries == 11:
		want = journal.Completed
	case r.Entries%2 == 1:
		want = journal.StepRequested
		var request struct {
			Kind    string    `json:"kind"`
			FireAt  time.Time `json:"fire_at"`
			ChildID string    `json:"child_id"`
		}
		if err := json.Unmarshal(entry.Payload, &request); err != nil {
			return err
		}
		if request.Kind != "activity" || !request.FireAt.IsZero() || request.ChildID != "" {
			return fmt.Errorf("capacity fixture is not an activity population")
		}
	}
	if entry.Kind != want {
		return fmt.Errorf("capacity journal kind mismatch")
	}
	if r.Entries == 0 {
		r.Started = msg.Time.UTC()
		if r.Started.Before(r.Enabled) {
			return fmt.Errorf("capacity start precedes invocation")
		}
	}
	if r.Entries == 11 {
		if string(entry.Payload) != `"ok"` {
			return fmt.Errorf("capacity terminal value mismatch")
		}
		r.Completed = msg.Time.UTC()
		if r.Completed.Before(r.Started) {
			return fmt.Errorf("capacity terminal precedes start")
		}
	}
	r.Entries++
	return nil
}
func (r *matrixCapacityTimes) samples(deadline time.Time) ([]matrixLatencySample, error) {
	if r.Entries != 12 || r.Started.IsZero() || r.Completed.IsZero() || r.Completed.After(deadline) {
		return nil, fmt.Errorf("incomplete or late capacity journal")
	}
	return []matrixLatencySample{
		{Type: "audit", ID: r.ID, Event: "start", Enabled: r.Enabled, Observed: r.Started, Delay: r.Started.Sub(r.Enabled)},
		{Type: "audit", ID: r.ID, Event: "terminal", Enabled: r.Enabled, Observed: r.Completed, Delay: r.Completed.Sub(r.Enabled)},
	}, nil
}

// Bound publication memory to eight producers, each retaining thirteen futures.
// The complete 400k/4.8M population is fixed; no smaller-count override exists.
func matrixPopulateLatencyCapacity(ctx context.Context, js jetstream.JetStream, progress func(int)) error {
	state, err := js.KeyValue(ctx, "WF_STATE")
	if err != nil {
		return err
	}
	jobs := make(chan int)
	failures := make(chan error, 8)
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				err := func() error {
					id := fmt.Sprintf("capacity-%06d", i)
					futures := make([]jetstream.PubAckFuture, 0, 13)
					publish := func(subject string, data []byte) error {
						f, e := js.PublishAsync(subject, data)
						if e == nil {
							futures = append(futures, f)
						}
						return e
					}
					// Establish INV before writing JRN on a different stream leader.
					// Sending both asynchronously cannot establish this causality.
					if _, e := js.Publish(ctx, identity.InvocationSubject("audit", id), []byte("input")); e != nil {
						return e
					}
					entries := []journal.Entry{{Index: 0, Epoch: 1, Kind: journal.Started, WorkerID: "audit-proof"}}
					for n := uint64(0); n < 5; n++ {
						entries = append(entries, journal.Entry{Index: 1 + 2*n, Epoch: 1, Kind: journal.StepRequested, WorkerID: "audit-proof", Payload: json.RawMessage(`{"kind":"activity"}`)}, journal.Entry{Index: 2 + 2*n, Epoch: 1, Kind: journal.StepCompleted, WorkerID: "audit-proof"})
					}
					entries = append(entries, journal.Entry{Index: 11, Epoch: 1, Kind: journal.Completed, WorkerID: "audit-proof", Payload: json.RawMessage(`"ok"`)})
					for _, entry := range entries {
						data, e := json.Marshal(entry)
						if e != nil {
							return e
						}
						if e = publish(identity.JournalSubject("audit", id), data); e != nil {
							return e
						}
					}
					for _, f := range futures {
						select {
						case <-f.Ok():
						case e := <-f.Err():
							return e
						case <-ctx.Done():
							return ctx.Err()
						}
					}
					_, e := state.Put(ctx, identity.Key("audit", id), []byte(`"ok"`))
					return e
				}()
				if err != nil {
					failures <- err
					return
				}
			}
		}()
	}
	var failure error
send:
	for i := 0; i < matrixLatencyCapacityCount; i++ {
		select {
		case jobs <- i:
		case failure = <-failures:
			break send
		case <-ctx.Done():
			failure = ctx.Err()
			break send
		}
		if i > 0 && i%10000 == 0 {
			progress(i)
		}
	}
	close(jobs)
	wg.Wait()
	close(failures)
	for err := range failures {
		if failure == nil {
			failure = err
		}
	}
	return failure
}

func TestMatrixBulkLatencyFull400kCapacity(t *testing.T) {
	root := os.Getenv("WF_MATRIX_BULK_CAPACITY_ROOT")
	if root == "" {
		t.Skip("opt-in fresh valid R5 full400k/4.8M latency capacity gate")
	}
	if !filepath.IsAbs(root) {
		t.Fatal("fixture root must be absolute")
	}
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	proof := map[string]any{"population": matrixLatencyCapacityCount, "stage_limit_ns": int64(6 * time.Minute), "accepted": false, "scope": "fresh synthetic full400k/4.8M quiet latency and SDK process memory capacity; no real-workflow, cursor-fault, default/matrix/24h qualification"}
	defer func() {
		var usage syscall.Rusage
		if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err == nil {
			proof["sdk_test_body_peak_rss_kib"] = usage.Maxrss
		} else {
			proof["rss_error"] = err.Error()
		}
		proof["rss_scope"] = "Linux RUSAGE_SELF through test-body completion including fixture preparation, before test cleanup and SDK exit, excluding Docker server processes; Go memory target is not an RSS bound"
		proof["profile"] = map[string]any{"gomaxprocs": runtime.GOMAXPROCS(0), "gogc": os.Getenv("GOGC"), "gomemlimit": os.Getenv("GOMEMLIMIT")}
		data, err := json.MarshalIndent(proof, "", "  ")
		if err == nil {
			err = os.WriteFile(filepath.Join(root, "capacity.json"), append(data, '\n'), 0600)
		}
		if err != nil {
			t.Error(err)
		}
	}()
	cluster, err := testcluster.StartDockerCluster(filepath.Join(root, "cluster"), 5)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	t.Cleanup(func() {
		for n := 0; n < 5; n++ {
			data, e := cluster.Logs(n)
			if e == nil {
				e = os.WriteFile(filepath.Join(root, fmt.Sprintf("server-%d.log", n)), []byte(data), 0600)
			}
			if e != nil {
				t.Error(e)
			}
		}
	})
	urls := []string{}
	for n := 0; n < 5; n++ {
		urls = append(urls, cluster.ClientURL(n))
	}
	nc, err := nats.Connect(strings.Join(urls, ","), nats.IgnoreDiscoveredServers(), nats.MaxReconnects(-1), nats.Timeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc, jetstream.WithPublishAsyncMaxPending(1024))
	if err != nil {
		t.Fatal(err)
	}
	setup, stop := context.WithTimeout(context.Background(), 90*time.Minute)
	defer stop()
	ready, done := context.WithTimeout(setup, 4*time.Minute)
	for {
		call, cancel := context.WithTimeout(ready, 3*time.Second)
		err = provision.Ensure(call, js, 5)
		cancel()
		if err == nil {
			break
		}
		if ready.Err() != nil {
			done()
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	done()
	began := time.Now()
	if err = matrixPopulateLatencyCapacity(setup, js, func(n int) { t.Logf("CAPACITY_POPULATION dispatched=%d elapsed=%s", n, time.Since(began)) }); err != nil {
		t.Fatal(err)
	}
	proof["population_elapsed_ns"] = int64(time.Since(began))
	deadline := time.Now().Add(5 * time.Minute)
	proof["original_completion_deadline"] = deadline
	want := integrity.Report{Invocations: 400000, Journals: 400000, Entries: 4800000, Terminal: 400000}
	check := func(label string) {
		call, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		at := time.Now()
		report, e := integrity.CheckWithChunkedConcurrentStateReads(call, js)
		elapsed := time.Since(at)
		cancel()
		proof[label] = map[string]any{"report": report, "error": fmt.Sprint(e), "elapsed_ns": int64(elapsed), "limit_ns": int64(20 * time.Second)}
		t.Logf("CAPACITY_INTEGRITY %s elapsed=%s report=%+v err=%v", label, elapsed, report, e)
		if e != nil || report != want || elapsed >= 20*time.Second {
			t.Fatalf("full integrity %s rejected", label)
		}
	}
	check("before")
	// Independently collect timestamps and validate the entire known input shape.
	// This does not invoke matrixBulkProjection or either shared latency reducer.
	oracle := matrixCapacityOracle{BySubject: map[string]*matrixCapacityTimes{}}
	call, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	for _, name := range []string{"WF_INV", "WF_JRN"} {
		stream, e := js.Stream(call, name)
		if e != nil {
			cancel()
			t.Fatal(e)
		}
		info, e := stream.Info(call)
		if e != nil {
			cancel()
			t.Fatal(e)
		}
		expected := uint64(400000)
		visit := oracle.invocation
		if name == "WF_JRN" {
			expected = 4800000
			visit = oracle.entry
		}
		if info.Config.Replicas != 5 || info.Config.Storage != jetstream.FileStorage || info.State.Msgs != expected || info.State.Consumers != 0 {
			cancel()
			t.Fatalf("unexpected %s capacity source metadata", name)
		}
		if e = integrity.WalkRetainedWithChunkedReads(call, stream, info.State.LastSeq, visit); e != nil {
			cancel()
			t.Fatal(e)
		}
	}
	cancel()
	if len(oracle.Ordered) != 400000 {
		t.Fatal("incomplete independent timestamp census")
	}
	for _, r := range oracle.Ordered {
		if _, err = r.samples(deadline); err != nil {
			t.Fatal(err)
		}
	}
	// Deterministic spread includes both endpoints and all population quarters.
	pointChecks := 0
	for i := 0; i < 256; i++ {
		r := oracle.Ordered[i*(len(oracle.Ordered)-1)/255]
		call, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		samples, e := matrixInvocationLatenciesLegacyOracle(call, js, "audit", r.ID, r.Enabled, deadline, 0)
		cancel()
		expected, _ := r.samples(deadline)
		if e != nil || !reflect.DeepEqual(samples, expected) {
			t.Fatalf("frozen point oracle %s: err=%v", r.ID, e)
		}
		pointChecks++
	}
	proof["frozen_point_checks"] = pointChecks
	t.Logf("CAPACITY_ORACLE timestamps=400000 entries=4800000 frozen_points=%d", pointChecks)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	stage, stopStage := context.WithTimeout(context.Background(), 6*time.Minute)
	began = time.Now()
	results, stats, err := matrixBulkInvocationAudits(stage, js, want, deadline)
	if err == nil && len(results) != len(oracle.Ordered) {
		err = fmt.Errorf("bulk result census mismatch")
	}
	if err == nil {
		for i, r := range oracle.Ordered {
			if err = stage.Err(); err != nil {
				break
			}
			expected, e := r.samples(deadline)
			if e != nil {
				err = e
				break
			}
			if !reflect.DeepEqual(results[i].samples, expected) {
				err = fmt.Errorf("bulk differs from independent timestamp oracle at %d/%s", i, r.ID)
				break
			}
		}
	}
	elapsed := time.Since(began)
	stopStage()
	runtime.ReadMemStats(&after)
	proof["bulk"] = map[string]any{"stats": stats, "elapsed_ns": int64(elapsed), "error": fmt.Sprint(err), "allocated_bytes": after.TotalAlloc - before.TotalAlloc, "gc_cycles": after.NumGC - before.NumGC}
	if err != nil || elapsed >= 6*time.Minute {
		results = nil
		t.Fatalf("full400k bulk rejected: elapsed=%s err=%v", elapsed, err)
	}
	check("after")
	// Persist complete accepted samples only after the independent final audit.
	file, err := os.OpenFile(filepath.Join(root, "samples.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	encoder := json.NewEncoder(file)
	for _, result := range results {
		if err = encoder.Encode(result.samples); err != nil {
			break
		}
	}
	closeError := file.Close()
	if err != nil {
		t.Fatal(err)
	}
	if closeError != nil {
		t.Fatal(closeError)
	}
	proof["samples"] = 800000
	proof["all_bulk_samples_equal_independent_full_timestamp_oracle"] = true
	proof["accepted"] = true
	t.Logf("CAPACITY_BULK_RESULT invocations=400000 entries=4800000 samples=800000 elapsed=%s charged=%d", elapsed, stats.ChargedBytes)
}
