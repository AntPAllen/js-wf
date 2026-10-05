//go:build linux

package integrity

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/testcluster"
)

// This is a quiet synthetic population capacity gate, not a live soak. A failure
// retains each original 20s audit verdict and all stores for diagnosis.
func TestConcurrentStateR5PopulationCapacity(t *testing.T) {
	if os.Getenv("WF_AUDIT_CONCURRENT_CAPACITY") != "1" {
		t.Skip("opt-in fresh R5 400k population capacity gate")
	}
	const count = 400000
	root := candidateNativeRoot(t)
	cluster, err := testcluster.StartDockerCluster(filepath.Join(root, "cluster"), 5)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	t.Cleanup(func() {
		for n := 0; n < 5; n++ {
			data, err := cluster.Logs(n)
			if err != nil {
				t.Error(err)
				continue
			}
			if err = os.WriteFile(filepath.Join(root, fmt.Sprintf("server-%d.log", n)), []byte(data), 0600); err != nil {
				t.Error(err)
			}
		}
	})
	var urls []string
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
	ready, done := context.WithTimeout(setup, 30*time.Second)
	for {
		err = provision.Ensure(ready, js, 5)
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
	state, err := js.KeyValue(setup, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	// One producer owns each invocation, preserving per-subject entry order.
	// Every publish acknowledgment is checked; only 13 futures per producer are
	// retained, so preparation memory does not grow with population.
	jobs := make(chan int)
	failures := make(chan error, 8)
	var wg sync.WaitGroup
	started := time.Now()
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				id := fmt.Sprintf("capacity-%06d", i)
				futures := make([]jetstream.PubAckFuture, 0, 13)
				publish := func(subject string, data []byte) error {
					f, e := js.PublishAsync(subject, data)
					if e == nil {
						futures = append(futures, f)
					}
					return e
				}
				if e := publish(identity.InvocationSubject("audit", id), []byte("input")); e != nil {
					failures <- e
					return
				}
				entries := []journal.Entry{{Index: 0, Epoch: 1, Kind: journal.Started, WorkerID: "audit-proof"}}
				for n := uint64(0); n < 5; n++ {
					entries = append(entries, journal.Entry{Index: 1 + 2*n, Epoch: 1, Kind: journal.StepRequested, WorkerID: "audit-proof"}, journal.Entry{Index: 2 + 2*n, Epoch: 1, Kind: journal.StepCompleted, WorkerID: "audit-proof"})
				}
				entries = append(entries, journal.Entry{Index: 11, Epoch: 1, Kind: journal.Completed, WorkerID: "audit-proof", Payload: json.RawMessage(`"ok"`)})
				for _, entry := range entries {
					data, e := json.Marshal(entry)
					if e != nil {
						failures <- e
						return
					}
					if e = publish(identity.JournalSubject("audit", id), data); e != nil {
						failures <- e
						return
					}
				}
				for _, f := range futures {
					select {
					case <-f.Ok():
					case e := <-f.Err():
						failures <- e
						return
					case <-setup.Done():
						failures <- setup.Err()
						return
					}
				}
				if _, e := state.Put(setup, identity.Key("audit", id), []byte(`"ok"`)); e != nil {
					failures <- e
					return
				}
			}
		}()
	}
	var populationError error
send:
	for i := 0; i < count; i++ {
		select {
		case jobs <- i:
		case populationError = <-failures:
			break send
		case <-setup.Done():
			populationError = setup.Err()
			break send
		}
		if i > 0 && i%10000 == 0 {
			t.Logf("population dispatched=%d elapsed=%s", i, time.Since(started))
		}
	}
	close(jobs)
	wg.Wait()
	close(failures)
	for e := range failures {
		if populationError == nil {
			populationError = e
		}
	}
	if populationError != nil {
		t.Fatal(populationError)
	}
	want := Report{Invocations: count, Journals: count, Entries: count * 12, Terminal: count}
	for _, name := range []string{"WF_INV", "WF_JRN"} {
		stream, e := js.Stream(setup, name)
		if e != nil {
			t.Fatal(e)
		}
		info := stream.CachedInfo()
		expected := uint64(count)
		if name == "WF_JRN" {
			expected *= 12
		}
		if info.Config.Replicas != 5 || info.Config.Storage != jetstream.FileStorage || info.State.Msgs != expected {
			t.Fatalf("population metadata %s=%+v", name, info)
		}
	}
	type result struct {
		Mode           string
		ElapsedNS      int64
		Report         Report
		Error          string
		AllocatedBytes uint64
		GCCycles       uint32
	}
	var results []result
	for _, mode := range []struct {
		name  string
		check func(context.Context, jetstream.JetStream) (Report, error)
	}{{"sequential", CheckWithStreamingStateReads}, {"concurrent", CheckWithConcurrentStreamingStateReads}, {"sequential_recheck", CheckWithStreamingStateReads}} {
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		call, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		began := time.Now()
		report, e := mode.check(call, js)
		elapsed := time.Since(began)
		cancel()
		runtime.ReadMemStats(&after)
		results = append(results, result{mode.name, int64(elapsed), report, fmt.Sprint(e), after.TotalAlloc - before.TotalAlloc, after.NumGC - before.NumGC})
		data, marshalError := json.MarshalIndent(results, "", "  ")
		if marshalError != nil {
			t.Fatal(marshalError)
		}
		if writeError := os.WriteFile(filepath.Join(root, "capacity-comparison.json"), append(data, '\n'), 0600); writeError != nil {
			t.Fatal(writeError)
		}
		t.Logf("capacity mode=%s elapsed=%s report=%+v err=%v", mode.name, elapsed, report, e)
		if e == nil && report != want {
			t.Errorf("%s report=%+v want=%+v", mode.name, report, want)
		}
		if mode.name == "concurrent" && (e != nil || report != want || elapsed >= 20*time.Second) {
			t.Errorf("400k concurrent population gate missed: %+v", results[len(results)-1])
		}
	}
}
