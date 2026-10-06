package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/testcluster"
	"js-wf/wf"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go/jetstream"
)

func outerHandlerNativeFixture(t *testing.T, limit time.Duration) (context.Context, *testcluster.Cluster, jetstream.JetStream) {
	t.Helper()
	cluster, err := testcluster.Start(t.TempDir(), 3)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), limit)
	t.Cleanup(cancel)
	// TCP readiness precedes metadata election. Bound each startup request
	// within the original whole-test context, as the integration harness does.
	for {
		attempt, stop := context.WithTimeout(ctx, 2*time.Second)
		_, readyErr := js.AccountInfo(attempt)
		stop()
		if readyErr == nil {
			attempt, stop = context.WithTimeout(ctx, 3*time.Second)
			err = provision.Ensure(attempt, js, 3)
			stop()
			if err == nil {
				break
			}
		}
		if ctx.Err() != nil {
			t.Fatalf("metadata/provision readiness: %v / %v", readyErr, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	return ctx, cluster, js
}

func TestOuterHandlerGoexitRecordsBoundedAttempts(t *testing.T) {
	ctx, _, js := outerHandlerNativeFixture(t, 20*time.Second)
	w, err := New(ctx, js, "outer-goexit-worker", map[string]Handler{"test": func(*wf.Context, json.RawMessage) (json.RawMessage, error) {
		runtime.Goexit()
		return nil, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := client.New(js).Start(ctx, "test", "outer-goexit", []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() {
		done <- w.RunPartition(workerCtx, identity.Partition("test", "outer-goexit", provision.Partitions))
	}()
	_, err = client.New(js).Await(ctx, "test", "outer-goexit")
	if err == nil || !strings.Contains(err.Error(), "workflow exited via runtime.Goexit") {
		t.Fatalf("terminal error: %v", err)
	}
	stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("worker did not stop")
	}
	records, _, err := journal.New(js).Read(ctx, "test", "outer-goexit")
	if err != nil || len(records) != 5 || records[0].Kind != journal.Started || records[4].Kind != journal.Failed {
		t.Fatalf("journal=%+v err=%v", records, err)
	}
	for i := 1; i <= 3; i++ {
		attempt, err := journal.DecodeAttempt(records[i].Payload)
		if records[i].Kind != journal.Attempt || err != nil || attempt.Count != i || attempt.Error != "workflow exited via runtime.Goexit" {
			t.Fatalf("attempt %d: %+v err=%v", i, attempt, err)
		}
	}
}

func TestOuterHandlerNativeCancellationAndLateAppend(t *testing.T) {
	for _, mode := range []string{"worker_stop", "durable_cancel"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cluster, js := outerHandlerNativeFixture(t, 30*time.Second)
			const typ = "test"
			id := "outer-" + mode
			entered, release := make(chan struct{}), make(chan struct{})
			late := make(chan error, 1)
			var releaseOnce atomic.Bool
			releaseHandler := func() {
				if releaseOnce.CompareAndSwap(false, true) {
					close(release)
				}
			}
			defer releaseHandler()
			var lateEffect atomic.Bool
			acks := make(chan struct{}, 8)
			observe := func(e DispatchEvent) {
				if e.Stage == "ack" && e.Error == "" {
					select {
					case acks <- struct{}{}:
					default:
					}
				}
			}
			first, err := New(ctx, js, "outer-old", map[string]Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
				close(entered)
				<-release
				_, err := wf.Run(c, "late", 0, func(context.Context) (int, error) { lateEffect.Store(true); return 99, nil })
				late <- err
				return json.RawMessage(`99`), nil
			}}, WithDispatchObserver(observe))
			if err != nil {
				t.Fatal(err)
			}
			defer first.Close()
			firstCtx, stopFirst := context.WithCancel(ctx)
			defer stopFirst()
			firstDone := make(chan error, 1)
			partition := identity.Partition(typ, id, provision.Partitions)
			go func() { firstDone <- first.RunPartition(firstCtx, partition) }()
			c := client.New(js)
			if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
				t.Fatal(err)
			}
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("outer handler did not enter")
			}
			if mode == "worker_stop" {
				began := time.Now()
				stopFirst()
				select {
				case err := <-firstDone:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("ignored cancellation trapped shutdown")
				}
				t.Logf("outer handler shutdown elapsed=%s", time.Since(began))
				second, err := New(ctx, js, "outer-new", map[string]Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
					value, err := wf.Run(c, "successor", 0, func(context.Context) (int, error) { return 42, nil })
					return json.RawMessage(fmt.Sprint(value)), err
				}}, WithDispatchObserver(observe))
				if err != nil {
					t.Fatal(err)
				}
				defer second.Close()
				secondCtx, stopSecond := context.WithCancel(ctx)
				defer stopSecond()
				secondDone := make(chan error, 1)
				go func() { secondDone <- second.RunPartition(secondCtx, partition) }()
				result, err := c.Await(ctx, typ, id)
				if err != nil || string(result) != "42" {
					t.Fatalf("successor result=%s err=%v", result, err)
				}
				select {
				case <-acks:
				case <-ctx.Done():
					t.Fatal("successor did not confirm ACK")
				}
				waitOuterHandlerPhysicalDrain(t, ctx, cluster)
				stopSecond()
				select {
				case err := <-secondDone:
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal("successor did not stop")
				}
			} else {
				if _, err := c.Cancel(ctx, typ, id); err != nil {
					t.Fatal(err)
				}
				if _, err := c.Await(ctx, typ, id); !errors.Is(err, client.ErrCancelled) {
					t.Fatalf("durable cancel outcome: %v", err)
				}
				select {
				case <-acks:
				case <-ctx.Done():
					t.Fatal("cancel did not confirm ACK")
				}
				waitOuterHandlerPhysicalDrain(t, ctx, cluster)
				stopFirst()
				select {
				case err := <-firstDone:
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal("cancelled worker did not stop")
				}
			}
			store := journal.New(js)
			before, tail, err := store.Read(ctx, typ, id)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "worker_stop" {
				if len(before) != 4 || before[0].Kind != journal.Started || before[3].Kind != journal.Completed || before[1].Epoch <= before[0].Epoch {
					t.Fatalf("successor did not take a new epoch: %+v", before)
				}
			} else if len(before) != 3 || before[1].Kind != journal.SignalConsumed || before[2].Kind != journal.Failed || before[1].Epoch <= before[0].Epoch {
				t.Fatalf("cancel was not drained on a new delivery: %+v", before)
			}
			releaseHandler()
			select {
			case err := <-late:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("late SDK call: %v", err)
				}
			case <-ctx.Done():
				t.Fatal("late handler did not return")
			}
			after, afterTail, err := store.Read(ctx, typ, id)
			if err != nil || tail != afterTail || !reflect.DeepEqual(before, after) || lateEffect.Load() {
				t.Fatalf("late handler changed accepted journal or ran effect: %v", err)
			}
			report, err := integrity.Check(ctx, js)
			expectedEntries := 4
			if mode == "durable_cancel" {
				expectedEntries = 3
			}
			if err != nil || report != (integrity.Report{Invocations: 1, Journals: 1, Entries: expectedEntries, Terminal: 1}) {
				t.Fatalf("integrity=%+v err=%v", report, err)
			}
		})
	}
}

func waitOuterHandlerPhysicalDrain(t *testing.T, ctx context.Context, cluster *testcluster.Cluster) {
	t.Helper()
	for ctx.Err() == nil {
		drained := true
		identities := map[string]bool{}
		for node, actual := range cluster.Servers {
			state, err := actual.Jsz(&server.JSzOptions{Accounts: true, Streams: true, Consumer: true, RaftGroups: true})
			if err != nil || state == nil || state.Disabled || state.ID == "" || identities[state.ID] {
				t.Fatalf("physical peer%d: state=%+v err=%v", node, state, err)
			}
			identities[state.ID] = true
			found := 0
			for _, account := range state.AccountDetails {
				for _, stream := range account.Streams {
					if stream.Name == "WF_RUN" {
						found++
						// This fixture starts one partition loop, hence one durable.
						drained = drained && stream.State.Msgs == 0 && stream.State.Consumers == 1
					}
				}
			}
			if found != 1 {
				t.Fatalf("physical peer%d WF_RUN census=%d", node, found)
			}
		}
		if drained {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("all three physical queues did not drain within the original test context")
}
