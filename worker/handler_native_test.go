package worker

import (
	"context"
	"encoding/json"
	"runtime"
	"strings"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/testcluster"
	"js-wf/wf"

	"github.com/nats-io/nats.go/jetstream"
)

func TestOuterHandlerGoexitRecordsBoundedAttempts(t *testing.T) {
	cluster, err := testcluster.Start(t.TempDir(), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
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
