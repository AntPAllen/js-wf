package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"js-wf/assignment"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/provision"
	"js-wf/testcluster"
	"js-wf/wf"

	"github.com/nats-io/nats.go/jetstream"
)

func TestKVAssignmentMovesInflightInvocation(t *testing.T) {
	cluster, err := testcluster.Start(t.TempDir(), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	jsB, err := jetstream.New(cluster.Clients[1])
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var readyErr error
	for ctx.Err() == nil {
		attempt, done := context.WithTimeout(ctx, 3*time.Second)
		_, readyErr = js.AccountInfo(attempt)
		if readyErr == nil {
			readyErr = provision.Ensure(attempt, js, 3)
		}
		done()
		if readyErr == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if readyErr != nil {
		t.Fatal(fmt.Errorf("provision cluster: %w", readyErr))
	}
	assignments, err := assignment.New(ctx, js)
	if err != nil {
		t.Fatal(err)
	}
	partition := identity.Partition("test", "move", provision.Partitions)
	revision, err := assignments.Assign(ctx, partition, "owner-a", 0)
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{}, 1)
	a, err := New(ctx, js, "owner-a", map[string]Handler{"test": func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		_, err := wf.Run(c, "wait", nil, func(stepCtx context.Context) (int, error) {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-stepCtx.Done()
			return 0, stepCtx.Err()
		})
		return nil, err
	}})
	if err != nil {
		t.Fatal(err)
	}
	b, err := New(ctx, jsB, "owner-b", map[string]Handler{"test": func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		value, err := wf.Run(c, "wait", nil, func(context.Context) (int, error) { return 42, nil })
		if err != nil {
			return nil, err
		}
		return json.Marshal(value)
	}})
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	aDone, bDone := make(chan error, 1), make(chan error, 1)
	go func() { aDone <- a.RunKVAssignments(workerCtx) }()
	go func() { bDone <- b.RunKVAssignments(workerCtx) }()
	defer func() {
		stop()
		if err := <-aDone; err != nil {
			t.Error(err)
		}
		if err := <-bDone; err != nil {
			t.Error(err)
		}
	}()
	if _, err := client.New(js).Start(ctx, "test", "move", []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("first worker did not enter step")
	}
	if _, err := assignments.Assign(ctx, partition, "owner-b", revision); err != nil {
		t.Fatal(err)
	}
	if _, err := assignments.Assign(ctx, partition, "owner-a", revision); !errors.Is(err, assignment.ErrConflict) {
		t.Fatalf("stale assignment update: %v", err)
	}
	result, err := client.New(js).Await(ctx, "test", "move")
	if err != nil || string(result) != "42" {
		t.Fatalf("handoff result=%s err=%v", result, err)
	}
	if _, err := integrity.Check(ctx, js); err != nil {
		t.Fatal(err)
	}
}
