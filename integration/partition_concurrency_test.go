package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/provision"
	"js-wf/wf"
	"js-wf/worker"
)

func TestPartitionConcurrencyCancelsActiveHandlers(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	const typ, firstID = "concurrent", "first"
	partition := identity.Partition(typ, firstID, provision.Partitions)
	secondID := ""
	for index := 0; index < 1000; index++ {
		candidate := fmt.Sprintf("second-%d", index)
		if identity.Partition(typ, candidate, provision.Partitions) == partition {
			secondID = candidate
			break
		}
	}
	if secondID == "" {
		t.Fatal("could not find second ID on the same partition")
	}
	entered := make(chan struct{}, 2)
	var active atomic.Int32
	handler := func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		active.Add(1)
		defer active.Add(-1)
		entered <- struct{}{}
		<-c.Context().Done()
		return nil, c.Context().Err()
	}
	w, err := worker.New(ctx, all[1], "concurrent-worker", map[string]worker.Handler{typ: handler}, worker.WithPartitionConcurrency(2))
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- w.RunPartition(workerCtx, partition) }()
	c := client.New(all[0])
	for _, id := range []string{firstID, secondID} {
		if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("two handlers did not enter concurrently")
		}
	}
	if got := active.Load(); got != 2 {
		t.Fatalf("active handlers=%d, want 2", got)
	}
	stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("partition loop did not stop active handlers")
	}
	if got := active.Load(); got != 0 {
		t.Fatalf("active handlers after stop=%d", got)
	}
}
