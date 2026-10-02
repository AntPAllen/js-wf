//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/provision"
)

// Workers are joined before the fault. Count only the target payload inside
// the physical sequence interval for these publications; unrelated scheduled
// wakeups can advance WF_RUN while the mixed cohort remains suspended.
func challengeMixedEnqueue(t *testing.T, ctx context.Context, nodes []jetstream.JetStream, survivor int, typ, id string) int {
	t.Helper()
	var run jetstream.Stream
	var err error
	for until := time.Now().Add(20 * time.Second); time.Now().Before(until) && ctx.Err() == nil; {
		attempt, stop := context.WithTimeout(ctx, 2*time.Second)
		run, err = nodes[survivor].Stream(attempt, "WF_RUN")
		stop()
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatal("enqueue stream readiness", err)
	}
	before, err := run.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	const callers = 64
	messageID := "mixed-enqueue:" + identity.Key(typ, id)
	ready := sync.WaitGroup{}
	ready.Add(callers)
	start := make(chan struct{})
	results := make(chan error, callers)
	for i := 0; i < callers; i++ {
		js := nodes[(survivor+i%2)%len(nodes)]
		go func() { ready.Done(); <-start; results <- client.New(js).Enqueue(ctx, typ, id, messageID) }()
	}
	ready.Wait()
	close(start)
	for i := 0; i < callers; i++ {
		if err := <-results; err != nil {
			t.Fatal("unrelated enqueue outcome", err)
		}
	}
	after, err := run.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	retained := 0
	headersPresent := 0
	subject := identity.RunSubject(typ, id, provision.Partitions)
	for sequence := before.State.LastSeq + 1; sequence <= after.State.LastSeq; sequence++ {
		raw, err := run.GetMsg(ctx, sequence)
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			continue
		}
		if err != nil {
			t.Fatal("enqueue retained read", err)
		}
		if raw.Subject != subject || string(raw.Data) != identity.Key(typ, id) {
			continue
		}
		retained++
		if raw.Header.Get("Nats-Msg-Id") == messageID {
			headersPresent++
		} else if raw.Header.Get("Nats-Msg-Id") != "" {
			t.Fatal("unexpected message identity", raw.Header)
		}
		proof, err := json.Marshal(raw)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("MIXED_ENQUEUE_RAW_RECEIPT %s", proof)
	}
	if retained != 1 && retained < callers {
		t.Fatalf("partial enqueue retention: calls=%d retained=%d", callers, retained)
	}
	if retained == 1 && headersPresent != 1 || retained > 1 && headersPresent != 0 {
		t.Fatalf("enqueue header evidence mismatch: retained=%d id_headers=%d", retained, headersPresent)
	}
	if retained >= callers {
		t.Log("MIXED_ENQUEUE_RETAINED_DUPLICATES acknowledged_calls=64 retained_at_least64=true id_headers=0")
	}
	t.Logf("MIXED_ENQUEUE_ADMISSION acknowledged_calls=64 first_sequence=%d last_sequence=%d retained=%d id_headers=%d", before.State.LastSeq+1, after.State.LastSeq, retained, headersPresent)
	return retained
}
