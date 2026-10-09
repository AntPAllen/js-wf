package graphpublication

import (
	"context"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// With no logical writer, cooperating reads of a stable authority should each
// finish with one acknowledged witness. Physical sequence churn is measured
// independently from the returned logical root.
func TestNativeGraphStableReadCohort(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			_, authority, ctx := nativeGraphFixture(t, replicas)
			initial, err := authority.ReadRoot(ctx, "stable")
			if err != nil {
				t.Fatal(err)
			}
			before, err := authority.stream.GetLastMsgForSubject(ctx, authority.subject("root", "stable"))
			if err != nil {
				t.Fatal(err)
			}
			const readers = 32
			ready := make(chan struct{})
			var wg sync.WaitGroup
			roots := make([]Root, readers)
			errs := make([]error, readers)
			for i := range readers {
				wg.Add(1)
				go func(i int) { defer wg.Done(); <-ready; roots[i], errs[i] = authority.ReadRoot(ctx, "stable") }(i)
			}
			close(ready)
			wg.Wait()
			failures := 0
			for i := range readers {
				if errs[i] != nil {
					failures++
					t.Errorf("stable read %d: %v", i, errs[i])
					continue
				}
				if !reflect.DeepEqual(roots[i], initial) {
					t.Errorf("stable read %d changed logical authority", i)
				}
			}
			after, err := authority.stream.GetLastMsgForSubject(ctx, authority.subject("root", "stable"))
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("stable readers=%d failed=%d acknowledged_physical_witnesses=%d logical_head=%d", readers, failures, after.Sequence-before.Sequence, initial.Head)
			if failures == 0 && after.Sequence-before.Sequence != readers {
				t.Fatal("unexpected witness count", after.Sequence-before.Sequence)
			}
			if string(before.Data) != string(after.Data) {
				t.Fatal("read cohort changed logical bytes")
			}
		})
	}
}

type heldReadWitness struct {
	jetstream.JetStream
	subject string
	held    atomic.Bool
	entered chan struct{}
	resume  chan struct{}
}

func (p *heldReadWitness) PublishMsg(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	if msg.Subject == p.subject && msg.Header.Get("Wf-Authority-Read-Witness") == "1" && p.held.CompareAndSwap(false, true) {
		close(p.entered)
		select {
		case <-p.resume:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return p.JetStream.PublishMsg(ctx, msg, opts...)
}

func TestNativeGraphQueuedReadCancellationAndConcurrentMutation(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			_, authority, ctx := nativeGraphFixture(t, replicas)
			held := &heldReadWitness{JetStream: authority.js, subject: authority.subject("root", "held"), entered: make(chan struct{}), resume: make(chan struct{})}
			authority.js = held
			var once sync.Once
			resume := func() { once.Do(func() { close(held.resume) }) }
			defer resume()
			type result struct {
				root Root
				err  error
			}
			first := make(chan result, 1)
			go func() { r, e := authority.ReadRoot(ctx, "held"); first <- result{r, e} }()
			select {
			case <-held.entered:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			queuedCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			second := make(chan error, 1)
			go func() { _, e := authority.ReadRoot(queuedCtx, "held"); second <- e }()
			ticker := time.NewTicker(time.Millisecond)
			defer ticker.Stop()
			for {
				authority.reads.mu.Lock()
				entry := authority.reads.entries["root:held"]
				queued := entry != nil && entry.references == 2
				authority.reads.mu.Unlock()
				if queued {
					break
				}
				select {
				case e := <-second:
					t.Fatalf("read bypassed held witness: %v", e)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-ticker.C:
				}
			}
			cancel()
			select {
			case e := <-second:
				if !errors.Is(e, context.Canceled) {
					t.Fatal(e)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			// Cancellation must not release the active read's gate. Mutations and
			// independent subjects must still reach their real quorum while it is held.
			authority.reads.mu.Lock()
			entry := authority.reads.entries["root:held"]
			protected := entry != nil && entry.references == 1 && len(entry.gate) == 0
			authority.reads.mu.Unlock()
			if !protected {
				t.Fatal("queued cancellation released active witness")
			}
			written, e := authority.CASRoot(ctx, "held", 0, EmptyRoot())
			if e != nil || written.Head != 1 {
				t.Fatal(written, e)
			}
			independent, e := authority.ReadRoot(ctx, "independent")
			if e != nil || !reflect.DeepEqual(independent, EmptyRoot()) {
				t.Fatal(independent, e)
			}
			resume()
			select {
			case value := <-first:
				if value.err != nil || !reflect.DeepEqual(value.root, written) {
					t.Fatal(value.root, value.err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			authority.reads.mu.Lock()
			remaining := len(authority.reads.entries)
			authority.reads.mu.Unlock()
			if remaining != 0 {
				t.Fatal("inactive coordination entries retained", remaining)
			}
			t.Log("queued cancellation isolated, mutation and other subject completed, stale witness reloaded, no coordination entries retained")
		})
	}
}
