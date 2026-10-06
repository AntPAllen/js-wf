package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/provision"
	"js-wf/wf"

	"github.com/nats-io/nats.go/jetstream"
)

// Pause only the old owner's heartbeat ticks. The real replicated KV expires
// its production 12-second lease; a second live worker receives redelivery.
// Resume the old heartbeat after successor completion, while its user code
// still ignores cancellation. No parent cancellation or KV deletion causes
// the tested handoff.
func TestOuterHandlerNativeLeaseExpiryAndLateAppend(t *testing.T) {
	for _, mode := range []string{"handler", "continuation"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cluster, js := outerHandlerNativeFixture(t, 45*time.Second)
			const typ = "lease-expiry"
			id := mode
			partition := identity.Partition(typ, id, provision.Partitions)
			entered, release := make(chan struct{}), make(chan struct{})
			late := make(chan error, 1)
			var released, lateEffect atomic.Bool
			releaseHandler := func() {
				if released.CompareAndSwap(false, true) {
					close(release)
				}
			}
			defer releaseHandler()
			blocked := func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
				close(entered)
				<-release
				_, err := wf.Run(c, "late", 0, func(context.Context) (int, error) {
					lateEffect.Store(true)
					return 99, nil
				})
				late <- err
				return json.RawMessage(`99`), nil
			}
			successor := func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
				value, err := wf.Run(c, "successor", 0, func(context.Context) (int, error) { return 42, nil })
				return json.RawMessage(fmt.Sprint(value)), err
			}
			ticks := make(chan time.Time)
			fenced, nacked := make(chan FencingEvent, 8), make(chan struct{}, 8)
			firstOptions := []Option{WithHeartbeatTicks(ticks), WithFencingObserver(func(e FencingEvent) { fenced <- e }), WithDispatchObserver(func(e DispatchEvent) {
				if e.Stage == "nak" {
					nacked <- struct{}{}
				}
			})}
			var secondOptions []Option
			firstHandler, secondHandler := Handler(blocked), Handler(successor)
			if mode == "continuation" {
				initial := func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
					return nil, wf.Continue(c, "held_v1", 17)
				}
				stage := func(handler Handler) ContinuationHandler {
					return func(c *wf.Context, input, locals json.RawMessage) (json.RawMessage, error) {
						if string(input) != "null" || string(locals) != "17" {
							return nil, fmt.Errorf("continuation inputs=%s locals=%s", input, locals)
						}
						return handler(c, input)
					}
				}
				firstHandler, secondHandler = initial, initial
				firstOptions = append(firstOptions, WithContinuations(typ, map[string]ContinuationHandler{"held_v1": stage(blocked)}))
				secondOptions = append(secondOptions, WithContinuations(typ, map[string]ContinuationHandler{"held_v1": stage(successor)}))
			}
			first, err := New(ctx, js, "expired-old", map[string]Handler{typ: firstHandler}, firstOptions...)
			if err != nil {
				t.Fatal(err)
			}
			defer first.Close()
			firstCtx, stopFirst := context.WithCancel(ctx)
			defer stopFirst()
			firstDone := make(chan error, 1)
			go func() { firstDone <- first.RunPartition(firstCtx, partition) }()
			c := client.New(js)
			if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
				t.Fatal(err)
			}
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("old handler did not enter")
			}
			began := time.Now()
			kv, err := js.KeyValue(ctx, "WF_LEASE")
			if err != nil {
				t.Fatal(err)
			}
			status, err := kv.Status(ctx)
			if err != nil || status.TTL() != 12*time.Second {
				t.Fatalf("production lease TTL: %v / %v", status, err)
			}
			entry, err := kv.Get(ctx, identity.Key(typ, id))
			if err != nil {
				t.Fatal(err)
			}
			var old lease.Value
			if json.Unmarshal(entry.Value(), &old) != nil || old.Worker != first.ID || old.Epoch == 0 {
				t.Fatalf("old owner: %s", entry.Value())
			}
			for {
				_, err := kv.Get(ctx, identity.Key(typ, id))
				if errors.Is(err, jetstream.ErrKeyNotFound) {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				select {
				case <-ctx.Done():
					t.Fatal("real server lease did not expire")
				case <-time.After(20 * time.Millisecond):
				}
			}
			if firstCtx.Err() != nil || released.Load() {
				t.Fatal("old owner was stopped or released before expiry")
			}
			second, err := New(ctx, js, "expired-new", map[string]Handler{typ: secondHandler}, secondOptions...)
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
			if elapsed := time.Since(began); elapsed >= 30*time.Second {
				t.Fatalf("lease-expiry takeover exceeded 30s: %s", elapsed)
			} else {
				t.Logf("production TTL12s / AckWait13s takeover=%s", elapsed)
			}
			store := journal.New(js)
			before, tail, err := store.Read(ctx, typ, id)
			if err != nil || len(before) == 0 || before[len(before)-1].Kind != journal.Completed || before[len(before)-1].Epoch <= old.Epoch {
				t.Fatalf("successor epoch/journal=%+v old=%+v err=%v", before, old, err)
			}
			select {
			case ticks <- time.Now():
			case <-ctx.Done():
				t.Fatal("old heartbeat did not resume")
			}
			select {
			case e := <-fenced:
				if e.Reason != "lease_heartbeat_lost" || e.Epoch != old.Epoch || e.Worker != first.ID || e.Error != lease.ErrLost.Error() {
					t.Fatalf("wrong fencing evidence: %+v", e)
				}
				t.Logf("old owner fenced: %+v", e)
			case <-ctx.Done():
				t.Fatal("old outer handler trapped lease-loss cancellation")
			}
			select {
			case <-nacked:
			case <-ctx.Done():
				t.Fatal("old delivery did not hand off")
			}
			stopFirst()
			select {
			case err := <-firstDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("old worker did not stop with handler held")
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
			releaseHandler()
			select {
			case err := <-late:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("late SDK append: %v", err)
				}
			case <-ctx.Done():
				t.Fatal("late handler did not return")
			}
			after, afterTail, err := store.Read(ctx, typ, id)
			if err != nil || tail != afterTail || !reflect.DeepEqual(before, after) || lateEffect.Load() {
				t.Fatalf("late handler changed journal/effect: %v", err)
			}
			report, err := integrity.Check(ctx, js)
			if err != nil || report.Invocations != 1 || report.Journals != 1 || report.Terminal != 1 {
				t.Fatalf("integrity=%+v err=%v", report, err)
			}
		})
	}
}
