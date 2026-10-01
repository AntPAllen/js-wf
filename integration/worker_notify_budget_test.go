package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/provision"
	"js-wf/wf"
	"js-wf/worker"
)

// Keep real stores and production SDK calls; withhold one reply at the
// worker's JetStream boundary after the child handler has executed.
type missingParentReply struct {
	jetstream.JetStream
	mode         string
	armed, fired atomic.Bool
	entered      chan error
	deadline     time.Time
}

func (p *missingParentReply) stall(ctx context.Context) error {
	deadline, ok := ctx.Deadline()
	p.deadline = deadline
	if !ok || time.Until(deadline) > 15*time.Second {
		err := fmt.Errorf("parent notification uses delivery lifetime rather than 15s budget")
		p.entered <- err
		return err
	}
	p.entered <- nil
	<-ctx.Done()
	return ctx.Err()
}
func (p *missingParentReply) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	if p.mode == "invocation_drop" && name == "WF_INV" && p.armed.Load() && p.fired.CompareAndSwap(false, true) {
		return nil, p.stall(ctx)
	}
	return p.JetStream.Stream(ctx, name)
}
func (p *missingParentReply) KeyValue(ctx context.Context, name string) (jetstream.KeyValue, error) {
	if p.mode == "state_drop" && name == "WF_STATE" && p.armed.Load() && p.fired.CompareAndSwap(false, true) {
		return nil, p.stall(ctx)
	}
	return p.JetStream.KeyValue(ctx, name)
}
func (p *missingParentReply) PublishMsg(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	if strings.HasPrefix(p.mode, "publish_") && strings.HasPrefix(msg.Subject, "wf.sig.") && p.armed.Load() && p.fired.CompareAndSwap(false, true) {
		if p.mode == "publish_ack_lost" {
			if _, err := p.JetStream.PublishMsg(ctx, msg, opts...); err != nil {
				return nil, err
			}
		}
		return nil, p.stall(ctx)
	}
	return p.JetStream.PublishMsg(ctx, msg, opts...)
}
func TestWorkerParentNotificationMissingReplyRecovery(t *testing.T) {
	for _, mode := range []string{"invocation_drop", "state_drop", "publish_drop", "publish_ack_lost"} {
		t.Run(mode, func(t *testing.T) {
			all, _ := setup(t)
			ctx, cancel := context.WithTimeout(context.Background(), 55*time.Second)
			defer cancel()
			const parentType, parentID, childType, childID = "notify-parent", "p", "notify-child", "c"
			port := &missingParentReply{JetStream: all[1], mode: mode, entered: make(chan error, 1)}
			var effects, heartbeats atomic.Int64
			w, err := worker.New(ctx, port, "notify-budget-owner", map[string]worker.Handler{
				parentType: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
					v, err := wf.AwaitSignal(c, "child")
					if err != nil {
						return nil, err
					}
					return v, nil
				},
				childType: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
					v, err := wf.RunOnce(c, "effect", 42, func(context.Context, string) (int, error) { effects.Add(1); return 42, nil })
					if err != nil {
						return nil, err
					}
					port.armed.Store(true)
					return json.Marshal(v)
				},
			}, worker.WithPartitionConcurrency(2), worker.WithOperationObserver(func(e worker.OperationEvent) {
				if e.Operation == "lease_renew_heartbeat" && e.Error == "" {
					heartbeats.Add(1)
				}
			}))
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			c := client.New(all[0])
			parent, err := c.Start(ctx, parentType, parentID, []byte(`null`))
			if err != nil {
				t.Fatal(err)
			}
			child, err := c.StartChild(ctx, childType, childID, []byte(`null`), parentType, parentID, parent.InvSeq, "child")
			if err != nil {
				t.Fatal(err)
			}
			runCtx, stop := context.WithCancel(ctx)
			done := make(chan error, 2)
			partitions := map[uint32]bool{identity.Partition(parentType, parentID, provision.Partitions): true, identity.Partition(childType, childID, provision.Partitions): true}
			for part := range partitions {
				go func(part uint32) { done <- w.RunPartition(runCtx, part) }(part)
			}
			defer func() {
				stop()
				for range partitions {
					if err := <-done; err != nil {
						t.Error(err)
					}
				}
			}()
			started := time.Now()
			select {
			case err := <-port.entered:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			prefix, _, err := journal.New(all[2]).Read(ctx, childType, childID)
			if err != nil || len(prefix) == 0 || prefix[len(prefix)-1].Kind != journal.Completed {
				t.Fatalf("child terminal prefix: %v", err)
			}
			state, err := all[2].KeyValue(ctx, "WF_STATE")
			if err != nil {
				t.Fatal(err)
			}
			before, err := state.Get(ctx, identity.Key(childType, childID))
			if err != nil {
				t.Fatal(err)
			}
			timer := time.NewTimer(time.Until(port.deadline.Add(-2 * time.Second)))
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			leases, err := lease.New(ctx, all[2])
			if err != nil {
				t.Fatal(err)
			}
			if l, err := leases.Acquire(ctx, childType, childID, "competitor"); !errors.Is(err, lease.ErrHeld) {
				if err == nil {
					_ = l.Release(ctx)
				}
				t.Fatalf("child lost live ownership before budget expiry: %v", err)
			}
			if heartbeats.Load() < 2 {
				t.Fatalf("heartbeat renewals=%d", heartbeats.Load())
			}
			got, err := c.Await(ctx, parentType, parentID)
			if err != nil {
				t.Fatal(err)
			}
			var outcome struct {
				InvSeq uint64 `json:"inv_seq"`
				Result []byte `json:"result"`
			}
			if err = json.Unmarshal(got, &outcome); err != nil || outcome.InvSeq != child.InvSeq || string(outcome.Result) != "42" {
				t.Fatalf("parent result=%s err=%v", got, err)
			}
			// Await observes durable state before lease cleanup. Poll boundedly for the
			// actual repaired notification and released lease.
			leaseKV, err := all[2].KeyValue(ctx, "WF_LEASE")
			if err != nil {
				t.Fatal(err)
			}
			for {
				_, err = leaseKV.Get(ctx, identity.Key(childType, childID))
				if errors.Is(err, jetstream.ErrKeyNotFound) {
					break
				}
				select {
				case <-time.After(50 * time.Millisecond):
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			after, _, err := journal.New(all[2]).Read(ctx, childType, childID)
			if err != nil || !reflect.DeepEqual(prefix, after) || effects.Load() != 1 {
				t.Fatalf("terminal replay changed prefix/effects: effects=%d err=%v", effects.Load(), err)
			}
			current, err := state.Get(ctx, identity.Key(childType, childID))
			if err != nil || current.Revision() != before.Revision() {
				t.Fatalf("child terminal state changed: %v", err)
			}
			sig, err := all[2].Stream(ctx, "WF_SIG")
			if err != nil {
				t.Fatal(err)
			}
			info, err := sig.Info(ctx)
			if err != nil || info.State.Msgs != 1 {
				t.Fatalf("deduplicated parent signal: info=%+v err=%v", info, err)
			}
			root := os.Getenv("WF_NOTIFY_ARTIFACT_ROOT")
			if root != "" {
				if err := os.MkdirAll(root, 0755); err != nil {
					t.Fatal(err)
				}
				proof := struct {
					Mode                 string
					Prefix, After        []journal.Record
					Before, AfterOutcome []byte
					ParentResult         []byte
					SignalInfo           *jetstream.StreamInfo
					Effects, Heartbeats  int64
				}{mode, prefix, after, before.Value(), current.Value(), got, info, effects.Load(), heartbeats.Load()}
				data, err := json.MarshalIndent(proof, "", "  ")
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(root, mode+".json"), data, 0644); err != nil {
					t.Fatal(err)
				}
			}
			if recovery := time.Since(started); recovery >= 30*time.Second {
				t.Fatalf("notification recovery=%s", recovery)
			} else {
				t.Logf("mode=%s recovery=%s effects=%d heartbeats=%d signals=%d", mode, recovery, effects.Load(), heartbeats.Load(), info.State.Msgs)
			}
		})
	}
}
