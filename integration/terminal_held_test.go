package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/provision"
	"js-wf/wf"
	"js-wf/worker"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func TestTerminalWakeupsDrainWhileHealthyLeaseHeld(t *testing.T) {
	for _, timer := range []bool{false, true} {
		t.Run(fmt.Sprintf("timer=%t", timer), func(t *testing.T) { runTerminalWakeupsDrainWhileHealthyLeaseHeld(t, timer) })
	}
}

func runTerminalWakeupsDrainWhileHealthyLeaseHeld(t *testing.T, timer bool) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	const typ, id = "terminal-held", "completed"
	var effects, fastAcks atomic.Int64
	w, err := worker.New(ctx, all[1], "terminal-drain", map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		if timer {
			handle, err := c.Timer("late", 50*time.Millisecond)
			if err != nil {
				return nil, err
			}
			if err := handle.Cancel(); err != nil {
				return nil, err
			}
		}
		v, err := wf.Run(c, "effect", 42, func(context.Context) (int, error) { effects.Add(1); return 42, nil })
		raw, _ := json.Marshal(v)
		return raw, err
	}}, worker.WithDispatchObserver(func(e worker.DispatchEvent) {
		if e.Stage == "terminal_held" {
			fastAcks.Add(1)
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- w.RunPartition(ctx, identity.Partition(typ, id, provision.Partitions)) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	c := client.New(all[0])
	startedInvocation, err := c.Start(ctx, typ, id, []byte(`null`))
	if err != nil {
		t.Fatal(err)
	}
	if result, err := c.Await(ctx, typ, id); err != nil || string(result) != "42" {
		t.Fatalf("result=%s err=%v", result, err)
	}
	stream, err := all[0].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	wait := func(budget time.Duration) error {
		deadline, stop := context.WithTimeout(ctx, budget)
		defer stop()
		for deadline.Err() == nil {
			attempt, cancel := context.WithTimeout(deadline, time.Second)
			info, err := stream.Info(attempt)
			cancel()
			if err == nil && info.State.Msgs == 0 {
				return nil
			}
			time.Sleep(10 * time.Millisecond)
		}
		return deadline.Err()
	}
	if err := wait(4 * time.Second); err != nil {
		t.Fatal(err)
	}
	leasing, err := lease.New(ctx, all[2])
	if err != nil {
		t.Fatal(err)
	}
	holder, err := leasing.Acquire(ctx, typ, id, "healthy-terminal-owner")
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Release(context.Background())
	kv, err := all[0].KeyValue(ctx, "WF_LEASE")
	if err != nil {
		t.Fatal(err)
	}
	before, err := kv.Get(ctx, identity.Key(typ, id))
	if err != nil {
		t.Fatal(err)
	}
	prefix, tail, err := journal.New(all[0]).Read(ctx, typ, id)
	if err != nil {
		t.Fatal(err)
	}
	state, err := all[0].KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := state.Get(ctx, identity.Key(typ, id))
	if err != nil {
		t.Fatal(err)
	}
	noOpsBefore := w.Metrics().CancelledTimerNoOps
	const count = 32
	started := time.Now()
	for i := 0; i < count; i++ {
		message := &nats.Msg{Subject: identity.RunSubject(typ, id, provision.Partitions), Data: []byte(identity.Key(typ, id)), Header: nats.Header{}}
		if timer {
			message.Header.Set(identity.TimerInvSeqHeader, strconv.FormatUint(startedInvocation.InvSeq, 10))
			message.Header.Set(identity.TimerStepHeader, "0")
		}
		if _, err := all[0].PublishMsg(ctx, message, jetstream.WithMsgID(fmt.Sprintf("terminal-held:%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := wait(4 * time.Second); err != nil {
		t.Fatalf("terminal wakeups failed to drain under a healthy held lease: %v", err)
	}
	elapsed := time.Since(started)
	after, err := kv.Get(ctx, identity.Key(typ, id))
	if err != nil || before.Revision() != after.Revision() || !bytes.Equal(before.Value(), after.Value()) {
		t.Fatalf("healthy lease changed: before=%d after=%v err=%v", before.Revision(), after, err)
	}
	if effects.Load() != 1 || fastAcks.Load() != count {
		t.Fatalf("effects=%d fast_acks=%d", effects.Load(), fastAcks.Load())
	}
	records, newTail, err := journal.New(all[2]).Read(ctx, typ, id)
	if err != nil || newTail != tail || !reflect.DeepEqual(records, prefix) {
		t.Fatalf("terminal journal changed: %v", err)
	}
	current, err := state.Get(ctx, identity.Key(typ, id))
	if err != nil || current.Revision() != terminal.Revision() || !bytes.Equal(current.Value(), terminal.Value()) {
		t.Fatal("terminal outcome changed")
	}
	if timer && w.Metrics().CancelledTimerNoOps != noOpsBefore+count {
		t.Fatalf("canceled timer noops=%d want=%d", w.Metrics().CancelledTimerNoOps, noOpsBefore+count)
	}
	if _, err := integrity.Check(ctx, all[2]); err != nil {
		t.Fatal(err)
	}
	t.Logf("terminal held wakeups=%d drain=%s effects=%d owner_revision=%d unchanged", count, elapsed, effects.Load(), after.Revision())
}
