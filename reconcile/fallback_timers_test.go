package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"
	"time"

	"js-wf/identity"
	"js-wf/provision"
	"js-wf/retention"
	"js-wf/testcluster"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestFallbackTimerScanRetriesCommittedWakeupBeforeDelete(t *testing.T) {
	cluster, err := testcluster.Start(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := provision.EnsureFallback(ctx, js, 1); err != nil {
		t.Fatal(err)
	}
	const typ, id = "fallback", "lost-delete"
	const generation, step = uint64(42), uint64(7)
	payload, _ := json.Marshal(struct {
		FireAt time.Time `json:"fire_at"`
	}{time.Now().Add(-time.Second)})
	timerSubject := identity.TimerSubject(typ, id, generation, step)
	if _, err := js.Publish(ctx, timerSubject, payload); err != nil {
		t.Fatal(err)
	}
	wakeup := &nats.Msg{Subject: identity.RunSubject(typ, id, provision.Partitions), Data: []byte(identity.Key(typ, id)), Header: nats.Header{}}
	wakeup.Header.Set(identity.TimerInvSeqHeader, strconv.FormatUint(generation, 10))
	wakeup.Header.Set(identity.TimerStepHeader, strconv.FormatUint(step, 10))
	messageID := "fallback-timer:fallback:lost-delete:42:7"
	first, err := js.PublishMsg(ctx, wakeup, jetstream.WithMsgID(messageID))
	if err != nil {
		t.Fatal(err)
	}
	scan := NewFallbackTimerScan(js)
	result, err := scan.Scan(ctx, 1, 10, false)
	if err != nil || result.Reenqueued != 1 {
		t.Fatalf("fallback retry result=%+v err=%v", result, err)
	}
	run, err := js.Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	info, err := run.Info(ctx)
	if err != nil || info.State.LastSeq != first.Sequence {
		t.Fatalf("duplicate wakeup stored a new message: first=%d info=%+v err=%v", first.Sequence, info, err)
	}
	timers, err := js.Stream(ctx, "WF_TIMER")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := timers.GetLastMsgForSubject(ctx, timerSubject); !errors.Is(err, jetstream.ErrMsgNotFound) {
		t.Fatalf("timer remained after duplicate publish acknowledgement: %v", err)
	}
}

func TestFallbackTimerScanRemovesRetiredGenerations(t *testing.T) {
	cluster, err := testcluster.Start(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := provision.EnsureFallback(ctx, js, 1); err != nil {
		t.Fatal(err)
	}
	const typ, id = "fallback", "retired"
	state, err := js.KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.Put(ctx, "purging."+identity.Key(typ, id), []byte("42")); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	tomb, _ := json.Marshal(retention.Tombstone{Tombstone: true, InvSeq: 43, PurgedAt: now, ExpiresAt: now.Add(time.Minute)})
	if _, err := state.Put(ctx, identity.Key(typ, id), tomb); err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(struct {
		FireAt time.Time `json:"fire_at"`
	}{now.Add(time.Hour)})
	for _, generation := range []uint64{42, 43, 44} {
		if _, err := js.Publish(ctx, identity.TimerSubject(typ, id, generation, 0), payload); err != nil {
			t.Fatal(err)
		}
	}
	scan := NewFallbackTimerScan(js)
	dry, err := scan.Scan(ctx, 1, 10, true)
	if err != nil || dry.Removed != 2 || dry.Reenqueued != 0 {
		t.Fatalf("retired timer dry run=%+v err=%v", dry, err)
	}
	result, err := scan.Scan(ctx, 1, 10, false)
	if err != nil || result.Removed != 2 || result.Reenqueued != 0 {
		t.Fatalf("retired timer scan=%+v err=%v", result, err)
	}
	timers, err := js.Stream(ctx, "WF_TIMER")
	if err != nil {
		t.Fatal(err)
	}
	for _, generation := range []uint64{42, 43} {
		if _, err := timers.GetLastMsgForSubject(ctx, identity.TimerSubject(typ, id, generation, 0)); !errors.Is(err, jetstream.ErrMsgNotFound) {
			t.Fatalf("retired generation %d timer remains: %v", generation, err)
		}
	}
	if _, err := timers.GetLastMsgForSubject(ctx, identity.TimerSubject(typ, id, 44, 0)); err != nil {
		t.Fatalf("live generation timer removed: %v", err)
	}
}
