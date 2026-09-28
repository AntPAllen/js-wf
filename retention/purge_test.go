package retention

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"
	"time"

	"js-wf/identity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/testcluster"
	"js-wf/wf"

	"github.com/nats-io/nats.go/jetstream"
)

func TestPurgeResumesAfterEveryStage(t *testing.T) {
	cluster, err := testcluster.Start(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := provision.Ensure(ctx, js, 1); err != nil {
		t.Fatal(err)
	}
	state, err := js.KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	const typ = "test"
	for _, stopAt := range []string{"marker", "signals", "journal", "snapshot", "tombstone", "invocation"} {
		t.Run(stopAt, func(t *testing.T) {
			id := "purge-" + stopAt
			ack, err := js.Publish(ctx, identity.InvocationSubject(typ, id), []byte(`null`))
			if err != nil {
				t.Fatal(err)
			}
			j := journal.New(js)
			seq, err := j.Append(ctx, typ, id, journal.Entry{Epoch: 1, Index: 0, Kind: journal.Started}, 0)
			if err != nil {
				t.Fatal(err)
			}
			terminal, _ := json.Marshal(wf.Outcome{InvSeq: ack.Sequence, Result: []byte(`1`)})
			if _, err := j.Append(ctx, typ, id, journal.Entry{Epoch: 1, Index: 1, Kind: journal.Completed, Payload: terminal}, seq); err != nil {
				t.Fatal(err)
			}
			if _, err := j.SnapshotPrefix(ctx, typ, id, 1); err != nil {
				t.Fatal(err)
			}
			if _, err := state.Put(ctx, identity.Key(typ, id), terminal); err != nil {
				t.Fatal(err)
			}
			if _, err := js.Publish(ctx, "wf.sig."+typ+"."+id+".go", []byte(`1`)); err != nil {
				t.Fatal(err)
			}
			injected := errors.New("injected stop")
			err = purge(ctx, js, typ, id, time.Minute, func(stage string) error {
				if stage == stopAt {
					return injected
				}
				return nil
			})
			if !errors.Is(err, injected) {
				t.Fatalf("stopped at %s: %v", stopAt, err)
			}
			if err := Purge(ctx, js, typ, id, time.Minute); err != nil {
				t.Fatalf("resume after %s: %v", stopAt, err)
			}
			if err := Purge(ctx, js, typ, id, time.Minute); err != nil {
				t.Fatalf("idempotent retry after %s: %v", stopAt, err)
			}
			value, err := state.Get(ctx, identity.Key(typ, id))
			if err != nil {
				t.Fatal(err)
			}
			marker, tomb, err := Decode(value.Value())
			if err != nil || !tomb || marker.InvSeq != ack.Sequence {
				t.Fatalf("tombstone=%+v found=%v err=%v", marker, tomb, err)
			}
			if _, err := state.Get(ctx, "purging."+identity.Key(typ, id)); !errors.Is(err, jetstream.ErrKeyNotFound) {
				t.Fatalf("purge marker remains: %v", err)
			}
			inv, _ := js.Stream(ctx, "WF_INV")
			if _, err := inv.GetLastMsgForSubject(ctx, identity.InvocationSubject(typ, id)); !errors.Is(err, jetstream.ErrMsgNotFound) {
				t.Fatalf("invocation remains: %v", err)
			}
			purges, err := js.Stream(ctx, "WF_PURGE")
			if err != nil {
				t.Fatal(err)
			}
			event, err := purges.GetLastMsgForSubject(ctx, "wf.purge."+typ+"."+id)
			if err != nil || string(event.Data) != strconv.FormatUint(ack.Sequence, 10) {
				t.Fatalf("purge event=%v err=%v", event, err)
			}
		})
	}
}

func TestPurgeFallbackTimersByGenerationAndResume(t *testing.T) {
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
	const typ, id = "test", "fallback-purge"
	ack, err := js.Publish(ctx, identity.InvocationSubject(typ, id), []byte(`null`))
	if err != nil {
		t.Fatal(err)
	}
	j := journal.New(js)
	seq, err := j.Append(ctx, typ, id, journal.Entry{Epoch: 1, Index: 0, Kind: journal.Started}, 0)
	if err != nil {
		t.Fatal(err)
	}
	terminal, _ := json.Marshal(wf.Outcome{InvSeq: ack.Sequence, Result: []byte(`true`)})
	if _, err := j.Append(ctx, typ, id, journal.Entry{Epoch: 1, Index: 1, Kind: journal.Completed, Payload: terminal}, seq); err != nil {
		t.Fatal(err)
	}
	state, err := js.KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.Put(ctx, identity.Key(typ, id), terminal); err != nil {
		t.Fatal(err)
	}
	currentSubject := identity.TimerSubject(typ, id, ack.Sequence, 0)
	otherSubject := identity.TimerSubject(typ, id, ack.Sequence+100, 0)
	for _, subject := range []string{currentSubject, otherSubject} {
		if _, err := js.Publish(ctx, subject, []byte(`{"fire_at":"2099-01-01T00:00:00Z"}`)); err != nil {
			t.Fatal(err)
		}
	}
	injected := errors.New("stop after fallback timer purge")
	err = purge(ctx, js, typ, id, time.Minute, func(stage string) error {
		if stage == "timers" {
			return injected
		}
		return nil
	})
	if !errors.Is(err, injected) {
		t.Fatalf("timer stage fault: %v", err)
	}
	timers, err := js.Stream(ctx, "WF_TIMER")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := timers.GetLastMsgForSubject(ctx, currentSubject); !errors.Is(err, jetstream.ErrMsgNotFound) {
		t.Fatalf("current generation timer retained: %v", err)
	}
	if _, err := timers.GetLastMsgForSubject(ctx, otherSubject); err != nil {
		t.Fatalf("other generation timer removed: %v", err)
	}
	if err := Purge(ctx, js, typ, id, time.Minute); err != nil {
		t.Fatalf("resume after timer stage: %v", err)
	}
	if _, err := timers.GetLastMsgForSubject(ctx, otherSubject); err != nil {
		t.Fatalf("resume removed other generation timer: %v", err)
	}
}
