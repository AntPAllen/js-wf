package sim

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/retention"

	"github.com/nats-io/nats.go"
)

func TestClientSignalTransportFaultBoundaries(t *testing.T) {
	ctx := context.Background()
	model := NewSignalTransport(NewScheduler(41))
	c := client.NewWithSignalPorts(model, model)
	const typ, id = "test", "signals"
	generation, err := model.PublishInvocation(ctx, &nats.Msg{Subject: identity.InvocationSubject(typ, id), Data: []byte("input")})
	if err != nil {
		t.Fatal(err)
	}
	first, err := c.Signal(ctx, typ, id, "go", []byte(`"first"`), "first")
	if err != nil || first == 0 || len(model.SignalFor(typ, id, "go")) != 1 || len(model.Runs()) != 1 {
		t.Fatalf("first signal: seq=%d signals=%v runs=%v err=%v", first, model.SignalFor(typ, id, "go"), model.Runs(), err)
	}
	duplicate, err := c.Signal(ctx, typ, id, "go", []byte(`"first"`), "first")
	if err != nil || duplicate != first || len(model.SignalFor(typ, id, "go")) != 1 || len(model.Runs()) != 1 {
		t.Fatalf("matching duplicate: seq=%d signals=%v runs=%v err=%v", duplicate, model.SignalFor(typ, id, "go"), model.Runs(), err)
	}
	if _, err := c.Signal(ctx, typ, id, "go", []byte(`"changed"`), "first"); !errors.Is(err, client.ErrSignalMismatch) {
		t.Fatalf("changed duplicate: %v", err)
	}
	if err := model.QueueSignalFault(SignalDropBeforeCommit); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Signal(ctx, typ, id, "go", []byte(`"drop"`), "drop"); !errors.Is(err, client.ErrSignalUnknown) || len(model.SignalFor(typ, id, "go")) != 1 {
		t.Fatalf("dropped publish: signals=%v err=%v", model.SignalFor(typ, id, "go"), err)
	}
	droppedRetry, err := c.Signal(ctx, typ, id, "go", []byte(`"drop"`), "drop")
	if err != nil || droppedRetry <= first || len(model.SignalFor(typ, id, "go")) != 2 {
		t.Fatalf("dropped retry: seq=%d signals=%v err=%v", droppedRetry, model.SignalFor(typ, id, "go"), err)
	}
	if err := model.QueueSignalFault(SignalLoseAckAfterCommit); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Signal(ctx, typ, id, "go", []byte(`"lost"`), "lost"); !errors.Is(err, client.ErrSignalUnknown) || len(model.SignalFor(typ, id, "go")) != 3 || len(model.Runs()) != 2 {
		t.Fatalf("lost publish acknowledgment: signals=%v runs=%v err=%v", model.SignalFor(typ, id, "go"), model.Runs(), err)
	}
	lostRetry, err := c.Signal(ctx, typ, id, "go", []byte(`"lost"`), "lost")
	if err != nil || lostRetry <= droppedRetry || len(model.SignalFor(typ, id, "go")) != 3 || len(model.Runs()) != 3 {
		t.Fatalf("lost acknowledgment retry: seq=%d signals=%v runs=%v err=%v", lostRetry, model.SignalFor(typ, id, "go"), model.Runs(), err)
	}
	if got := model.SignalGeneration(lostRetry); got != strconv.FormatUint(generation, 10) {
		t.Fatalf("signal generation=%q want=%d", got, generation)
	}
	if _, err := c.Signal(ctx, typ, id, "go", []byte(`"different"`), "lost"); !errors.Is(err, client.ErrSignalMismatch) {
		t.Fatalf("lost acknowledgment changed retry: %v", err)
	}
}

func TestClientSignalTransportTombstoneAndReuse(t *testing.T) {
	ctx := context.Background()
	model := NewSignalTransport(NewScheduler(43))
	c := client.NewWithSignalPorts(model, model)
	const typ, id = "test", "reused"
	subject := identity.InvocationSubject(typ, id)
	oldGeneration, err := model.PublishInvocation(ctx, &nats.Msg{Subject: subject, Data: []byte("old")})
	if err != nil {
		t.Fatal(err)
	}
	purgedAt := time.Unix(100, 0).UTC()
	marker, err := json.Marshal(retention.Tombstone{Tombstone: true, InvSeq: oldGeneration, PurgedAt: purgedAt, ExpiresAt: purgedAt.Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	model.SetState(identity.Key(typ, id), marker)
	if _, err := c.Signal(ctx, typ, id, "go", []byte("old"), "old"); !errors.Is(err, client.ErrPurged) {
		t.Fatalf("signal to tombstoned generation: %v", err)
	}
	model.PurgeInvocation(subject)
	if _, err := c.SignalToGeneration(ctx, typ, id, "go", []byte("old"), "old", oldGeneration); !errors.Is(err, client.ErrStaleGeneration) {
		t.Fatalf("signal to removed generation: %v", err)
	}
	newGeneration, err := model.PublishInvocation(ctx, &nats.Msg{Subject: subject, Data: []byte("new")})
	if err != nil || newGeneration <= oldGeneration {
		t.Fatalf("new generation=%d old=%d err=%v", newGeneration, oldGeneration, err)
	}
	if sequence, err := c.Signal(ctx, typ, id, "go", []byte("new"), "new"); err != nil || sequence == 0 || len(model.SignalFor(typ, id, "go")) != 1 {
		t.Fatalf("signal to reused ID: seq=%d signals=%v err=%v", sequence, model.SignalFor(typ, id, "go"), err)
	}
}

func TestClientSignalTransportGenerationTerminalAndBlob(t *testing.T) {
	ctx := context.Background()
	model := NewSignalTransport(NewScheduler(42))
	c := client.NewWithSignalPorts(model, model)
	const typ, id = "test", "signal-options"
	generation, err := model.PublishInvocation(ctx, &nats.Msg{Subject: identity.InvocationSubject(typ, id), Data: []byte("input")})
	if err != nil {
		t.Fatal(err)
	}
	large := bytes.Repeat([]byte("z"), client.MaxInlineSignal+1)
	sequence, err := c.SignalWithOptions(ctx, typ, id, "large", large, "large", client.SignalOptions{RequireRunning: true})
	if err != nil || sequence == 0 {
		t.Fatalf("large signal: seq=%d err=%v", sequence, err)
	}
	stored, err := model.GetSignal(ctx, sequence)
	if err != nil || len(stored.Data) != 0 || !bytes.Equal(model.Input(stored.Header.Get("Wf-Signal-Ref")), large) {
		t.Fatalf("large signal object: stored=%+v err=%v", stored, err)
	}
	model.SetJournal(typ, id, []journal.Record{{Entry: journal.Entry{Kind: journal.Completed}}})
	if _, err := c.SignalWithOptions(ctx, typ, id, "go", []byte("x"), "terminal", client.SignalOptions{RequireRunning: true}); !errors.Is(err, client.ErrNotRunning) {
		t.Fatalf("terminal RequireRunning signal: %v", err)
	}
	model.PurgeInvocation(identity.InvocationSubject(typ, id))
	newGeneration, err := model.PublishInvocation(ctx, &nats.Msg{Subject: identity.InvocationSubject(typ, id), Data: []byte("new")})
	if err != nil || newGeneration <= generation {
		t.Fatalf("replacement generation=%d old=%d err=%v", newGeneration, generation, err)
	}
	if _, err := c.SignalToGeneration(ctx, typ, id, "go", []byte("x"), "stale", generation); !errors.Is(err, client.ErrStaleGeneration) {
		t.Fatalf("retired generation accepted: %v", err)
	}
}
