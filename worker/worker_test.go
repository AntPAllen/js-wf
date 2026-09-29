package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/history"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/testcluster"
	"js-wf/wf"

	"github.com/anishathalye/porcupine"
	"github.com/nats-io/nats.go/jetstream"
)

func TestJournalLimitRecordsTerminalFailure(t *testing.T) {
	cluster, err := testcluster.Start(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := provision.Ensure(ctx, js, 1); err != nil {
		t.Fatal(err)
	}
	var effects atomic.Int64
	w, err := New(ctx, js, "limit-worker", map[string]Handler{"test": func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		for i := 0; i < 10; i++ {
			if _, err := wf.Run(c, "step", i, func(context.Context) (int, error) {
				effects.Add(1)
				return i, nil
			}); err != nil {
				return nil, err
			}
		}
		return json.RawMessage(`1`), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	w.maxEntries = 8
	if _, err := client.New(js).Start(ctx, "test", "limit", []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- w.RunPartition(workerCtx, identity.Partition("test", "limit", provision.Partitions)) }()
	recorder := &history.Recorder{}
	observed := client.NewObserved(js, recorder)
	_, err = observed.Await(ctx, "test", "limit")
	if err == nil || !strings.Contains(err.Error(), journal.ErrTooLong.Error()) {
		t.Fatalf("terminal error: %v", err)
	}
	_, err = observed.Await(ctx, "test", "limit")
	if err == nil || !strings.Contains(err.Error(), journal.ErrTooLong.Error()) {
		t.Fatalf("reread terminal error: %v", err)
	}
	if result, err := history.CheckResults(recorder.Snapshot(), time.Second); err != nil || result != porcupine.Ok {
		t.Fatalf("failed result history=%s err=%v", result, err)
	}
	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	records, _, err := journal.New(js).Read(ctx, "test", "limit")
	if err != nil || len(records) != 8 || effects.Load() != 3 {
		t.Fatalf("records=%d effects=%d err=%v", len(records), effects.Load(), err)
	}
	if records[7].Kind != journal.Failed {
		t.Fatalf("terminal kind=%s", records[7].Kind)
	}
	var outcome wf.Outcome
	if err := json.Unmarshal(records[7].Payload, &outcome); err != nil {
		t.Fatal(err)
	}
	var attempted struct {
		Kind      string `json:"kind"`
		Name      string `json:"name"`
		InputHash string `json:"input_hash"`
	}
	if err := json.Unmarshal(outcome.LimitRequest, &attempted); err != nil {
		t.Fatal(err)
	}
	inputHash := sha256.Sum256([]byte(`3`))
	if outcome.Error != journal.ErrTooLong.Error() || attempted.Kind != "run" || attempted.Name != "step" || attempted.InputHash != hex.EncodeToString(inputHash[:]) {
		t.Fatalf("journal limit metadata: outcome=%+v attempted=%+v", outcome, attempted)
	}
	if _, err := integrity.Check(ctx, js); err != nil {
		t.Fatal(err)
	}
}

func TestJournalLimitRecordsRejectedSignalDrain(t *testing.T) {
	cluster, err := testcluster.Start(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	if err := provision.Ensure(ctx, js, 1); err != nil {
		t.Fatal(err)
	}
	const typ, id = "signal-limit", "drain"
	w, err := New(ctx, js, "signal-limit-worker", map[string]Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		_, err := wf.AwaitSignal(c, "go")
		return json.RawMessage(`true`), err
	}})
	if err != nil {
		t.Fatal(err)
	}
	w.maxEntries = 4
	c := client.New(js)
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- w.RunPartition(workerCtx, identity.Partition(typ, id, provision.Partitions)) }()
	store := journal.New(js)
	for ctx.Err() == nil {
		records, _, err := store.Read(ctx, typ, id)
		if err != nil {
			t.Fatal(err)
		}
		if len(records) == 3 && records[2].Kind == journal.Suspended {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatalf("handler did not suspend before signal: %v", ctx.Err())
	}
	signalSeq, err := c.Signal(ctx, typ, id, "go", []byte(`true`), "at-limit")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Await(ctx, typ, id); err == nil || !strings.Contains(err.Error(), journal.ErrTooLong.Error()) {
		t.Fatalf("terminal journal-limit result: %v", err)
	}
	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	records, _, err := store.Read(ctx, typ, id)
	if err != nil || len(records) != 4 || records[3].Kind != journal.Failed {
		t.Fatalf("rejected signal journal: records=%+v err=%v", records, err)
	}
	var outcome wf.Outcome
	if err := json.Unmarshal(records[3].Payload, &outcome); err != nil || outcome.LimitEntry == nil || outcome.LimitEntry.Kind != string(journal.SignalConsumed) {
		t.Fatalf("rejected signal metadata: outcome=%+v err=%v", outcome, err)
	}
	var attempted struct {
		Sequence uint64 `json:"sig_seq"`
		Name     string `json:"name"`
		Payload  []byte `json:"payload"`
		Hash     string `json:"hash"`
	}
	if err := json.Unmarshal(outcome.LimitEntry.Payload, &attempted); err != nil || attempted.Sequence != signalSeq || attempted.Name != "go" || string(attempted.Payload) != "true" {
		t.Fatalf("attempted signal drain=%+v err=%v", attempted, err)
	}
	digest := sha256.Sum256([]byte(`true`))
	if attempted.Hash != hex.EncodeToString(digest[:]) {
		t.Fatalf("attempted signal hash=%q", attempted.Hash)
	}
	if _, err := integrity.Check(ctx, js); err != nil {
		t.Fatal(err)
	}
}

func TestJournalLimitRecordsRejectedPanicAttempt(t *testing.T) {
	cluster, err := testcluster.Start(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := provision.Ensure(ctx, js, 1); err != nil {
		t.Fatal(err)
	}
	w, err := New(ctx, js, "limit-panic-worker", map[string]Handler{"test": func(*wf.Context, json.RawMessage) (json.RawMessage, error) {
		panic("limit panic")
	}})
	if err != nil {
		t.Fatal(err)
	}
	w.maxEntries = 2
	if _, err := client.New(js).Start(ctx, "test", "limit-panic", []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- w.RunPartition(workerCtx, identity.Partition("test", "limit-panic", provision.Partitions))
	}()
	_, err = client.New(js).Await(ctx, "test", "limit-panic")
	if err == nil || !strings.Contains(err.Error(), journal.ErrTooLong.Error()) {
		t.Fatalf("terminal error: %v", err)
	}
	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	records, _, err := journal.New(js).Read(ctx, "test", "limit-panic")
	if err != nil || len(records) != 2 || records[1].Kind != journal.Failed {
		t.Fatalf("records=%+v err=%v", records, err)
	}
	var outcome wf.Outcome
	if err := json.Unmarshal(records[1].Payload, &outcome); err != nil {
		t.Fatal(err)
	}
	if outcome.LimitEntry == nil || outcome.LimitEntry.Kind != string(journal.Attempt) {
		t.Fatalf("missing attempted panic entry: %+v", outcome)
	}
	attempt, err := journal.DecodeAttempt(outcome.LimitEntry.Payload)
	if err != nil || attempt.Count != 1 || attempt.Error != "workflow panic: limit panic" {
		t.Fatalf("attempt=%+v err=%v", attempt, err)
	}
}

func TestWorkerConstructorBoundsMetadataLookup(t *testing.T) {
	cluster, err := testcluster.Start(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := provision.Ensure(ctx, js, 1); err != nil {
		t.Fatal(err)
	}
	proxy, err := testcluster.NewClientProxy(cluster.Servers[0].ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	nc, err := proxy.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	proxied, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	proxy.HoldResponses()
	start := time.Now()
	_, err = New(ctx, proxied, "bounded-constructor", nil)
	if err == nil || !strings.Contains(err.Error(), "lease bucket") || ctx.Err() != nil || time.Since(start) > 8*time.Second {
		t.Fatalf("bounded constructor: elapsed=%s err=%v context=%v", time.Since(start), err, ctx.Err())
	}
}
