package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/internal/testworkflow"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/testcluster"
	"js-wf/wf"
	"js-wf/worker"
)

func TestReplayContinuationPlugin(t *testing.T) {
	pluginPath := buildReplayPlugin(t)
	cluster, err := testcluster.Start(t.TempDir(), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	for ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, 3*time.Second)
		err = provision.Ensure(attempt, js, 3)
		stop()
		if err == nil {
			break
		}
		t.Logf("provision attempt: %v", err)
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	const typ, id = "continued", "plugin"
	definition := testworkflow.Definition
	w, err := worker.New(ctx, js, "plugin-worker", map[string]worker.Handler{typ: definition.Handler}, worker.WithContinuations(typ, definition.Continuations))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- w.RunPartition(ctx, identity.Partition(typ, id, provision.Partitions)) }()
	defer func() { cancel(); <-done }()
	c := client.New(js)
	if _, err := c.Start(ctx, typ, id, []byte(`7`)); err != nil {
		t.Fatal(err)
	}
	var suspended replayBundle
	for ctx.Err() == nil {
		candidate, err := fetchReplayBundle(ctx, js, typ, id)
		if err == nil && len(candidate.Journal) > 0 {
			tail := candidate.Journal[len(candidate.Journal)-1]
			if tail.Kind == journal.Suspended {
				var wait struct {
					WaitingOn string `json:"waiting_on"`
				}
				_ = json.Unmarshal(tail.Payload, &wait)
				if wait.WaitingOn == "signal:gate" {
					suspended = candidate
					break
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(suspended.Journal) == 0 {
		t.Fatal("no stage suspension")
	}
	before := testworkflow.Effects.Load()
	report, err := runReplayBundle(suspended, pluginPath, "ContinuedWorkflow")
	if err != nil || report.Status != "suspended" || report.WaitingOn != "signal:gate" {
		t.Fatalf("suspended=%+v err=%v", report, err)
	}
	if _, err := c.Signal(ctx, typ, id, "gate", []byte(`true`), "gate"); err != nil {
		t.Fatal(err)
	}
	result, err := c.Await(ctx, typ, id)
	if err != nil || string(result) != "60" {
		t.Fatalf("result=%s err=%v", result, err)
	}
	bundle, err := fetchReplayBundle(ctx, js, typ, id)
	if err != nil {
		t.Fatal(err)
	}
	if testworkflow.Effects.Load() != before+1 {
		t.Fatal("live effect count")
	}
	for _, symbol := range []string{"ContinuedWorkflow", "ContinuedFactory"} {
		report, err = runReplayBundle(bundle, pluginPath, symbol)
		if err != nil || report.Status != "completed" || string(report.Result) != "60" {
			t.Fatalf("%s: %+v err=%v", symbol, report, err)
		}
	}
	if testworkflow.Effects.Load() != before+1 {
		t.Fatal("offline effect ran")
	}
	if _, err := runReplayBundle(bundle, pluginPath, "ChangedContinuation"); !errors.Is(err, wf.ErrNonDeterministic) {
		t.Fatalf("changed stage=%v", err)
	}
	if _, err := runReplayBundle(bundle, pluginPath, "MissingContinuation"); !errors.Is(err, wf.ErrUnknownContinuation) {
		t.Fatalf("missing stage=%v", err)
	}
	// Synthetic failure tails exercise the CLI's specialized validators using
	// the real checkpoint prefix; they are not new real cancellation/cap proofs.
	canceled := suspended
	canceled.Journal = append([]journal.Record(nil), suspended.Journal...)
	last := canceled.Journal[len(canceled.Journal)-1]
	signal, _ := json.Marshal(map[string]any{"sig_seq": 1, "name": client.CancelSignalName, "payload": []byte(`null`)})
	failure, _ := json.Marshal(wf.Outcome{InvSeq: bundle.InvSeq, Error: client.ErrCancelled.Error()})
	canceled.Journal = append(canceled.Journal,
		journal.Record{Entry: journal.Entry{Index: last.Index + 1, Epoch: last.Epoch, Kind: journal.SignalConsumed, Payload: signal}, Sequence: last.Sequence + 1},
		journal.Record{Entry: journal.Entry{Index: last.Index + 2, Epoch: last.Epoch, Kind: journal.Failed, Payload: failure}, Sequence: last.Sequence + 2})
	if report, err := runReplayBundle(canceled, pluginPath, "ContinuedWorkflow"); err != nil || report.Status != "failed" || report.Error != client.ErrCancelled.Error() {
		t.Fatalf("canceled continuation=%+v err=%v", report, err)
	}
	limited := bundle
	for i, record := range bundle.Journal {
		var declaration struct{ Kind, Name string }
		if record.Kind != journal.StepRequested || json.Unmarshal(record.Payload, &declaration) != nil || declaration.Kind != "run" || declaration.Name != "double" {
			continue
		}
		limited.Journal = append([]journal.Record(nil), bundle.Journal[:i]...)
		failure, _ := json.Marshal(wf.Outcome{InvSeq: bundle.InvSeq, Error: journal.ErrTooLong.Error(), LimitRequest: record.Payload})
		record.Kind, record.Payload = journal.Failed, failure
		limited.Journal = append(limited.Journal, record)
		break
	}
	if len(limited.Journal) == len(bundle.Journal) {
		t.Fatal("no rejected-effect fixture")
	}
	if report, err := runReplayBundle(limited, pluginPath, "ContinuedWorkflow"); err != nil || report.Status != "failed" || report.Error != journal.ErrTooLong.Error() {
		t.Fatalf("limited continuation=%+v err=%v", report, err)
	}
	if testworkflow.Effects.Load() != before+1 {
		t.Fatal("failure replay executed effect")
	}
	corrupt := bundle
	corrupt.Objects = map[string][]byte{}
	if _, err := runReplayBundle(corrupt, pluginPath, "ContinuedWorkflow"); !errors.Is(err, wf.ErrReplayObjectMissing) {
		t.Fatalf("missing frame=%v", err)
	}
	raw, _ := json.Marshal(bundle)
	path := filepath.Join(t.TempDir(), "bundle.json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := run([]string{"-url", "nats://127.0.0.1:1", "-handler-plugin", pluginPath, "-handler-symbol", "ContinuedWorkflow", "-replay-bundle", path, "replay"}, &out); err != nil {
		t.Fatal(err)
	}
	if testworkflow.Effects.Load() != before+1 {
		t.Fatal("CLI effect ran")
	}
	if _, err := integrity.Check(ctx, js); err != nil {
		t.Fatal(err)
	}
	t.Logf("two continuation plugin stages: raw_entries=%d objects=%d result=%s offline_effects=0", len(bundle.Journal), len(bundle.Objects), result)
}
