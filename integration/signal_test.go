package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/history"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/reconcile"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/anishathalye/porcupine"
)

func TestSignalSuspendsAndResumes(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	recorder := &history.Recorder{}
	w, err := worker.New(ctx, all[1], "signal-worker", map[string]worker.Handler{"test": func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		value, err := wf.AwaitSignal(c, "go")
		if err != nil {
			return nil, err
		}
		return json.RawMessage(value), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	c := client.NewObserved(all[0], recorder)
	if _, err := c.Start(ctx, "test", "late-signal", []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() {
		done <- w.RunPartition(workerCtx, identity.Partition("test", "late-signal", provision.Partitions))
	}()
	j := journal.New(all[2])
	for {
		entries, _, err := j.Read(ctx, "test", "late-signal")
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) > 0 && entries[len(entries)-1].Kind == journal.Suspended {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("workflow did not suspend")
		}
		time.Sleep(30 * time.Millisecond)
	}
	if _, err := c.SignalWithOptions(ctx, "test", "late-signal", "go", []byte(`{"answer":42}`), "key-1", client.SignalOptions{RequireRunning: true}); err != nil {
		t.Fatal(err)
	}
	result, err := c.Await(ctx, "test", "late-signal")
	if err != nil || string(result) != `{"answer":42}` {
		t.Fatalf("result=%s err=%v", result, err)
	}
	stop()
	<-done
	entries, _, err := j.Read(ctx, "test", "late-signal")
	if err != nil {
		t.Fatal(err)
	}
	var suspended, consumed, completed int
	for _, e := range entries {
		switch e.Kind {
		case journal.Suspended:
			suspended++
		case journal.SignalConsumed:
			consumed++
		case journal.Completed:
			completed++
		}
	}
	if suspended != 1 || consumed != 1 || completed != 1 {
		t.Fatalf("journal: suspended=%d consumed=%d completed=%d", suspended, consumed, completed)
	}
	operations := recorder.Snapshot()
	if len(operations) != 3 || operations[0].Op != "start" || operations[1].Op != "signal" || operations[2].Op != "getResult" {
		t.Fatalf("observed operations=%+v", operations)
	}
	for _, operation := range operations {
		if operation.InvokeTS.IsZero() || operation.ReturnTS.Before(operation.InvokeTS) || len(operation.Args) == 0 || len(operation.Result) == 0 {
			t.Fatalf("incomplete observed operation=%+v", operation)
		}
	}
	for i, status := range []string{"started", "signaled", "completed"} {
		var observed struct {
			Status     string `json:"status"`
			ResultHash string `json:"result_hash"`
		}
		if err := json.Unmarshal(operations[i].Result, &observed); err != nil || observed.Status != status {
			t.Fatalf("operation %d status=%+v err=%v", i, observed, err)
		}
		if i == 2 {
			digest := sha256.Sum256(result)
			if observed.ResultHash != hex.EncodeToString(digest[:]) {
				t.Fatalf("result hash=%s", observed.ResultHash)
			}
		}
	}
	if _, err := integrity.Check(ctx, all[0]); err != nil {
		t.Fatalf("integrity: %v", err)
	}
}

func TestSignalsBufferedAndIdempotent(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	recorder := &history.Recorder{}
	c := client.NewObserved(all[0], recorder)
	if _, err := c.Start(ctx, "test", "early-signal", []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	first, err := c.Signal(ctx, "test", "early-signal", "go", []byte(`"first"`), "first")
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := c.Signal(ctx, "test", "early-signal", "go", []byte(`"first"`), "first")
	if err != nil || duplicate != first {
		t.Fatalf("duplicate seq=%d first=%d err=%v", duplicate, first, err)
	}
	if _, err := c.Signal(ctx, "test", "early-signal", "go", []byte(`"different"`), "first"); !errors.Is(err, client.ErrSignalMismatch) {
		t.Fatalf("mismatched retry: %v", err)
	}
	if _, err := c.Signal(ctx, "test", "early-signal", "go", []byte(`"second"`), "second"); err != nil {
		t.Fatal(err)
	}
	if result, err := history.CheckSignals(recorder.Snapshot(), time.Second); err != nil || result != porcupine.Ok {
		t.Fatalf("signal history=%s err=%v", result, err)
	}
	w, err := worker.New(ctx, all[1], "buffer-worker", map[string]worker.Handler{"test": func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		a, err := wf.AwaitSignal(c, "go")
		if err != nil {
			return nil, err
		}
		b, err := wf.AwaitSignal(c, "go")
		if err != nil {
			return nil, err
		}
		return json.Marshal([]json.RawMessage{a, b})
	}})
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() {
		done <- w.RunPartition(workerCtx, identity.Partition("test", "early-signal", provision.Partitions))
	}()
	result, err := c.Await(ctx, "test", "early-signal")
	if err != nil || string(result) != `["first","second"]` {
		t.Fatalf("result=%s err=%v", result, err)
	}
	stop()
	<-done
	entries, _, err := journal.New(all[0]).Read(ctx, "test", "early-signal")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Kind == journal.Suspended {
			t.Fatal("buffered signal caused suspension")
		}
	}
	if _, err := integrity.Check(ctx, all[0]); err != nil {
		t.Fatalf("integrity: %v", err)
	}
}

func TestConcurrentSignalQueueHistory(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	const writers, perWriter = 100, 100
	if _, err := client.New(all[0]).Start(ctx, "test", "signal-history", []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	recorder := &history.Recorder{}
	type published struct {
		sequence uint64
		hash     string
	}
	results := make([]published, writers*perWriter)
	writerErrors := make(chan error, writers)
	var wg sync.WaitGroup
	for writer := 0; writer < writers; writer++ {
		wg.Add(1)
		go func(writer int) {
			defer wg.Done()
			c := client.NewObserved(all[writer%len(all)], recorder)
			for n := 0; n < perWriter; n++ {
				index := writer*perWriter + n
				payload := []byte(fmt.Sprintf(`{"writer":%d,"number":%d}`, writer, n))
				seq, err := c.Signal(ctx, "test", "signal-history", "go", payload, fmt.Sprintf("writer-%d-number-%d", writer, n))
				if err != nil {
					writerErrors <- fmt.Errorf("writer %d signal %d: %w", writer, n, err)
					return
				}
				digest := sha256.Sum256(payload)
				results[index] = published{seq, hex.EncodeToString(digest[:])}
			}
			key := fmt.Sprintf("writer-%d-number-0", writer)
			firstPayload := []byte(fmt.Sprintf(`{"writer":%d,"number":0}`, writer))
			duplicate, err := c.Signal(ctx, "test", "signal-history", "go", firstPayload, key)
			if err != nil || duplicate != results[writer*perWriter].sequence {
				writerErrors <- fmt.Errorf("writer %d duplicate sequence=%d: %v", writer, duplicate, err)
				return
			}
			if _, err := c.Signal(ctx, "test", "signal-history", "go", []byte(`"changed"`), key); !errors.Is(err, client.ErrSignalMismatch) {
				writerErrors <- fmt.Errorf("writer %d mismatched retry: %v", writer, err)
			}
		}(writer)
	}
	wg.Wait()
	close(writerErrors)
	for err := range writerErrors {
		t.Error(err)
	}
	if t.Failed() {
		return
	}
	if result, err := history.CheckSignals(recorder.Snapshot(), time.Minute); err != nil || result != porcupine.Ok {
		t.Fatalf("concurrent signal history=%s err=%v", result, err)
	}
	stream, err := all[0].Stream(ctx, "WF_SIG")
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != writers*perWriter {
		t.Fatalf("signal stream messages=%d, want %d", info.State.Msgs, writers*perWriter)
	}
	seen := make(map[uint64]bool, len(results))
	for _, item := range results {
		if item.sequence == 0 || seen[item.sequence] {
			t.Fatalf("duplicate or zero signal sequence %d", item.sequence)
		}
		seen[item.sequence] = true
		message, err := stream.GetMsg(ctx, item.sequence)
		if err != nil {
			t.Fatal(err)
		}
		if message.Subject != "wf.sig.test.signal-history.go" || message.Header.Get("Wf-Input-SHA256") != item.hash {
			t.Fatalf("stream sequence %d subject=%s hash=%s", item.sequence, message.Subject, message.Header.Get("Wf-Input-SHA256"))
		}
	}
}

func TestSignalReconcilerRepairsMissingWakeup(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c := client.New(all[0])
	if _, err := c.Start(ctx, "test", "lost-wakeup", []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	w, err := worker.New(ctx, all[1], "lost-wakeup-worker", map[string]worker.Handler{"test": func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		value, err := wf.AwaitSignal(c, "go")
		if err != nil {
			return nil, err
		}
		return json.RawMessage(value), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() {
		done <- w.RunPartition(workerCtx, identity.Partition("test", "lost-wakeup", provision.Partitions))
	}()
	j := journal.New(all[2])
	for {
		entries, _, err := j.Read(ctx, "test", "lost-wakeup")
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) > 0 && entries[len(entries)-1].Kind == journal.Suspended {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("workflow did not suspend")
		}
		time.Sleep(30 * time.Millisecond)
	}
	// Emulate a crash after WF_SIG acknowledges the signal, before WF_RUN.
	if _, err := all[0].Publish(ctx, "wf.sig.test.lost-wakeup.go", []byte(`42`)); err != nil {
		t.Fatal(err)
	}
	result, err := reconcile.NewSignalScan(all[2]).Scan(ctx, 0, 10, false)
	if err != nil || result.Reenqueued != 1 {
		t.Fatalf("scan=%+v err=%v", result, err)
	}
	value, err := c.Await(ctx, "test", "lost-wakeup")
	if err != nil || string(value) != "42" {
		t.Fatalf("result=%s err=%v", value, err)
	}
	stop()
	<-done
	if _, err := integrity.Check(ctx, all[0]); err != nil {
		t.Fatalf("integrity: %v", err)
	}
}

func TestLargeSignalFromObjectStore(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	c := client.New(all[0])
	if _, err := c.Start(ctx, "test", "large-signal", []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	payload := []byte(strings.Repeat("z", 1024*1024))
	if _, err := c.Signal(ctx, "test", "large-signal", "go", payload, "large"); err != nil {
		t.Fatal(err)
	}
	w, err := worker.New(ctx, all[1], "large-signal-worker", map[string]worker.Handler{"test": func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		got, err := wf.AwaitSignal(c, "go")
		if err != nil {
			return nil, err
		}
		if len(got) != len(payload) {
			return nil, fmt.Errorf("signal bytes=%d", len(got))
		}
		return json.RawMessage(`true`), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() {
		done <- w.RunPartition(workerCtx, identity.Partition("test", "large-signal", provision.Partitions))
	}()
	value, err := c.Await(ctx, "test", "large-signal")
	if err != nil || string(value) != "true" {
		t.Fatalf("result=%s err=%v", value, err)
	}
	stop()
	<-done
}
