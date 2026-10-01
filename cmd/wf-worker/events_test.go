package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"js-wf/reconcile"
	"js-wf/worker"
)

type memoryEventFile struct {
	bytes.Buffer
	writeErr, syncErr, closeErr error
}

func (f *memoryEventFile) Write(data []byte) (int, error) {
	if f.writeErr != nil {
		return 0, f.writeErr
	}
	return f.Buffer.Write(data)
}
func (f *memoryEventFile) Sync() error  { return f.syncErr }
func (f *memoryEventFile) Close() error { return f.closeErr }

type heldEventFile struct {
	memoryEventFile
	entered, release chan struct{}
}

func (f *heldEventFile) Write(data []byte) (int, error) {
	select {
	case f.entered <- struct{}{}:
	default:
	}
	<-f.release
	return f.memoryEventFile.Write(data)
}

func TestWorkerEventLogPreservesTypedRecordsAndAppendSessions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	for i := 0; i < 2; i++ {
		log, err := openWorkerEventLog(path, "event-worker")
		if err != nil {
			t.Fatal(err)
		}
		log.fencing(worker.FencingEvent{At: time.Now().UTC(), Worker: "event-worker", Type: "test", ID: "id", RunSequence: 10, Delivery: 2, Epoch: 4, Reason: "lease_heartbeat_lost", Error: "lease was lost"})
		log.repair(reconcile.RepairEvent{At: time.Now().UTC(), Kind: "start", Type: "test", ID: "id", SourceSequence: 1, InvocationSequence: 1, Outcome: "uncertain", Error: "acknowledgement lost"})
		if err = log.Close(); err != nil {
			t.Fatal(err)
		}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimSpace(data), []byte("\n"))
	if len(lines) != 4 {
		t.Fatalf("records=%d", len(lines))
	}
	var previous time.Time
	for i, line := range lines {
		var record struct {
			Version   int
			Worker    string `json:"worker_id"`
			PID       int
			StartedAt time.Time `json:"process_started_at"`
			Sequence  uint64
			Kind      string
			Event     json.RawMessage
		}
		if err = json.Unmarshal(line, &record); err != nil {
			t.Fatal(err)
		}
		if record.Version != 1 || record.Worker != "event-worker" || record.PID != os.Getpid() || record.StartedAt.IsZero() || record.Sequence != uint64(i%2+1) {
			t.Fatalf("bad event envelope=%s", line)
		}
		if i%2 == 0 {
			if i > 0 && record.StartedAt.Equal(previous) {
				t.Fatal("append session identity was reused")
			}
			previous = record.StartedAt
			var event worker.FencingEvent
			_ = json.Unmarshal(record.Event, &event)
			if record.Kind != "fencing" || event.Epoch != 4 || event.RunSequence != 10 || event.Worker != record.Worker {
				t.Fatalf("fencing identity erased=%s", line)
			}
		} else {
			var event reconcile.RepairEvent
			_ = json.Unmarshal(record.Event, &event)
			if record.Kind != "repair" || event.Outcome != "uncertain" || event.Error != "acknowledgement lost" || event.InvocationSequence != 1 {
				t.Fatalf("repair uncertainty erased=%s", line)
			}
		}
	}
}

func TestWorkerEventLogCallbacksDoNotWaitForIOAndOverflowFails(t *testing.T) {
	file := &heldEventFile{entered: make(chan struct{}, 1), release: make(chan struct{})}
	log := newWorkerEventLog(file, "held-worker", 1)
	log.repair(reconcile.RepairEvent{})
	select {
	case <-file.entered:
	case <-time.After(time.Second):
		t.Fatal("writer did not enter hold")
	}
	returned := make(chan struct{})
	go func() { log.repair(reconcile.RepairEvent{}); log.repair(reconcile.RepairEvent{}); close(returned) }()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("callback blocked on writer")
	}
	select {
	case err := <-log.errors:
		if !strings.Contains(err.Error(), "queue is full") {
			t.Fatal(err)
		}
	default:
		t.Fatal("overflow was silently lost")
	}
	close(file.release)
	if err := log.Close(); err == nil {
		t.Fatal("incomplete evidence did not fail close")
	}
}

func TestWorkerEventLogWriteSyncAndCloseFailuresAreFatal(t *testing.T) {
	for _, operation := range []string{"write", "sync", "close"} {
		t.Run(operation, func(t *testing.T) {
			expected := fmt.Errorf("injected %s failure", operation)
			file := &memoryEventFile{}
			switch operation {
			case "write":
				file.writeErr = expected
			case "sync":
				file.syncErr = expected
			case "close":
				file.closeErr = expected
			}
			log := newWorkerEventLog(file, "failed-worker", 2)
			log.repair(reconcile.RepairEvent{})
			if err := log.Close(); !errors.Is(err, expected) {
				t.Fatalf("failure was masked: %v", err)
			}
			select {
			case err := <-log.errors:
				if !errors.Is(err, expected) {
					t.Fatal(err)
				}
			default:
				t.Fatal("runner error channel was not notified")
			}
		})
	}
}

func TestWorkerEventLogRefusesIncompleteAppendTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "partial.jsonl")
	if err := os.WriteFile(path, []byte(`{"partial":`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := openWorkerEventLog(path, "worker"); err == nil {
		t.Fatal("partial tail was silently extended")
	}
	file := &memoryEventFile{writeErr: io.ErrShortWrite}
	log := newWorkerEventLog(file, "short-write", 1)
	log.repair(reconcile.RepairEvent{})
	if err := log.Close(); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short write accepted: %v", err)
	}
}
