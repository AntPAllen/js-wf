package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"js-wf/reconcile"
	"js-wf/worker"
)

type workerEventRecord struct {
	Version   int       `json:"version"`
	Worker    string    `json:"worker_id"`
	PID       int       `json:"pid"`
	StartedAt time.Time `json:"process_started_at"`
	Sequence  uint64    `json:"sequence"`
	Kind      string    `json:"kind"`
	Event     any       `json:"event"`
}

type eventFile interface {
	io.Writer
	Sync() error
	Close() error
}

// Callbacks never wait for disk I/O. A full queue or failed write is fatal to
// the runner; it cannot silently continue with incomplete diagnostic evidence.
// Graceful close drains and syncs. SIGKILL can lose the queued/unsynced tail.
type workerEventLog struct {
	worker      string
	started     time.Time
	file        eventFile
	queue       chan workerEventRecord
	done        chan struct{}
	errors      chan error
	mu          sync.RWMutex
	closed      bool
	closeOnce   sync.Once
	failureOnce sync.Once
	failure     error
	failureMu   sync.Mutex
}

func openWorkerEventLog(path, workerID string) (*workerEventLog, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return nil, fmt.Errorf("open worker events: %w", err)
	}
	info, err := f.Stat()
	if err == nil && !info.Mode().IsRegular() {
		err = fmt.Errorf("event file must be a regular file")
	}
	if err == nil && info.Size() > 0 {
		var last [1]byte
		_, err = f.ReadAt(last[:], info.Size()-1)
		if err == nil && last[0] != '\n' {
			err = fmt.Errorf("event file has an incomplete final record")
		}
	}
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("worker events: %w", err)
	}
	return newWorkerEventLog(f, workerID, 256), nil
}

func newWorkerEventLog(file eventFile, workerID string, capacity int) *workerEventLog {
	l := &workerEventLog{worker: workerID, started: time.Now().UTC(), file: file, queue: make(chan workerEventRecord, capacity), done: make(chan struct{}), errors: make(chan error, 1)}
	go l.write()
	return l
}

func (l *workerEventLog) fail(err error) {
	l.failureOnce.Do(func() {
		err = fmt.Errorf("worker event log: %w", err)
		l.failureMu.Lock()
		l.failure = err
		l.failureMu.Unlock()
		l.errors <- err
	})
}

func (l *workerEventLog) enqueue(kind string, event any) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.closed {
		l.fail(fmt.Errorf("event emitted after close"))
		return
	}
	record := workerEventRecord{Version: 1, Worker: l.worker, PID: os.Getpid(), StartedAt: l.started, Kind: kind, Event: event}
	select {
	case l.queue <- record:
	default:
		l.fail(fmt.Errorf("event queue is full"))
	}
}

func (l *workerEventLog) fencing(event worker.FencingEvent)  { l.enqueue("fencing", event) }
func (l *workerEventLog) repair(event reconcile.RepairEvent) { l.enqueue("repair", event) }

func (l *workerEventLog) write() {
	defer close(l.done)
	encoder := json.NewEncoder(l.file)
	var sequence uint64
	for record := range l.queue {
		sequence++
		record.Sequence = sequence
		if err := encoder.Encode(record); err != nil {
			l.fail(err)
			return
		}
	}
	if err := l.file.Sync(); err != nil {
		l.fail(err)
	}
}

func (l *workerEventLog) Close() error {
	l.closeOnce.Do(func() {
		l.mu.Lock()
		l.closed = true
		close(l.queue)
		l.mu.Unlock()
		<-l.done
		if err := l.file.Close(); err != nil {
			l.fail(err)
		}
	})
	l.failureMu.Lock()
	defer l.failureMu.Unlock()
	return l.failure
}
