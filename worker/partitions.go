package worker

import (
	"context"
	"fmt"
	"strings"
	"time"

	"js-wf/assignment"

	"js-wf/provision"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// StaticPartitions divides all dispatch partitions among a fixed set of
// workers. Every index receives a disjoint, nonempty set when count <= 64.
func StaticPartitions(index, count int) ([]uint32, error) {
	if count < 1 || count > int(provision.Partitions) || index < 0 || index >= count {
		return nil, fmt.Errorf("invalid static worker assignment %d of %d", index, count)
	}
	var partitions []uint32
	for partition := uint32(index); partition < provision.Partitions; partition += uint32(count) {
		partitions = append(partitions, partition)
	}
	return partitions, nil
}

// RunAssigned runs this worker's static share of all 64 partitions. A later
// deployment can change the worker count after stopping the previous owners;
// overlapping owners remain fenced by per-invocation leases and journal CAS.
func (w *Worker) RunAssigned(ctx context.Context, index, count int) error {
	partitions, err := StaticPartitions(index, count)
	if err != nil {
		return err
	}
	return w.RunPartitions(ctx, partitions)
}

// RunPartitions processes each supplied partition concurrently until the
// context is canceled or one partition loop returns an error.
func (w *Worker) RunPartitions(ctx context.Context, partitions []uint32) error {
	if len(partitions) == 0 {
		return fmt.Errorf("no dispatch partitions assigned")
	}
	seen := map[uint32]bool{}
	for _, partition := range partitions {
		if partition >= provision.Partitions || seen[partition] {
			return fmt.Errorf("invalid or duplicate dispatch partition %d", partition)
		}
		seen[partition] = true
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, len(partitions))
	for _, partition := range partitions {
		go func(partition uint32) { results <- w.RunPartition(runCtx, partition) }(partition)
	}
	var firstErr error
	for range partitions {
		if err := <-results; err != nil && firstErr == nil {
			firstErr = err
			cancel()
		}
	}
	return firstErr
}

type partitionLoop struct {
	cancel   context.CancelFunc
	stopping bool
}

type partitionResult struct {
	partition uint32
	err       error
}

// RunKVAssignments follows WF_ASSIGN and runs precisely the partitions
// assigned to this worker ID. Each assignment change stops the previous loop;
// invocation leases and journal CAS fence any handoff that overlaps in time.
func (w *Worker) RunKVAssignments(ctx context.Context) error {
	assignments, err := assignment.New(ctx, w.js)
	if err != nil {
		return err
	}
	connection := w.js.Conn()
	statuses := connection.StatusChanged(nats.CONNECTED, nats.RECONNECTING, nats.CLOSED)
	defer connection.RemoveStatusListener(statuses)
	var watcher jetstream.KeyWatcher
	var updates <-chan jetstream.KeyValueEntry
	defer func() {
		if watcher != nil {
			_ = watcher.Stop()
		}
	}()
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	desired := make(map[uint32]bool)
	var revisions [provision.Partitions]uint64
	active := make(map[uint32]*partitionLoop)
	results := make(chan partitionResult, provision.Partitions)
	start := func(partition uint32) {
		loopCtx, stop := context.WithCancel(runCtx)
		active[partition] = &partitionLoop{cancel: stop}
		go func() {
			results <- partitionResult{partition: partition, err: w.RunPartition(loopCtx, partition)}
		}()
	}
	reset := func() {
		if watcher != nil {
			_ = watcher.Stop()
			watcher = nil
		}
		updates = nil
		desired = make(map[uint32]bool)
		revisions = [provision.Partitions]uint64{}
		for _, loop := range active {
			if !loop.stopping {
				loop.stopping = true
				loop.cancel()
			}
		}
	}
	watch := func() {
		if watcher != nil || !connection.IsConnected() {
			return
		}
		var watchErr error
		watcher, watchErr = assignments.WatchAll(ctx)
		if watchErr == nil {
			updates = watcher.Updates()
		}
	}
	apply := func(partition uint32, owner string, revision uint64) {
		if revision == 0 && revisions[partition] != 0 {
			return
		}
		if revision != 0 {
			if revision <= revisions[partition] {
				return
			}
			revisions[partition] = revision
		}
		owned := owner == w.ID
		desired[partition] = owned
		if loop, running := active[partition]; running {
			if !owned && !loop.stopping {
				loop.stopping = true
				loop.cancel()
			}
		} else if owned {
			start(partition)
		}
	}
	reconcile := func() error {
		checkCtx, done := context.WithTimeout(ctx, 4*time.Second)
		defer done()
		for partition := uint32(0); partition < provision.Partitions; partition++ {
			owner, revision, err := assignments.Get(checkCtx, partition)
			if err != nil {
				return err
			}
			if revision == 0 && revisions[partition] != 0 {
				owner, revision, err = assignments.GetLatest(checkCtx, partition)
				if err != nil {
					return err
				}
			}
			apply(partition, owner, revision)
		}
		return nil
	}
	watch()
	retry := time.NewTicker(time.Second)
	defer retry.Stop()
	defer func() {
		for _, loop := range active {
			loop.cancel()
		}
		for range active {
			<-results
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return nil
		case status, ok := <-statuses:
			if !ok || status == nats.CLOSED {
				return fmt.Errorf("assignment connection closed")
			}
			reset()
			if status == nats.CONNECTED {
				watch()
			}
		case <-retry.C:
			if !connection.IsConnected() {
				if watcher != nil {
					reset()
				}
			} else {
				watch()
				if err := reconcile(); err != nil && ctx.Err() == nil {
					reset()
				}
			}
		case entry, ok := <-updates:
			if !ok {
				reset()
				watch()
				continue
			}
			if entry == nil { // End of initial snapshot.
				continue
			}
			partition, err := assignment.ParseKey(entry.Key())
			if err != nil {
				return err
			}
			owner := ""
			if entry.Operation() == jetstream.KeyValuePut {
				owner = string(entry.Value())
				if owner == "" || strings.TrimSpace(owner) != owner {
					return fmt.Errorf("invalid owner for partition %d", partition)
				}
			}
			apply(partition, owner, entry.Revision())
		case result := <-results:
			loop := active[result.partition]
			if loop == nil {
				return fmt.Errorf("unexpected partition %d result", result.partition)
			}
			delete(active, result.partition)
			if result.err != nil && !loop.stopping && ctx.Err() == nil {
				return fmt.Errorf("partition %d: %w", result.partition, result.err)
			}
			if desired[result.partition] && ctx.Err() == nil {
				start(result.partition)
			}
		}
	}
}
