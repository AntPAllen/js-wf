package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"js-wf/identity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/retention"
	"js-wf/wf"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type blockedSnapshotPutPort struct {
	journal.SnapshotWritePort
	started chan struct{}
	release chan struct{}
}

func (p blockedSnapshotPutPort) PutObject(ctx context.Context, name string, data []byte) error {
	close(p.started)
	select {
	case <-p.release:
		return p.SnapshotWritePort.PutObject(ctx, name, data)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestSnapshotPurgeLeaseBoundaryAgainstRealCluster(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const typ, id = "snapshot-purge-contract", "one"
	key := identity.Key(typ, id)
	objects, err := all[0].ObjectStore(ctx, "WF_BLOB")
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string]string{"input-contract": "input", "terminal-result-contract": "result", "input-orphan": "orphan"} {
		if _, err := objects.PutBytes(ctx, name, []byte(data)); err != nil {
			t.Fatal(err)
		}
	}
	header := nats.Header{}
	header.Set("Wf-Input-Ref", "input-contract")
	inv, err := all[0].PublishMsg(ctx, &nats.Msg{Subject: identity.InvocationSubject(typ, id), Header: header, Data: []byte(`null`)})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := json.Marshal(wf.Outcome{InvSeq: inv.Sequence, ResultRef: "terminal-result-contract"})
	if err != nil {
		t.Fatal(err)
	}
	store := journal.New(all[0])
	first, err := store.Append(ctx, typ, id, journal.Entry{Index: 0, Epoch: 1, Kind: journal.Started}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, typ, id, journal.Entry{Index: 1, Epoch: 1, Kind: journal.Completed, Payload: terminal}, first); err != nil {
		t.Fatal(err)
	}
	state, err := all[0].KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.Create(ctx, key, terminal); err != nil {
		t.Fatal(err)
	}
	leasing, err := lease.New(ctx, all[1])
	if err != nil {
		t.Fatal(err)
	}
	workerLease, err := leasing.Acquire(ctx, typ, id, "snapshot-worker")
	if err != nil {
		t.Fatal(err)
	}
	port := blockedSnapshotPutPort{SnapshotWritePort: journal.NewSnapshotPort(all[1]), started: make(chan struct{}), release: make(chan struct{})}
	compacted := make(chan error, 1)
	go func() {
		_, err := journal.NewWithJetStreamSnapshotPort(all[1], port).SnapshotPrefix(ctx, typ, id, 1)
		compacted <- err
	}()
	select {
	case <-port.started:
	case <-ctx.Done():
		t.Fatalf("waiting for snapshot object upload: %v", ctx.Err())
	}
	if err := retention.Purge(ctx, all[2], typ, id, time.Hour); !errors.Is(err, retention.ErrActive) {
		t.Fatalf("purge during snapshot upload: %v", err)
	}
	if _, err := state.Get(ctx, "purging."+key); !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatalf("purge marker written while worker held lease: %v", err)
	}
	close(port.release)
	select {
	case err := <-compacted:
		if err != nil {
			t.Fatalf("snapshot after fenced purge: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("waiting for snapshot: %v", ctx.Err())
	}
	if records, _, err := journal.New(all[2]).Read(ctx, typ, id); err != nil || len(records) != 2 || records[1].Kind != journal.Completed {
		t.Fatalf("journal after fenced purge: records=%v err=%v", records, err)
	}
	if err := workerLease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := retention.Purge(ctx, all[2], typ, id, time.Hour); err != nil {
		t.Fatalf("purge after worker release: %v", err)
	}
	if err := retention.Purge(ctx, all[2], typ, id, time.Hour); err != nil {
		t.Fatalf("idempotent purge: %v", err)
	}
	if _, err := state.Get(ctx, "snap."+key); !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatalf("snapshot manifest retained after purge: %v", err)
	}
	swept, err := retention.SweepBlobsQuiescent(ctx, all[0], 0, time.Now().Add(time.Minute))
	if err != nil || swept.Referenced != 0 || swept.Deleted != 4 {
		t.Fatalf("blob sweep after purge: %+v err=%v", swept, err)
	}
}

func TestSnapshotKeepsConcurrentAppendBeforePurgeAgainstRealCluster(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const typ, id = "snapshot-append-contract", "one"
	key := identity.Key(typ, id)
	objects, err := all[0].ObjectStore(ctx, "WF_BLOB")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"input-snapshot-append", "step-result-snapshot-append", "terminal-result-snapshot-append", "input-orphan-snapshot-append"} {
		if _, err := objects.PutBytes(ctx, name, []byte(name)); err != nil {
			t.Fatal(err)
		}
	}
	header := nats.Header{}
	header.Set("Wf-Input-Ref", "input-snapshot-append")
	inv, err := all[0].PublishMsg(ctx, &nats.Msg{Subject: identity.InvocationSubject(typ, id), Header: header, Data: []byte(`null`)})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := json.Marshal(wf.Outcome{InvSeq: inv.Sequence, ResultRef: "terminal-result-snapshot-append"})
	if err != nil {
		t.Fatal(err)
	}
	store := journal.New(all[0])
	var tail uint64
	for index, entry := range []journal.Entry{
		{Kind: journal.Started},
		{Kind: journal.StepRequested, Payload: json.RawMessage(`{"kind":"run","name":"work"}`)},
		{Kind: journal.StepCompleted, Payload: json.RawMessage(`{"result_ref":"step-result-snapshot-append"}`)},
	} {
		entry.Index, entry.Epoch = uint64(index), 1
		tail, err = store.Append(ctx, typ, id, entry, tail)
		if err != nil {
			t.Fatal(err)
		}
	}
	leasing, err := lease.New(ctx, all[1])
	if err != nil {
		t.Fatal(err)
	}
	workerLease, err := leasing.Acquire(ctx, typ, id, "snapshot-writer")
	if err != nil {
		t.Fatal(err)
	}
	port := blockedSnapshotPutPort{SnapshotWritePort: journal.NewSnapshotPort(all[1]), started: make(chan struct{}), release: make(chan struct{})}
	type compactResult struct {
		snapshot journal.Snapshot
		err      error
	}
	compacted := make(chan compactResult, 1)
	go func() {
		snapshot, err := journal.NewWithJetStreamSnapshotPort(all[1], port).SnapshotPrefix(ctx, typ, id, 1)
		compacted <- compactResult{snapshot, err}
	}()
	select {
	case <-port.started:
	case <-ctx.Done():
		t.Fatalf("waiting for snapshot object upload: %v", ctx.Err())
	}
	finalSeq, err := store.Append(ctx, typ, id, journal.Entry{Index: 3, Epoch: 1, Kind: journal.Completed, Payload: terminal}, tail)
	if err != nil {
		t.Fatalf("terminal append during snapshot upload: %v", err)
	}
	state, err := all[0].KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.Create(ctx, key, terminal); err != nil {
		t.Fatal(err)
	}
	if err := retention.Purge(ctx, all[2], typ, id, time.Hour); !errors.Is(err, retention.ErrActive) {
		t.Fatalf("purge while writer holds lease: %v", err)
	}
	close(port.release)
	var snap journal.Snapshot
	select {
	case result := <-compacted:
		if result.err != nil {
			t.Fatalf("snapshot after terminal append: %v", result.err)
		}
		snap = result.snapshot
	case <-ctx.Done():
		t.Fatalf("waiting for snapshot: %v", ctx.Err())
	}
	if snap.LastIndex != 1 || snap.LastSeq >= finalSeq {
		t.Fatalf("snapshot covered concurrent terminal append: %+v final sequence=%d", snap, finalSeq)
	}
	records, readTail, err := journal.New(all[2]).Read(ctx, typ, id)
	if err != nil || len(records) != 4 || readTail != finalSeq || records[3].Kind != journal.Completed {
		t.Fatalf("read after concurrent append: records=%v tail=%d err=%v", records, readTail, err)
	}
	stream, err := all[0].Stream(ctx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.GetMsg(ctx, finalSeq); err != nil {
		t.Fatalf("terminal journal entry purged by snapshot: %v", err)
	}
	if err := workerLease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := retention.Purge(ctx, all[2], typ, id, time.Hour); err != nil {
		t.Fatalf("purge after worker release: %v", err)
	}
	swept, err := retention.SweepBlobsQuiescent(ctx, all[0], 0, time.Now().Add(time.Minute))
	if err != nil || swept.Referenced != 0 || swept.Deleted != 5 {
		t.Fatalf("blob sweep after purge: %+v err=%v", swept, err)
	}
}
