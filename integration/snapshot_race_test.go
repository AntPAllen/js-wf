package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/identity"
	"js-wf/journal"

	"github.com/nats-io/nats.go/jetstream"
)

type snapshotRevisionBarrier struct {
	arrivals chan uint64
	release  chan struct{}
}

type gatedSnapshotPort struct {
	journal.SnapshotWritePort
	barrier *snapshotRevisionBarrier
}

func (p gatedSnapshotPort) GetManifestRevision(ctx context.Context, key string) (journal.SnapshotManifestValue, error) {
	value, err := p.SnapshotWritePort.GetManifestRevision(ctx, key)
	if err != nil {
		return value, err
	}
	select {
	case p.barrier.arrivals <- value.Revision:
	case <-ctx.Done():
		return journal.SnapshotManifestValue{}, ctx.Err()
	}
	select {
	case <-p.barrier.release:
		return value, nil
	case <-ctx.Done():
		return journal.SnapshotManifestValue{}, ctx.Err()
	}
}

// Both writers read the same manifest revision before either is released.
// Distinct keep lengths make an unsafe stale purge visible as a journal gap.
func TestConcurrentSnapshotCompactorsAgainstRealCluster(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	const typ = "snapshot-compactor-contract"
	state, err := all[0].KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	stream, err := all[0].Stream(ctx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	for round := 0; round < 10; round++ {
		id := fmt.Sprintf("race-%02d", round)
		base := journal.New(all[0])
		var records []journal.Record
		add := func(kind journal.Kind) {
			t.Helper()
			entry := journal.Entry{Kind: kind, Index: uint64(len(records)), Epoch: 1, WorkerID: "compactor"}
			var expected uint64
			if len(records) > 0 {
				expected = records[len(records)-1].Sequence
			}
			sequence, err := base.Append(ctx, typ, id, entry, expected)
			if err != nil {
				t.Fatalf("round %d append %d: %v", round, entry.Index, err)
			}
			records = append(records, journal.Record{Entry: entry, Sequence: sequence})
		}
		add(journal.Started)
		for i := 0; i < 10; i++ {
			add(journal.StepRequested)
			add(journal.StepCompleted)
		}
		if _, err := base.SnapshotPrefix(ctx, typ, id, 6); err != nil {
			t.Fatalf("round %d initial snapshot: %v", round, err)
		}
		for i := 0; i < 2; i++ {
			add(journal.StepRequested)
			add(journal.StepCompleted)
		}
		initialManifest, err := state.Get(ctx, "snap."+identity.Key(typ, id))
		if err != nil {
			t.Fatalf("round %d initial manifest: %v", round, err)
		}
		initialRevision := initialManifest.Revision()
		barrier := &snapshotRevisionBarrier{arrivals: make(chan uint64, 2), release: make(chan struct{})}
		type outcome struct {
			snapshot journal.Snapshot
			err      error
		}
		outcomes := make(chan outcome, 2)
		for actor, keep := range []int{4, 2} {
			actor, keep := actor, keep
			go func() {
				port := gatedSnapshotPort{SnapshotWritePort: journal.NewSnapshotPort(all[actor+1]), barrier: barrier}
				snap, err := journal.NewWithJetStreamSnapshotPort(all[actor+1], port).SnapshotPrefix(ctx, typ, id, keep)
				outcomes <- outcome{snap, err}
			}()
		}
		for i := 0; i < 2; i++ {
			select {
			case revision := <-barrier.arrivals:
				if revision != initialRevision {
					t.Fatalf("round %d read manifest revision %d", round, revision)
				}
			case <-ctx.Done():
				t.Fatalf("round %d waiting for both manifest reads: %v", round, ctx.Err())
			}
		}
		close(barrier.release)
		var winner journal.Snapshot
		var won, stale int
		for i := 0; i < 2; i++ {
			select {
			case result := <-outcomes:
				switch {
				case result.err == nil:
					won++
					winner = result.snapshot
				case errors.Is(result.err, journal.ErrSnapshotStale):
					stale++
				default:
					t.Fatalf("round %d compactor: %v", round, result.err)
				}
			case <-ctx.Done():
				t.Fatalf("round %d waiting for compactor results: %v", round, ctx.Err())
			}
		}
		if won != 1 || stale != 1 {
			t.Fatalf("round %d outcomes: won=%d stale=%d", round, won, stale)
		}
		manifest, err := state.Get(ctx, "snap."+identity.Key(typ, id))
		if err != nil {
			t.Fatalf("round %d manifest: %v", round, err)
		}
		if manifest.Revision() != initialRevision+1 {
			t.Fatalf("round %d manifest revision: %d", round, manifest.Revision())
		}
		var published journal.Snapshot
		if err := json.Unmarshal(manifest.Value(), &published); err != nil || published != winner {
			t.Fatalf("round %d manifest=%+v winner=%+v err=%v", round, published, winner, err)
		}
		read, tail, err := journal.New(all[0]).Read(ctx, typ, id)
		if err != nil || !reflect.DeepEqual(read, records) || tail != records[len(records)-1].Sequence {
			t.Fatalf("round %d reconstructed %d/%d tail=%d err=%v", round, len(read), len(records), tail, err)
		}
		for _, record := range records {
			_, err := stream.GetMsg(ctx, record.Sequence)
			if record.Sequence <= winner.LastSeq && !errors.Is(err, jetstream.ErrMsgNotFound) || record.Sequence > winner.LastSeq && err != nil {
				t.Fatalf("round %d retained sequence %d cutoff=%d: %v", round, record.Sequence, winner.LastSeq, err)
			}
		}
	}
}

type snapshotReadHookJS struct {
	jetstream.JetStream
	once  sync.Once
	fired atomic.Bool
	hook  func(context.Context) error
	err   error
}

func (h *snapshotReadHookJS) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	stream, err := h.JetStream.Stream(ctx, name)
	if err != nil || name != "WF_JRN" {
		return stream, err
	}
	return &snapshotReadHookStream{Stream: stream, parent: h}, nil
}

type snapshotReadHookStream struct {
	jetstream.Stream
	parent *snapshotReadHookJS
}

func (s *snapshotReadHookStream) GetMsg(ctx context.Context, seq uint64, opts ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
	s.parent.once.Do(func() {
		s.parent.err = s.parent.hook(ctx)
		s.parent.fired.Store(true)
	})
	if s.parent.err != nil {
		return nil, s.parent.err
	}
	return s.Stream.GetMsg(ctx, seq, opts...)
}

func TestReadRetriesSnapshotMovedDuringLiveScan(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const typ, id = "test", "snapshot-read-race"
	store := journal.New(all[0])
	var tail uint64
	for index := uint64(0); index < 120; index++ {
		kind := journal.StepCompleted
		if index == 0 {
			kind = journal.Started
		}
		var err error
		tail, err = store.Append(ctx, typ, id, journal.Entry{Epoch: 1, Index: index, Kind: kind}, tail)
		if err != nil {
			t.Fatal(err)
		}
	}
	first, err := store.SnapshotPrefix(ctx, typ, id, 16)
	if err != nil {
		t.Fatal(err)
	}
	for index := uint64(120); index < 200; index++ {
		tail, err = store.Append(ctx, typ, id, journal.Entry{Epoch: 1, Index: index, Kind: journal.StepCompleted}, tail)
		if err != nil {
			t.Fatal(err)
		}
	}
	hooked := &snapshotReadHookJS{JetStream: all[1]}
	hooked.hook = func(ctx context.Context) error {
		second, err := store.SnapshotPrefix(ctx, typ, id, 16)
		if err != nil {
			return err
		}
		if second.LastSeq <= first.LastSeq {
			return journal.ErrGap
		}
		return nil
	}
	records, gotTail, err := journal.New(hooked).Read(ctx, typ, id)
	if err != nil || !hooked.fired.Load() || len(records) != 200 || gotTail != tail {
		t.Fatalf("read across compaction: entries=%d tail=%d want=%d fired=%v err=%v", len(records), gotTail, tail, hooked.fired.Load(), err)
	}
	for index, record := range records {
		if record.Index != uint64(index) {
			t.Fatalf("entry %d has index %d", index, record.Index)
		}
	}
}
