package integration_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/journal"

	"github.com/nats-io/nats.go/jetstream"
)

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
