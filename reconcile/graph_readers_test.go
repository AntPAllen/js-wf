package reconcile

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/internal/graphpublication"
	"js-wf/lease"
)

type readerLoopModel struct {
	cursor                          graphpublication.ReaderSweepCursor
	revision                        uint64
	saved                           []graphpublication.ReaderSweepCursor
	loads, renews, releases, begins int
	scans                           []graphpublication.ReaderSweepCursor
	lostSave                        bool
	loseRenew                       int
	cancel                          context.CancelFunc
	stopAfter                       int
	partial                         bool
}

type readerCheckpointKV struct {
	jetstream.KeyValue
	data     []byte
	revision uint64
	lost     bool
}
type readerCheckpointEntry struct {
	jetstream.KeyValueEntry
	data     []byte
	revision uint64
}

func (e readerCheckpointEntry) Value() []byte    { return e.data }
func (e readerCheckpointEntry) Revision() uint64 { return e.revision }
func (m *readerCheckpointKV) Get(context.Context, string) (jetstream.KeyValueEntry, error) {
	if m.revision == 0 {
		return nil, jetstream.ErrKeyNotFound
	}
	return readerCheckpointEntry{data: m.data, revision: m.revision}, nil
}
func (m *readerCheckpointKV) Create(_ context.Context, _ string, data []byte, _ ...jetstream.KVCreateOpt) (uint64, error) {
	if m.revision != 0 {
		return 0, jetstream.ErrKeyExists
	}
	return m.store(data)
}
func (m *readerCheckpointKV) Update(_ context.Context, _ string, data []byte, revision uint64) (uint64, error) {
	if revision != m.revision {
		return 0, jetstream.ErrKeyRevisionMismatch
	}
	return m.store(data)
}
func (m *readerCheckpointKV) store(data []byte) (uint64, error) {
	m.data = append([]byte(nil), data...)
	m.revision++
	if m.lost {
		m.lost = false
		return 0, nats.ErrTimeout
	}
	return m.revision, nil
}
func TestReaderExpiryNativeCheckpointBoundary(t *testing.T) {
	ctx := context.Background()
	m := &readerCheckpointKV{}
	p := nativeReaderExpiryPort{&jetStreamLoopPort{state: m}}
	c, r, err := p.LoadReaderCursor(ctx)
	if err != nil || r != 0 || c != (graphpublication.ReaderSweepCursor{}) {
		t.Fatal(c, r, err)
	}
	want := graphpublication.ReaderSweepCursor{3, 7}
	m.lost = true
	if _, err = p.SaveReaderCursor(ctx, want, 0); err != nats.ErrTimeout {
		t.Fatal(err)
	}
	c, r, err = p.LoadReaderCursor(ctx)
	if err != nil || c != want || r != 1 {
		t.Fatal(c, r, err)
	}
	if _, err = p.SaveReaderCursor(ctx, graphpublication.ReaderSweepCursor{5, 7}, 0); err != ErrCursorStale {
		t.Fatal(err)
	}
	if _, err = p.SaveReaderCursor(ctx, graphpublication.ReaderSweepCursor{}, r); err != nil {
		t.Fatal(err)
	}
	for _, data := range []string{"null", "{}", "{\"Version\":2}", "{\"Version\":1}", "{\"Version\":1,\"Cursor\":null}", "{\"Version\":1,\"Cursor\":{\"Next\":0,\"Through\":7}}"} {
		m.data = []byte(data)
		if _, _, err = p.LoadReaderCursor(ctx); err == nil {
			t.Fatal("corrupt checkpoint accepted", data)
		}
	}
}

func (m *readerLoopModel) Prepare(context.Context) error                              { return nil }
func (m *readerLoopModel) Acquire(context.Context, string, string) (LoopLease, error) { return m, nil }
func (m *readerLoopModel) LoadReaderCursor(context.Context) (graphpublication.ReaderSweepCursor, uint64, error) {
	m.loads++
	return m.cursor, m.revision, nil
}
func (m *readerLoopModel) SaveReaderCursor(_ context.Context, c graphpublication.ReaderSweepCursor, r uint64) (uint64, error) {
	if r != m.revision {
		return 0, ErrCursorStale
	}
	m.cursor = c
	m.revision++
	m.saved = append(m.saved, c)
	if m.lostSave {
		m.lostSave = false
		return 0, nats.ErrTimeout
	}
	return m.revision, nil
}
func (m *readerLoopModel) Renew(context.Context) error {
	m.renews++
	if m.renews == m.loseRenew {
		m.cancel()
		return lease.ErrLost
	}
	return nil
}
func (m *readerLoopModel) Release(context.Context) error { m.releases++; return nil }
func (m *readerLoopModel) Wait(context.Context, time.Duration) error {
	if len(m.scans) >= m.stopAfter {
		m.cancel()
	}
	return nil
}
func (m *readerLoopModel) BeginReaderSweep(context.Context) (graphpublication.ReaderSweepCursor, error) {
	m.begins++
	return graphpublication.ReaderSweepCursor{Next: 1, Through: 5}, nil
}
func (m *readerLoopModel) ExpireReaderBatch(_ context.Context, c graphpublication.ReaderSweepCursor, _ int, _ time.Time) (graphpublication.ReaderSweepResult, error) {
	m.scans = append(m.scans, c)
	c.Next += 2
	if c.Next > c.Through+1 {
		c.Next = c.Through + 1
	}
	r := graphpublication.ReaderSweepResult{Cursor: c, Inspected: 1, Complete: c.Next > c.Through}
	if m.partial {
		m.partial = false
		return r, nats.ErrTimeout
	}
	return r, nil
}
func runReaderModel(t *testing.T, m *readerLoopModel) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.cancel = cancel
	if err := RunReaderExpiryWithPort(ctx, m, m, "reader-worker", time.Second, 1, func() time.Time { return time.Unix(1000, 0) }); err != nil {
		t.Fatal(err)
	}
}

func TestReaderExpiryLoopRestartAndLostSave(t *testing.T) {
	m := &readerLoopModel{stopAfter: 1, lostSave: true}
	runReaderModel(t, m)
	if m.loads != 2 || m.begins != 1 || len(m.scans) != 1 || m.scans[0] != (graphpublication.ReaderSweepCursor{1, 5}) || m.cursor != (graphpublication.ReaderSweepCursor{3, 5}) {
		t.Fatal(m)
	}
	// New worker/process uses the checkpoint, including the original watermark.
	m.stopAfter = 3
	runReaderModel(t, m)
	if m.begins != 1 || m.cursor != (graphpublication.ReaderSweepCursor{}) || !reflect.DeepEqual(m.scans, []graphpublication.ReaderSweepCursor{{1, 5}, {3, 5}, {5, 5}}) {
		t.Fatal(m)
	}
	if m.releases != 3 {
		t.Fatal("lease not released", m.releases)
	}
}

func TestReaderExpiryLoopDoesNotSaveAfterLeaseLoss(t *testing.T) {
	m := &readerLoopModel{cursor: graphpublication.ReaderSweepCursor{1, 5}, revision: 1, loseRenew: 2, stopAfter: 1}
	runReaderModel(t, m)
	if len(m.scans) != 1 || len(m.saved) != 0 || m.cursor.Next != 1 || m.releases != 1 {
		t.Fatal(m)
	}
}

func TestReaderExpiryLoopCheckpointsConfirmedPartialBatch(t *testing.T) {
	m := &readerLoopModel{cursor: graphpublication.ReaderSweepCursor{1, 5}, revision: 1, partial: true, stopAfter: 2}
	runReaderModel(t, m)
	if m.loads != 2 || !reflect.DeepEqual(m.scans, []graphpublication.ReaderSweepCursor{{1, 5}, {3, 5}}) || m.cursor.Next != 5 {
		t.Fatal(m)
	}
}

func TestReaderExpiryLoopRejectsConfigurationAndCheckpoint(t *testing.T) {
	m := &readerLoopModel{cursor: graphpublication.ReaderSweepCursor{0, 5}, stopAfter: 1}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.cancel = cancel
	if err := RunReaderExpiryWithPort(ctx, m, m, "worker", time.Second, 1, time.Now); err == nil || len(m.scans) != 0 || len(m.saved) != 0 {
		t.Fatal(err, m)
	}
	if err := RunReaderExpiryWithPort(ctx, m, m, "worker", time.Second, graphpublication.MaxReaderSweepBatch+1, time.Now); err == nil {
		t.Fatal("invalid budget accepted")
	}
}
