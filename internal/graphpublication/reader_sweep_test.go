package graphpublication

import (
	"context"
	"encoding/json"
	"math"
	"testing"
	"time"
)

type readerScanModel struct {
	*memoryPort
	entries []RootCatalogEntry
	calls   int
	err     error
	force   *RootCatalogEntry
}

func (m *readerScanModel) RootCatalogHighWater(context.Context) (uint64, error) {
	if len(m.entries) == 0 {
		return 0, nil
	}
	return m.entries[len(m.entries)-1].Sequence, nil
}

func (m *readerScanModel) NextRoot(_ context.Context, next uint64) (*RootCatalogEntry, error) {
	m.calls++
	if m.force != nil {
		return m.force, nil
	}
	if m.err != nil {
		return nil, m.err
	}
	for _, entry := range m.entries {
		if entry.Sequence >= next {
			return &entry, nil
		}
	}
	return nil, nil
}

func readerScanFixture(t *testing.T) (*readerScanModel, Protocol) {
	t.Helper()
	m, p := newModel("reader-batch")
	scan := &readerScanModel{memoryPort: m, entries: []RootCatalogEntry{{1, "a"}, {3, "b"}, {5, "c"}}}
	p.Port = scan
	for _, entry := range scan.entries {
		if _, _, err := p.AcquireReader(ctx, entry.Destination, 0, epoch.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	return scan, p
}

func TestReaderSweepBatchResumeAndWatermark(t *testing.T) {
	m, p := readerScanFixture(t)
	cursor, err := p.BeginReaderSweep(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// A newly discovered root is deferred even if it arrives before the first batch.
	m.entries = append(m.entries, RootCatalogEntry{7, "later"})
	if _, _, err := p.AcquireReader(ctx, "later", 0, epoch.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	for i, next := range []uint64{2, 4, 6} {
		result, err := p.ExpireReaderBatch(ctx, cursor, 1, epoch.Add(time.Hour))
		if err != nil || result.Inspected != 1 || result.Cursor.Next != next || result.Complete != (i == 2) || m.calls != i+1 {
			t.Fatal(result, err, m.calls)
		}
		for j, destination := range []string{"a", "b", "c", "later"} {
			want := 1
			if j <= i {
				want = 0
			}
			if len(m.roots[destination].Readers) != want {
				t.Fatal(destination, m.roots[destination])
			}
		}
		// Simulate a persisted checkpoint and a reconstructed protocol.
		data, _ := json.Marshal(result.Cursor)
		if err := json.Unmarshal(data, &cursor); err != nil {
			t.Fatal(err)
		}
		p = Protocol{Port: m, NewID: p.NewID}
	}
	if m.deletes != 0 {
		t.Fatal("batch deleted objects")
	}
	cursor, err = p.BeginReaderSweep(ctx)
	if err != nil {
		t.Fatal(err)
	}
	result, err := p.ExpireReaderBatch(ctx, cursor, MaxReaderSweepBatch, epoch.Add(time.Hour))
	if err != nil || !result.Complete || len(m.roots["later"].Readers) != 0 {
		t.Fatal(result, err)
	}
}

func TestReaderSweepUncertainCheckpoint(t *testing.T) {
	for _, mode := range []string{"scan", "read", "expiry"} {
		t.Run(mode, func(t *testing.T) {
			m, p := readerScanFixture(t)
			cursor, _ := p.BeginReaderSweep(ctx)
			first, err := p.ExpireReaderBatch(ctx, cursor, 1, epoch.Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "scan":
				m.err = lostReply
			case "read":
				m.readRootHook = func(string) error { return lostReply }
			case "expiry":
				m.rootAfter = func() error { m.rootAfter = nil; return lostReply }
			}
			result, err := p.ExpireReaderBatch(ctx, first.Cursor, 2, epoch.Add(time.Hour))
			if err == nil || result.Cursor != first.Cursor || result.Inspected != 0 || result.Complete || m.deletes != 0 {
				t.Fatal(result, err)
			}
			if mode == "expiry" && len(m.roots["b"].Readers) != 0 {
				t.Fatal("lost reply did not commit")
			}
			m.err = nil
			m.readRootHook = nil
			result, err = p.ExpireReaderBatch(ctx, result.Cursor, 2, epoch.Add(time.Hour))
			if err != nil || !result.Complete || result.Inspected != 2 || m.deletes != 0 {
				t.Fatal(result, err)
			}
		})
	}
}

func TestReaderSweepRejectsInvalidBatch(t *testing.T) {
	m, p := readerScanFixture(t)
	for _, cursor := range []ReaderSweepCursor{{0, 5}, {7, 5}, {1, math.MaxUint64}} {
		if _, err := p.ExpireReaderBatch(ctx, cursor, 1, epoch); err == nil {
			t.Fatal(cursor)
		}
	}
	for _, budget := range []int{0, MaxReaderSweepBatch + 1} {
		if _, err := p.ExpireReaderBatch(ctx, ReaderSweepCursor{1, 5}, budget, epoch); err == nil {
			t.Fatal(budget)
		}
	}
	if m.calls != 0 {
		t.Fatal("invalid input scanned catalog")
	}
	for _, entry := range []RootCatalogEntry{{0, "a"}, {1, ""}, {1, string([]byte{255})}, {math.MaxUint64, "a"}} {
		m.force = &entry
		if _, err := p.ExpireReaderBatch(ctx, ReaderSweepCursor{1, 5}, 1, epoch); err == nil {
			t.Fatal(entry)
		}
	}
	p.Port = struct{ Port }{m.memoryPort}
	if _, err := p.BeginReaderSweep(ctx); err == nil {
		t.Fatal("missing scan accepted")
	}
	if _, err := p.ExpireReaderBatch(ctx, ReaderSweepCursor{1, 5}, 1, epoch); err == nil {
		t.Fatal("missing scan accepted")
	}
}

func TestReaderSweepEmptyAndCancelled(t *testing.T) {
	m, p := readerScanFixture(t)
	m.entries = nil
	cursor, err := p.BeginReaderSweep(ctx)
	if err != nil {
		t.Fatal(err)
	}
	result, err := p.ExpireReaderBatch(ctx, cursor, 1, epoch)
	if err != nil || !result.Complete || result.Inspected != 0 || m.calls != 0 {
		t.Fatal(result, err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := p.BeginReaderSweep(cancelled); err != context.Canceled {
		t.Fatal(err)
	}
	if _, err := p.ExpireReaderBatch(cancelled, cursor, 1, epoch); err != context.Canceled {
		t.Fatal(err)
	}
	m.entries = []RootCatalogEntry{{math.MaxUint64, "a"}}
	if _, err := p.BeginReaderSweep(ctx); err == nil {
		t.Fatal("overflow accepted")
	}
}
