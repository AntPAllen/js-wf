package sim

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/internal/graphpublication"
	"js-wf/reconcile"
)

// The typed checkpoint bucket shares the existing modeled lease and cadence.
// Only the storage boundary is modeled; production chooses restart/checkpoints.
type readerMaintenancePort struct{ *LoopTransport }

func (p readerMaintenancePort) LoadReaderCursor(ctx context.Context) (graphpublication.ReaderSweepCursor, uint64, error) {
	entry, err := p.cursors.Get(ctx, "scan.graph-reader-expiry")
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return graphpublication.ReaderSweepCursor{}, 0, nil
	}
	if err != nil {
		return graphpublication.ReaderSweepCursor{}, 0, readerMaintenanceError(err)
	}
	var stored struct {
		Version int
		Cursor  graphpublication.ReaderSweepCursor
	}
	if err = json.Unmarshal(entry.Value, &stored); err != nil || stored.Version != 1 {
		return stored.Cursor, 0, fmt.Errorf("invalid modeled reader checkpoint")
	}
	return stored.Cursor, entry.Revision, nil
}
func (p readerMaintenancePort) SaveReaderCursor(ctx context.Context, c graphpublication.ReaderSweepCursor, r uint64) (uint64, error) {
	p.saves++
	if p.saves == p.faultAt {
		op := "update"
		if r == 0 {
			op = "create"
		}
		if err := p.cursors.QueueFault(KVFault{Operation: op, Kind: p.fault}); err != nil {
			return 0, err
		}
	}
	data, err := json.Marshal(struct {
		Version int
		Cursor  graphpublication.ReaderSweepCursor
	}{1, c})
	if err != nil {
		return 0, err
	}
	var next uint64
	if r == 0 {
		next, err = p.cursors.Create(ctx, "scan.graph-reader-expiry", data)
	} else {
		next, err = p.cursors.Update(ctx, "scan.graph-reader-expiry", data, r)
	}
	if errors.Is(err, jetstream.ErrKeyExists) || errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
		return 0, reconcile.ErrCursorStale
	}
	return next, readerMaintenanceError(err)
}
func readerMaintenanceError(err error) error {
	if errors.Is(err, ErrTransportLost) {
		return fmt.Errorf("%w: %w", nats.ErrTimeout, err)
	}
	return err
}

type readerMaintenanceProtocol struct{ graphpublication.Protocol }

func (p readerMaintenanceProtocol) BeginReaderSweep(ctx context.Context) (graphpublication.ReaderSweepCursor, error) {
	c, e := p.Protocol.BeginReaderSweep(ctx)
	return c, readerMaintenanceError(e)
}
func (p readerMaintenanceProtocol) ExpireReaderBatch(ctx context.Context, c graphpublication.ReaderSweepCursor, b int, n time.Time) (graphpublication.ReaderSweepResult, error) {
	r, e := p.Protocol.ExpireReaderBatch(ctx, c, b, n)
	return r, readerMaintenanceError(e)
}

var readerMaintenanceModes = []string{"healthy", "cursor_drop", "cursor_ack", "catalog_unknown", "watermark_unknown", "read_unknown", "expiry_drop", "expiry_ack"}

func runGraphReaderMaintenance(seed int64, replay *Trace) (trace Trace, runErr error) {
	s := NewScheduler(seed)
	if replay != nil {
		var e error
		s, e = ReplayScheduler(*replay)
		if e != nil {
			return trace, e
		}
	}
	if e := s.SetWorkload("graph_reader_maintenance"); e != nil {
		return trace, e
	}
	defer func() { trace = s.Trace() }()
	mode, e := s.Choose(readerMaintenanceModes)
	if e != nil {
		return trace, e
	}
	saveChoice, e := s.Choose([]string{"1", "2", "3"})
	if e != nil {
		return trace, e
	}
	saveAt, _ := strconv.Atoi(saveChoice)
	batchChoice, e := s.Choose([]string{"1", "2"})
	if e != nil {
		return trace, e
	}
	budget, _ := strconv.Atoi(batchChoice)
	ctx := context.Background()
	model := NewGraphPublicationTransport(s)
	p := model.Protocol()
	now := func() time.Time { return time.UnixMilli(s.NowMillis()).UTC() }
	start := now()
	prepared, e := p.PrepareAppend(ctx, "expired-0", 0, []byte("retained-value"), nil, start.Add(time.Second))
	if e != nil {
		return trace, e
	}
	root, e := p.Commit(ctx, prepared)
	if e != nil {
		return trace, e
	}
	_, root, e = p.AcquireReader(ctx, "expired-0", root.Head, start.Add(time.Second))
	if e != nil {
		return trace, e
	}
	if e = p.RetireLive(ctx, "expired-0", root.Head); e != nil {
		return trace, e
	}
	for i := 1; i < 5; i++ {
		if _, _, e = p.AcquireReader(ctx, fmt.Sprintf("expired-%d", i), 0, start.Add(time.Second)); e != nil {
			return trace, e
		}
	}
	if _, _, e = p.AcquireReader(ctx, "live", 0, start.Add(time.Minute)); e != nil {
		return trace, e
	}
	objects, e := model.Objects(ctx)
	if e != nil || len(objects) == 0 {
		return trace, fmt.Errorf("missing retained objects: %v", e)
	}
	if e = s.AdvanceMillis(1000); e != nil {
		return trace, e
	}
	loop := NewLoopTransport(s)
	port := readerMaintenancePort{loop}
	switch mode {
	case "cursor_drop", "cursor_ack":
		kind := KVDropBeforeCommit
		if mode == "cursor_ack" {
			kind = KVLoseAckAfterCommit
		}
		if e = loop.RejectCursorSaveAt(saveAt, kind); e != nil {
			return trace, e
		}
	case "catalog_unknown", "watermark_unknown", "read_unknown", "expiry_drop", "expiry_ack":
		op := map[string]string{"catalog_unknown": "next_root", "watermark_unknown": "catalog_high_water", "read_unknown": "read_root", "expiry_drop": "cas_root", "expiry_ack": "cas_root"}[mode]
		fault := DropBeforeCommit
		if mode == "expiry_ack" {
			fault = LoseAckAfterCommit
		}
		if e = model.QueueFault(op, fault); e != nil {
			return trace, e
		}
	}
	first, stop := context.WithCancel(ctx)
	loop.StopAfterWaits(4, stop)
	if e = reconcile.RunReaderExpiryWithPort(first, port, readerMaintenanceProtocol{p}, "first", 100*time.Millisecond, budget, now); e != nil {
		return trace, e
	}
	// Reconstruct scheduler adapter/protocol; retained lease and cursor buckets
	// are the only state transferred to the next worker.
	second, stopSecond := context.WithCancel(ctx)
	loop.StopAfterWaits(12, stopSecond)
	if e = reconcile.RunReaderExpiryWithPort(second, readerMaintenancePort{loop}, readerMaintenanceProtocol{model.Protocol()}, "replacement", 100*time.Millisecond, budget, now); e != nil {
		return trace, e
	}
	if (mode == "cursor_drop" || mode == "cursor_ack") && loop.saves < saveAt {
		return trace, fmt.Errorf("cursor fault not reached")
	}
	if len(loop.cursors.faults) != 0 {
		return trace, fmt.Errorf("cursor fault not consumed")
	}
	for op, queue := range model.faults {
		if len(queue) != 0 {
			return trace, fmt.Errorf("unconsumed %s fault", op)
		}
	}
	for i := 0; i < 5; i++ {
		r, e := model.ReadRoot(ctx, fmt.Sprintf("expired-%d", i))
		if e != nil || len(r.Readers) != 0 {
			return trace, fmt.Errorf("overdue pin survived: %d %v", i, e)
		}
	}
	live, e := model.ReadRoot(ctx, "live")
	if e != nil || len(live.Readers) != 1 {
		return trace, fmt.Errorf("live reader lost: %v", e)
	}
	after, e := model.Objects(ctx)
	if e != nil || !reflect.DeepEqual(objects, after) {
		return trace, fmt.Errorf("reader maintenance changed objects: %v", e)
	}
	if e = model.CheckReferences(); e != nil {
		return trace, e
	}
	if now().Sub(start) > 30*time.Second {
		return trace, fmt.Errorf("reader recovery exceeded 30 virtual seconds")
	}
	if e = s.Finish(); e != nil {
		return trace, e
	}
	return s.Trace(), nil
}

func TestSeededGraphReaderMaintenanceReplay(t *testing.T) {
	observed := map[string]int{}
	for seed := range seededSchedules(t) {
		generated, e := runGraphReaderMaintenance(seed, nil)
		if e != nil {
			path, _ := saveSeedFailureTrace(t.Name(), seed, generated)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, e)
		}
		replayed, e := runGraphReaderMaintenance(seed, &generated)
		if e != nil || !reflect.DeepEqual(generated, replayed) {
			path, _ := saveSeedFailureTrace(t.Name(), seed, generated)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s replay: %v", seed, path, e)
		}
		key := generated.Decisions[0].Chosen + "/" + generated.Decisions[1].Chosen + "/" + generated.Decisions[2].Chosen
		if observed[key] == 0 {
			if dir := os.Getenv("SIM_GRAPH_READER_MAINTENANCE_ROOT"); dir != "" {
				if e = generated.Save(filepath.Join(dir, fmt.Sprintf("seed-%d.json", seed))); e != nil {
					t.Fatal(e)
				}
			}
		}
		observed[key]++
	}
	if len(observed) != 48 {
		t.Fatal("incomplete declared mode/save/batch coverage", observed)
	}
	t.Logf("48 reader maintenance fault/save/batch combinations; exact replay, retained checkpoints, expired/live pin distinction, no object deletion: %v", observed)
}
