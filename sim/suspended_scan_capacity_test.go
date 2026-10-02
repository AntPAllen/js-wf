package sim

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/journal"
	"js-wf/reconcile"
)

// Immutable read fixtures avoid imposing an artificial order on concurrent
// independent reads. The production scan and fenced cursor loop still run.
type suspendedCapacityPort struct {
	schedule          *Scheduler
	last, target      uint64
	records, terminal []journal.Record
	reenqueued        int
}

func (p *suspendedCapacityPort) LastInvocationSequence(context.Context) (uint64, error) {
	return p.last, nil
}
func (p *suspendedCapacityPort) GetInvocation(_ context.Context, seq uint64) (*jetstream.RawStreamMsg, error) {
	if seq == 0 || seq > p.last {
		return nil, jetstream.ErrMsgNotFound
	}
	return &jetstream.RawStreamMsg{Subject: fmt.Sprintf("wf.inv.test.capacity-%d", seq), Sequence: seq}, nil
}
func (p *suspendedCapacityPort) ReadJournal(_ context.Context, typ, id string) ([]journal.Record, error) {
	if typ == "test" && id == fmt.Sprintf("capacity-%d", p.target) {
		return p.records, nil
	}
	return p.terminal, nil
}
func (p *suspendedCapacityPort) GetSignalAfter(context.Context, string, uint64) (*jetstream.RawStreamMsg, error) {
	return nil, jetstream.ErrMsgNotFound
}
func (p *suspendedCapacityPort) EnqueueSuspended(_ context.Context, typ, id string, seq uint64, window int64) error {
	if typ != "test" || id != fmt.Sprintf("capacity-%d", p.target) {
		return fmt.Errorf("unexpected repair %s/%s", typ, id)
	}
	p.reenqueued++
	p.schedule.RecordTransport(TransportEvent{Operation: "capacity_repair", Subject: id, Sequence: seq, Outcome: strconv.FormatInt(window, 10), AtMillis: p.schedule.NowMillis()})
	return nil
}

func runSuspendedScanCapacity(seed int64, replay *Trace) (trace Trace, runErr error) {
	s := NewScheduler(seed)
	if replay != nil {
		var err error
		s, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	if err := s.SetWorkload("suspended_scan_capacity"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = s.Trace() }()
	policy, err := s.Choose([]string{"eight_per_second", "256_per_100ms"})
	if err != nil {
		return trace, err
	}
	size, err := s.Choose([]string{"336", "1000", "3000"})
	if err != nil {
		return trace, err
	}
	n, _ := strconv.Atoi(size)
	target := uint64(n - 20)
	if n == 336 {
		target = 313
	}
	base := time.Unix(1700000000, 0).UTC()
	records, err := suspendedFixture(target, "due_timer", base.Add(2*time.Second), 0)
	if err != nil {
		return trace, err
	}
	var request map[string]any
	if err := json.Unmarshal(records[1].Payload, &request); err != nil {
		return trace, err
	}
	request["clock_domain"] = "utc-quorum-v1"
	records[1].Payload, err = json.Marshal(request)
	if err != nil {
		return trace, err
	}
	terminal, err := suspendedFixture(1, "terminal", time.Time{}, 0)
	if err != nil {
		return trace, err
	}
	port := &suspendedCapacityPort{schedule: s, last: uint64(n), target: target, records: records, terminal: terminal}
	scan := reconcile.NewSuspendedScanWithPort(port)
	scan.Now = func() time.Time { return base.Add(time.Duration(s.NowMillis()) * time.Millisecond) }
	scan.DomainNow = func(context.Context, string) (time.Time, error) { return scan.Now(), nil }
	loop := NewLoopTransport(s)
	if _, err := loop.SaveCursor(context.Background(), "suspended", 73, 0); err != nil {
		return trace, err
	}
	budget, cadence := 8, time.Second
	if policy == "256_per_100ms" {
		budget, cadence = 256, 100*time.Millisecond
	}
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	err = reconcile.RunLoopWithPort(ctx, loop, "capacity", "suspended", cadence, budget, func(ctx context.Context, next uint64, budget int, dry bool) (reconcile.ScanResult, error) {
		result, err := scan.Scan(ctx, next, budget, dry)
		s.RecordTransport(TransportEvent{Operation: "capacity_scan", Sequence: next, Outcome: fmt.Sprintf("next=%d repaired=%d", result.NextSequence, result.Reenqueued), AtMillis: s.NowMillis()})
		if result.Reenqueued > 0 || s.NowMillis() > 600000 {
			stop()
		}
		return result, err
	})
	if err != nil || port.reenqueued != 1 {
		return trace, fmt.Errorf("loop:%v repairs=%d", err, port.reenqueued)
	}
	at := s.NowMillis()
	if at < 3000 {
		return trace, fmt.Errorf("repair before canonical deadline plus grace: %d", at)
	}
	if policy == "eight_per_second" {
		expected := int64((target-73)/8) * 1000
		if at != expected || at <= 10000 {
			return trace, fmt.Errorf("legacy repair at%d want%d after10s admission", at, expected)
		}
	} else if at > 4500 {
		return trace, fmt.Errorf("configured repair at%d exceeds4.5s capacity bound", at)
	}
	s.RecordTransport(TransportEvent{Operation: "characterize_scan_capacity", Outcome: policy + "_" + size, AtMillis: at})
	if err := s.Finish(); err != nil {
		return trace, err
	}
	return s.Trace(), nil
}

func TestSeededSuspendedScanCapacityReplay(t *testing.T) {
	covered := map[string]bool{}
	for seed := range seededSchedules(t) {
		generated, err := runSuspendedScanCapacity(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "scan-capacity.json")
			}
			_ = generated.Save(path)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runSuspendedScanCapacity(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("seed%d replay:%v", seed, err)
			}
		}
		for _, event := range generated.Transport {
			if event.Operation != "characterize_scan_capacity" {
				continue
			}
			if os.Getenv("SIM_WRITE_SCAN_CAPACITY_PINS") == "1" && !covered[event.Outcome] {
				if err := generated.Save(filepath.Join("testdata", "regressions", "suspended-capacity-"+event.Outcome+".json")); err != nil {
					t.Fatal(err)
				}
			}
			covered[event.Outcome] = true
		}
	}
	if len(covered) != 6 {
		t.Fatalf("missing capacity cells:%v", covered)
	}
}
