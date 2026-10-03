package sim

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/reconcile"
)

// Only immutable reads are modeled. The production scanner and fenced cursor
// loop execute; network latency and the actual failed run's cursor are unknown.
type startCapacityPort struct {
	schedule *Scheduler
	last     uint64
	repairs  int
}

func (p *startCapacityPort) GetInvocation(_ context.Context, seq uint64) (*jetstream.RawStreamMsg, error) {
	if seq == 0 || seq > p.last {
		return nil, jetstream.ErrMsgNotFound
	}
	return &jetstream.RawStreamMsg{Subject: fmt.Sprintf("wf.inv.test.capacity-%d", seq), Sequence: seq}, nil
}
func (p *startCapacityPort) LastInvocationSequence(context.Context) (uint64, error) {
	return p.last, nil
}
func (p *startCapacityPort) JournalExists(_ context.Context, subject string) (bool, error) {
	return subject != fmt.Sprintf("wf.jrn.test.capacity-%d", p.last), nil
}
func (p *startCapacityPort) EnqueueStart(_ context.Context, typ, id string, seq uint64) error {
	if typ != "test" || id != fmt.Sprintf("capacity-%d", p.last) || seq != p.last {
		return fmt.Errorf("unexpected repair %s/%s/%d", typ, id, seq)
	}
	p.repairs++
	p.schedule.RecordTransport(TransportEvent{Operation: "start_capacity_repair", Sequence: seq, AtMillis: p.schedule.NowMillis()})
	return nil
}
func runStartScanCapacity(seed int64, replay *Trace) (trace Trace, runErr error) {
	s := NewScheduler(seed)
	if replay != nil {
		var err error
		s, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	if err := s.SetWorkload("start_scan_capacity"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = s.Trace() }()
	policy, err := s.Choose([]string{"32_per_second", "64_per_100ms"})
	if err != nil {
		return trace, err
	}
	port := &startCapacityPort{schedule: s, last: 1270}
	scan := reconcile.NewStartScanWithPort(port)
	loop := NewLoopTransport(s)
	budget, cadence := 32, time.Second
	if policy == "64_per_100ms" {
		budget, cadence = 64, 100*time.Millisecond
	}
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	err = reconcile.RunLoopWithPort(ctx, loop, "start-capacity", "start", cadence, budget, func(ctx context.Context, next uint64, budget int, dry bool) (reconcile.ScanResult, error) {
		result, err := scan.Scan(ctx, next, budget, dry)
		s.RecordTransport(TransportEvent{Operation: "start_capacity_scan", Sequence: next, Outcome: fmt.Sprintf("next=%d repaired=%d", result.NextSequence, result.Reenqueued), AtMillis: s.NowMillis()})
		if result.Reenqueued > 0 || s.NowMillis() > 60000 {
			stop()
		}
		return result, err
	})
	if err != nil || port.repairs != 1 {
		return trace, fmt.Errorf("loop:%v repairs=%d", err, port.repairs)
	}
	expected := int64((port.last-1)/uint64(budget)) * cadence.Milliseconds()
	if s.NowMillis() != expected {
		return trace, fmt.Errorf("repair at%d want%d", s.NowMillis(), expected)
	}
	if policy == "32_per_second" && expected <= 30000 {
		return trace, fmt.Errorf("legacy capacity no longer exceeds target")
	}
	if policy == "64_per_100ms" && expected >= 2000 {
		return trace, fmt.Errorf("configured capacity exceeds2s")
	}
	s.RecordTransport(TransportEvent{Operation: "characterize_start_capacity", Outcome: policy, AtMillis: expected})
	if err := s.Finish(); err != nil {
		return trace, err
	}
	return s.Trace(), nil
}
func TestStartScanCapacityReplay(t *testing.T) {
	covered := map[string]bool{}
	for seed := int64(1); seed <= 16; seed++ {
		generated, err := runStartScanCapacity(seed, nil)
		if err != nil {
			t.Fatalf("seed%d:%v", seed, err)
		}
		replayed, err := runStartScanCapacity(seed, &generated)
		if err != nil || !reflect.DeepEqual(generated, replayed) {
			t.Fatalf("seed%d replay:%v", seed, err)
		}
		for _, event := range generated.Transport {
			if event.Operation == "characterize_start_capacity" {
				if os.Getenv("SIM_WRITE_START_CAPACITY_PINS") == "1" && !covered[event.Outcome] {
					if err := generated.Save(filepath.Join("testdata", "regressions", "start-capacity-"+event.Outcome+".json")); err != nil {
						t.Fatal(err)
					}
				}
				covered[event.Outcome] = true
			}
		}
	}
	if len(covered) != 2 {
		t.Fatalf("missing capacity policies:%v", covered)
	}
}
