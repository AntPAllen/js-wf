package sim

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/reconcile"
)

// A five-second virtual attempt bounds sequential read cost. Reads are
// immutable; actual scanner, lease acquisition and cursor CAS execute.
type partialStartPort struct {
	schedule               *Scheduler
	deadline, cost         int64
	uncertain              bool
	enqueues, acknowledged int
}

func (p *partialStartPort) request(operation string, seq uint64) error {
	next := p.schedule.NowMillis() + p.cost
	outcome := "ok"
	if next >= p.deadline {
		next = p.deadline
		outcome = "deadline_exceeded"
	}
	p.schedule.AdvanceMillis(next - p.schedule.NowMillis())
	p.schedule.RecordTransport(TransportEvent{Operation: operation, Sequence: seq, Outcome: outcome, AtMillis: p.schedule.NowMillis()})
	if outcome != "ok" {
		return context.DeadlineExceeded
	}
	return nil
}
func (p *partialStartPort) GetInvocation(_ context.Context, seq uint64) (*jetstream.RawStreamMsg, error) {
	if err := p.request("partial_invocation_read", seq); err != nil {
		return nil, err
	}
	if seq > 35 {
		return nil, jetstream.ErrMsgNotFound
	}
	return &jetstream.RawStreamMsg{Subject: fmt.Sprintf("wf.inv.test.partial-%d", seq), Sequence: seq}, nil
}
func (p *partialStartPort) LastInvocationSequence(context.Context) (uint64, error) {
	return 35, p.request("partial_last_sequence", 35)
}
func (p *partialStartPort) JournalExists(_ context.Context, subject string) (bool, error) {
	if err := p.request("partial_journal_read", 0); err != nil {
		return false, err
	}
	return subject != "wf.jrn.test.partial-35", nil
}
func (p *partialStartPort) EnqueueStart(_ context.Context, typ, id string, seq uint64) error {
	if typ != "test" || id != "partial-35" || seq != 35 {
		return fmt.Errorf("wrong repair %s/%s/%d", typ, id, seq)
	}
	if err := p.request("partial_start_enqueue", seq); err != nil {
		return err
	}
	p.enqueues++
	if p.uncertain && p.enqueues == 1 {
		p.schedule.RecordTransport(TransportEvent{Operation: "partial_enqueue_ack", Sequence: seq, Outcome: "committed_ack_lost", AtMillis: p.schedule.NowMillis()})
		return nats.ErrTimeout
	}
	p.acknowledged++
	return nil
}
func runStartPartialCursor(seed int64, replay *Trace) (trace Trace, runErr error) {
	s := NewScheduler(seed)
	if replay != nil {
		var err error
		s, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	if err := s.SetWorkload("start_partial_cursor"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = s.Trace() }()
	policy, err := s.Choose([]string{"discard_prefix", "checkpoint_prefix"})
	if err != nil {
		return trace, err
	}
	cost, err := s.Choose([]string{"100ms", "150ms"})
	if err != nil {
		return trace, err
	}
	ack, err := s.Choose([]string{"ack", "lost_ack"})
	if err != nil {
		return trace, err
	}
	save, err := s.Choose([]string{"ack", "drop", "lost_ack"})
	if err != nil {
		return trace, err
	}
	port := &partialStartPort{schedule: s, cost: 100, uncertain: ack == "lost_ack"}
	if cost == "150ms" {
		port.cost = 150
	}
	loop := NewLoopTransport(s)
	if save != "ack" {
		kind := KVDropBeforeCommit
		if save == "lost_ack" {
			kind = KVLoseAckAfterCommit
		}
		if err := loop.RejectCursorSaveAt(1, kind); err != nil {
			return trace, err
		}
	}
	scan := reconcile.NewStartScanWithPort(port)
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	loop.StopAfterWaits(8, stop)
	err = reconcile.RunLoopWithPort(ctx, loop, "partial-capacity", "start", time.Second, 500, func(ctx context.Context, next uint64, budget int, dry bool) (reconcile.ScanResult, error) {
		port.deadline = s.NowMillis() + 5000
		result, err := scan.Scan(ctx, next, budget, dry)
		if policy == "discard_prefix" {
			result.RetrySequence = 0
		}
		s.RecordTransport(TransportEvent{Operation: "partial_scan", Sequence: next, Outcome: fmt.Sprintf("retry=%d error=%t", result.RetrySequence, err != nil), AtMillis: s.NowMillis()})
		if port.acknowledged > 0 {
			stop()
		}
		return result, err
	})
	if err != nil {
		return trace, err
	}
	cursor, _, err := loop.LoadCursor(context.Background(), "start")
	if err != nil {
		return trace, err
	}
	if policy == "discard_prefix" {
		if port.acknowledged != 0 || cursor != 1 {
			return trace, fmt.Errorf("legacy control advanced:acks=%d cursor=%d", port.acknowledged, cursor)
		}
	} else if port.acknowledged != 1 || s.NowMillis() >= 30000 {
		return trace, fmt.Errorf("prefix checkpoint fails progress:acks=%d cursor=%d at=%d", port.acknowledged, cursor, s.NowMillis())
	}
	if port.acknowledged > 0 && port.enqueues != 1+map[bool]int{true: 1, false: 0}[port.uncertain] {
		return trace, fmt.Errorf("uncertain repair skipped/replaced:enqueues=%d", port.enqueues)
	}
	s.RecordTransport(TransportEvent{Operation: "characterize_partial_cursor", Outcome: policy + "_" + save + "_" + ack, AtMillis: s.NowMillis()})
	if err := s.Finish(); err != nil {
		return trace, err
	}
	return s.Trace(), nil
}
func TestStartPartialCursorReplay(t *testing.T) {
	covered := map[string]bool{}
	for seed := int64(1); seed <= 128; seed++ {
		generated, err := runStartPartialCursor(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "partial-cursor.json")
			}
			_ = generated.Save(path)
			t.Fatalf("seed%d trace=%s:%v", seed, path, err)
		}
		replayed, err := runStartPartialCursor(seed, &generated)
		if err != nil || !reflect.DeepEqual(generated, replayed) {
			t.Fatalf("seed%d replay:%v", seed, err)
		}
		for _, event := range generated.Transport {
			if event.Operation == "characterize_partial_cursor" {
				if os.Getenv("SIM_WRITE_PARTIAL_CURSOR_PINS") == "1" && !covered[event.Outcome] {
					if err := generated.Save(filepath.Join("testdata", "regressions", "start-partial-"+event.Outcome+".json")); err != nil {
						t.Fatal(err)
					}
				}
				covered[event.Outcome] = true
			}
		}
	}
	if len(covered) != 12 {
		t.Fatalf("missing cursor/ack policy cells:%v", covered)
	}
}
