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
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/reconcile"
)

var timerPartialBase = time.Unix(1700000000, 0).UTC()

// Sequential requests consume virtual time; actual timer scanners and cursor
// loop execute. Native prefix journals are terminal; fallback prefix timers are
// future, so advancing their cursors cannot accidentally acknowledge a wakeup.
type partialTimerPort struct {
	partialStartPort
	retained       *SignalTransport
	target         uint64
	deleted        bool
	deleteFault    string
	deleteAttempts int
}

func (p *partialTimerPort) GetInvocation(_ context.Context, seq uint64) (*jetstream.RawStreamMsg, error) {
	if err := p.request("partial_timer_invocation", seq); err != nil {
		return nil, err
	}
	if seq > p.target {
		return nil, jetstream.ErrMsgNotFound
	}
	return &jetstream.RawStreamMsg{Subject: fmt.Sprintf("wf.inv.test.progress-%d", seq), Sequence: seq}, nil
}
func (p *partialTimerPort) LastInvocationSequence(context.Context) (uint64, error) {
	return p.target, p.request("partial_timer_last", p.target)
}
func (p *partialTimerPort) ReadJournal(_ context.Context, _, id string) ([]journal.Record, error) {
	if err := p.request("partial_timer_journal", 0); err != nil {
		return nil, err
	}
	if id != fmt.Sprintf("progress-%d", p.target) {
		return []journal.Record{{Entry: journal.Entry{Kind: journal.Completed}}}, nil
	}
	return []journal.Record{{Entry: journal.Entry{Kind: journal.StepRequested, Payload: []byte(fmt.Sprintf(`{"kind":"timer","fire_at":%q}`, timerPartialBase.Add(-time.Second).Format(time.RFC3339Nano)))}, Sequence: p.target * 2}}, nil
}
func (p *partialTimerPort) EnqueueTimer(ctx context.Context, typ, id string, seq uint64) error {
	if typ != "test" || id != fmt.Sprintf("progress-%d", p.target) || seq != p.target*2 {
		return fmt.Errorf("wrong native repair:%s/%s/%d", typ, id, seq)
	}
	if err := p.request("partial_timer_enqueue", seq); err != nil {
		return err
	}
	if err := p.retained.EnqueueTimer(ctx, typ, id, seq); err != nil {
		return err
	}
	return p.enqueueAck(seq)
}
func (p *partialTimerPort) enqueueAck(seq uint64) error {
	p.enqueues++
	if p.uncertain && p.enqueues == 1 {
		p.schedule.RecordTransport(TransportEvent{Operation: "partial_timer_enqueue_ack", Sequence: seq, Outcome: "committed_ack_lost", AtMillis: p.schedule.NowMillis()})
		return nats.ErrTimeout
	}
	p.acknowledged++
	return nil
}
func (p *partialTimerPort) LastTimerSequence(context.Context) (uint64, error) {
	return p.target, p.request("partial_fallback_last", p.target)
}
func (p *partialTimerPort) GetTimer(_ context.Context, seq uint64) (*jetstream.RawStreamMsg, error) {
	if err := p.request("partial_fallback_read", seq); err != nil {
		return nil, err
	}
	if seq > p.target || (seq == p.target && p.deleted) {
		return nil, jetstream.ErrMsgNotFound
	}
	fire := timerPartialBase.Add(48 * time.Hour)
	if seq == p.target {
		fire = timerPartialBase.Add(-time.Second)
	}
	return &jetstream.RawStreamMsg{Subject: fmt.Sprintf("wf.timer.test.progress-%d.10.1", seq), Sequence: seq, Data: []byte(fmt.Sprintf(`{"fire_at":%q}`, fire.Format(time.RFC3339Nano)))}, nil
}
func (p *partialTimerPort) StateValue(_ context.Context, key string) ([]byte, error) {
	if err := p.request("partial_fallback_state", 0); err != nil {
		return nil, err
	}
	return nil, jetstream.ErrKeyNotFound
}
func (p *partialTimerPort) PublishWakeup(ctx context.Context, msg *nats.Msg, id string) error {
	if string(msg.Data) != fmt.Sprintf("test.progress-%d", p.target) || msg.Header.Get(identity.TimerInvSeqHeader) != "10" || msg.Header.Get(identity.TimerStepHeader) != "1" || id != fmt.Sprintf("fallback-timer:test:progress-%d:10:1", p.target) {
		return fmt.Errorf("wrong fallback repair:%s %+v", id, msg)
	}
	if err := p.request("partial_fallback_publish", p.target); err != nil {
		return err
	}
	if err := p.retained.EnqueueRun(ctx, msg.Subject, msg.Data, id); err != nil {
		return err
	}
	return p.enqueueAck(p.target)
}
func (p *partialTimerPort) DeleteTimer(_ context.Context, seq uint64) error {
	if seq != p.target {
		return fmt.Errorf("deleted future timer%d", seq)
	}
	if err := p.request("partial_fallback_delete", seq); err != nil {
		return err
	}
	p.deleteAttempts++
	if p.deleteAttempts == 1 && p.deleteFault == "drop" {
		p.schedule.RecordTransport(TransportEvent{Operation: "partial_fallback_delete_commit", Sequence: seq, Outcome: "drop_before_commit", AtMillis: p.schedule.NowMillis()})
		return nats.ErrTimeout
	}
	p.deleted = true
	if p.deleteAttempts == 1 && p.deleteFault == "lost_ack" {
		p.schedule.RecordTransport(TransportEvent{Operation: "partial_fallback_delete_commit", Sequence: seq, Outcome: "committed_ack_lost", AtMillis: p.schedule.NowMillis()})
		return nats.ErrTimeout
	}
	p.schedule.RecordTransport(TransportEvent{Operation: "partial_fallback_delete_commit", Sequence: seq, Outcome: "acknowledged", AtMillis: p.schedule.NowMillis()})
	return nil
}
func runTimerPartialCursor(seed int64, replay *Trace) (trace Trace, runErr error) {
	s := NewScheduler(seed)
	if replay != nil {
		var err error
		s, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	if err := s.SetWorkload("timer_partial_cursor"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = s.Trace() }()
	backend, err := s.Choose([]string{"timer", "fallback-timer"})
	if err != nil {
		return trace, err
	}
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
	deletion, err := s.Choose([]string{"ack", "drop", "lost_ack"})
	if err != nil {
		return trace, err
	}
	port := &partialTimerPort{partialStartPort: partialStartPort{schedule: s, cost: 100, uncertain: ack == "lost_ack"}, retained: NewSignalTransport(s), target: 35, deleteFault: deletion}
	if backend == "fallback-timer" {
		port.target = 20
	}
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
	native := reconcile.NewTimerScanWithPort(port)
	native.Now = func() time.Time { return timerPartialBase }
	fallback := reconcile.NewFallbackTimerScanWithPort(port, func(context.Context) (time.Time, error) { return timerPartialBase, nil })
	scan := native.Scan
	if backend == "fallback-timer" {
		scan = fallback.Scan
	}
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	loop.StopAfterWaits(8, stop)
	err = reconcile.RunLoopWithPort(ctx, loop, "partial-capacity", backend, time.Second, 500, func(ctx context.Context, next uint64, budget int, dry bool) (reconcile.ScanResult, error) {
		port.deadline = s.NowMillis() + 5000
		result, err := scan(ctx, next, budget, dry)
		if policy == "discard_prefix" {
			result.RetrySequence = 0
		}
		s.RecordTransport(TransportEvent{Operation: "partial_scan", Sequence: next, Outcome: fmt.Sprintf("retry=%d error=%t", result.RetrySequence, err != nil), AtMillis: s.NowMillis()})
		if (backend == "timer" && port.acknowledged > 0) || (backend == "fallback-timer" && port.deleted && err == nil) {
			stop()
		}
		return result, err
	})
	if err != nil {
		return trace, err
	}
	cursor, _, err := loop.LoadCursor(context.Background(), backend)
	if err != nil {
		return trace, err
	}
	if policy == "discard_prefix" {
		if port.acknowledged != 0 || cursor != 1 {
			return trace, fmt.Errorf("legacy control advanced:acks=%d cursor=%d", port.acknowledged, cursor)
		}
	} else if port.acknowledged < 1 || s.NowMillis() >= 30000 {
		return trace, fmt.Errorf("prefix checkpoint fails progress:acks=%d cursor=%d at=%d", port.acknowledged, cursor, s.NowMillis())
	}
	runs := port.retained.Runs()
	want := 0
	if policy == "checkpoint_prefix" {
		want = 1
	}
	if len(runs) != want || (want == 1 && string(runs[0].Data) != fmt.Sprintf("test.progress-%d", port.target)) {
		return trace, fmt.Errorf("retained timer wakeup mismatch:%+v want%d", runs, want)
	}
	cell := backend + "_" + policy + "_" + save + "_" + ack
	if backend == "fallback-timer" {
		cell += "_delete_" + deletion
	}
	s.RecordTransport(TransportEvent{Operation: "characterize_partial_cursor", Outcome: cell, AtMillis: s.NowMillis()})
	if err := s.Finish(); err != nil {
		return trace, err
	}
	return s.Trace(), nil
}
func TestTimerPartialCursorReplay(t *testing.T) {
	covered := map[string]bool{}
	for seed := int64(1); seed <= 1024; seed++ {
		generated, err := runTimerPartialCursor(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "partial-cursor.json")
			}
			_ = generated.Save(path)
			t.Fatalf("seed%d trace=%s:%v", seed, path, err)
		}
		replayed, err := runTimerPartialCursor(seed, &generated)
		if err != nil || !reflect.DeepEqual(generated, replayed) {
			t.Fatalf("seed%d replay:%v", seed, err)
		}
		for _, event := range generated.Transport {
			if event.Operation == "characterize_partial_cursor" {
				if os.Getenv("SIM_WRITE_TIMER_PARTIAL_CURSOR_PINS") == "1" && !covered[event.Outcome] {
					if err := generated.Save(filepath.Join("testdata", "regressions", "timer-partial-"+event.Outcome+".json")); err != nil {
						t.Fatal(err)
					}
				}
				covered[event.Outcome] = true
			}
		}
	}
	if len(covered) != 48 {
		t.Fatalf("missing cursor/ack policy cells:%v", covered)
	}
}
