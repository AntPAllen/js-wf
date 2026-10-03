package sim

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/journal"
	"js-wf/reconcile"
)

// Prefix cases include terminal/consumed signals, absent invocations, stale
// generations and purged holes. Only sequence35 needs a wakeup. Request costs
// and the attempt deadline are virtual; the production scanner/loop execute.
type partialSignalPort struct {
	partialStartPort
	retained *SignalTransport
}

func (p *partialSignalPort) GetSignal(_ context.Context, seq uint64) (*jetstream.RawStreamMsg, error) {
	if err := p.request("partial_signal_read", seq); err != nil {
		return nil, err
	}
	if seq > 35 || (seq != 35 && seq%5 == 0) {
		return nil, jetstream.ErrMsgNotFound
	}
	return &jetstream.RawStreamMsg{Subject: fmt.Sprintf("wf.sig.test.partial-%d.go", seq), Sequence: seq, Header: map[string][]string{"Wf-Inv-Seq": {"10"}}}, nil
}
func (p *partialSignalPort) LastSignalSequence(context.Context) (uint64, error) {
	return 35, p.request("partial_signal_last", 35)
}
func (p *partialSignalPort) ReadJournal(_ context.Context, typ, id string) ([]journal.Record, error) {
	seq, _ := strconv.ParseUint(strings.TrimPrefix(id, "partial-"), 10, 64)
	if err := p.request("partial_signal_journal", seq); err != nil {
		return nil, err
	}
	if seq != 35 {
		switch seq % 5 {
		case 1:
			return []journal.Record{{Entry: journal.Entry{Kind: journal.Completed}}}, nil
		case 2:
			return []journal.Record{{Entry: journal.Entry{Kind: journal.SignalConsumed, Payload: []byte(fmt.Sprintf(`{"sig_seq":%d}`, seq))}}}, nil
		}
	}
	return nil, nil
}
func (p *partialSignalPort) LastInvocation(_ context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	seq, _ := strconv.ParseUint(strings.TrimPrefix(subject, "wf.inv.test.partial-"), 10, 64)
	if err := p.request("partial_signal_invocation", seq); err != nil {
		return nil, err
	}
	if seq != 35 && seq%5 == 3 {
		return nil, jetstream.ErrMsgNotFound
	}
	generation := uint64(10)
	if seq != 35 && seq%5 == 4 {
		generation = 11
	}
	return &jetstream.RawStreamMsg{Subject: subject, Sequence: generation}, nil
}
func (p *partialSignalPort) EnqueueSignal(ctx context.Context, typ, id string, seq uint64) error {
	if typ != "test" || id != "partial-35" || seq != 35 {
		return fmt.Errorf("wrong signal repair %s/%s/%d", typ, id, seq)
	}
	if err := p.request("partial_signal_enqueue", seq); err != nil {
		return err
	}
	if err := p.retained.EnqueueSignal(ctx, typ, id, seq); err != nil {
		return err
	}
	p.enqueues++
	if p.uncertain && p.enqueues == 1 {
		p.schedule.RecordTransport(TransportEvent{Operation: "partial_signal_enqueue_ack", Sequence: seq, Outcome: "committed_ack_lost", AtMillis: p.schedule.NowMillis()})
		return nats.ErrTimeout
	}
	p.acknowledged++
	return nil
}

func runSignalPartialCursor(seed int64, replay *Trace) (trace Trace, runErr error) {
	s := NewScheduler(seed)
	if replay != nil {
		var err error
		s, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	if err := s.SetWorkload("signal_partial_cursor"); err != nil {
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
	port := &partialSignalPort{partialStartPort: partialStartPort{schedule: s, cost: 100, uncertain: ack == "lost_ack"}, retained: NewSignalTransport(s)}
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
	scan := reconcile.NewSignalScanWithPort(port)
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	loop.StopAfterWaits(8, stop)
	err = reconcile.RunLoopWithPort(ctx, loop, "partial-capacity", "signal", time.Second, 500, func(ctx context.Context, next uint64, budget int, dry bool) (reconcile.ScanResult, error) {
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
	cursor, _, err := loop.LoadCursor(context.Background(), "signal")
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
	runs := port.retained.Runs()
	want := 0
	if port.acknowledged > 0 {
		want = 1
	}
	if len(runs) != want || (want == 1 && string(runs[0].Data) != "test.partial-35") {
		return trace, fmt.Errorf("retained signal wakeup mismatch:%+v want%d", runs, want)
	}
	s.RecordTransport(TransportEvent{Operation: "characterize_partial_cursor", Outcome: policy + "_" + save + "_" + ack, AtMillis: s.NowMillis()})
	if err := s.Finish(); err != nil {
		return trace, err
	}
	return s.Trace(), nil
}
func TestSignalPartialCursorReplay(t *testing.T) {
	covered := map[string]bool{}
	for seed := int64(1); seed <= 128; seed++ {
		generated, err := runSignalPartialCursor(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "partial-cursor.json")
			}
			_ = generated.Save(path)
			t.Fatalf("seed%d trace=%s:%v", seed, path, err)
		}
		replayed, err := runSignalPartialCursor(seed, &generated)
		if err != nil || !reflect.DeepEqual(generated, replayed) {
			t.Fatalf("seed%d replay:%v", seed, err)
		}
		for _, event := range generated.Transport {
			if event.Operation == "characterize_partial_cursor" {
				if os.Getenv("SIM_WRITE_SIGNAL_PARTIAL_CURSOR_PINS") == "1" && !covered[event.Outcome] {
					if err := generated.Save(filepath.Join("testdata", "regressions", "signal-partial-"+event.Outcome+".json")); err != nil {
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
