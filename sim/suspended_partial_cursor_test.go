package sim

import (
	"context"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/journal"
	"js-wf/reconcile"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"
)

// The admitted prefix is immutable for one concurrent scan. Later reads fail;
// this tests retry progress without imposing a goroutine completion order.
type partialSuspendedPort struct {
	schedule               *Scheduler
	retained               *SignalTransport
	limit                  uint64
	uncertain              bool
	enqueues, acknowledged int
}

func (p *partialSuspendedPort) LastInvocationSequence(context.Context) (uint64, error) {
	return 260, nil
}
func (p *partialSuspendedPort) GetInvocation(_ context.Context, seq uint64) (*jetstream.RawStreamMsg, error) {
	if seq > p.limit {
		return nil, context.DeadlineExceeded
	}
	if seq%7 == 0 && seq != 220 {
		return nil, jetstream.ErrMsgNotFound
	}
	return &jetstream.RawStreamMsg{Subject: fmt.Sprintf("wf.inv.test.prefix-%d", seq), Sequence: seq}, nil
}
func (p *partialSuspendedPort) ReadJournal(_ context.Context, typ, id string) ([]journal.Record, error) {
	if typ == "test" && id == "prefix-220" {
		return []journal.Record{{Entry: journal.Entry{Kind: journal.Suspended, Payload: []byte(`{"waiting_on":"continuation:next"}`)}, Sequence: 440}}, nil
	}
	return []journal.Record{{Entry: journal.Entry{Kind: journal.Completed}}}, nil
}
func (p *partialSuspendedPort) GetSignalAfter(context.Context, string, uint64) (*jetstream.RawStreamMsg, error) {
	return nil, jetstream.ErrMsgNotFound
}
func (p *partialSuspendedPort) EnqueueSuspended(ctx context.Context, typ, id string, seq uint64, window int64) error {
	if typ != "test" || id != "prefix-220" || seq != 440 {
		return fmt.Errorf("wrong repair %s/%s/%d", typ, id, seq)
	}
	if err := p.retained.EnqueueSuspended(ctx, typ, id, seq, window); err != nil {
		return err
	}
	p.enqueues++
	p.schedule.RecordTransport(TransportEvent{Operation: "partial_suspended_enqueue", Sequence: seq, Outcome: strconv.FormatInt(window, 10), AtMillis: p.schedule.NowMillis()})
	if p.uncertain && p.enqueues == 1 {
		return nats.ErrTimeout
	}
	p.acknowledged++
	return nil
}
func runSuspendedPartialCursor(seed int64, replay *Trace) (trace Trace, runErr error) {
	s := NewScheduler(seed)
	if replay != nil {
		var err error
		s, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	if err := s.SetWorkload("suspended_partial_cursor"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = s.Trace() }()
	policy, err := s.Choose([]string{"discard_prefix", "checkpoint_prefix"})
	if err != nil {
		return trace, err
	}
	batch, err := s.Choose([]string{"96", "128"})
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
	port := &partialSuspendedPort{schedule: s, retained: NewSignalTransport(s), uncertain: ack == "lost_ack"}
	width, _ := strconv.ParseUint(batch, 10, 64)
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
	scan := reconcile.NewSuspendedScanWithPort(port)
	// Keep retries in one deduplication window to check the lost-ack identity.
	scan.Now = func() time.Time { return time.Unix(1700000000, 0).UTC() }
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	loop.StopAfterWaits(8, stop)
	err = reconcile.RunLoopWithPort(ctx, loop, "partial-capacity", "suspended", time.Second, 500, func(ctx context.Context, next uint64, budget int, dry bool) (reconcile.ScanResult, error) {
		port.limit = next + width - 1
		result, err := scan.Scan(ctx, next, budget, dry)
		// This fixture models a five-second failed attempt, not parallel request
		// wall cost. Concurrent reads are immutable and independent of Go order.
		if err != nil {
			s.AdvanceMillis(5000)
		}
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
	cursor, _, err := loop.LoadCursor(context.Background(), "suspended")
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
	if len(runs) != want || (want == 1 && string(runs[0].Data) != "test.prefix-220") {
		return trace, fmt.Errorf("retained suspended wakeup mismatch:%+v want%d", runs, want)
	}
	s.RecordTransport(TransportEvent{Operation: "characterize_partial_cursor", Outcome: policy + "_" + save + "_" + ack, AtMillis: s.NowMillis()})
	if err := s.Finish(); err != nil {
		return trace, err
	}
	return s.Trace(), nil
}
func TestSuspendedPartialCursorReplay(t *testing.T) {
	covered := map[string]bool{}
	for seed := int64(1); seed <= 128; seed++ {
		generated, err := runSuspendedPartialCursor(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "partial-cursor.json")
			}
			_ = generated.Save(path)
			t.Fatalf("seed%d trace=%s:%v", seed, path, err)
		}
		replayed, err := runSuspendedPartialCursor(seed, &generated)
		if err != nil || !reflect.DeepEqual(generated, replayed) {
			t.Fatalf("seed%d replay:%v", seed, err)
		}
		for _, event := range generated.Transport {
			if event.Operation == "characterize_partial_cursor" {
				if os.Getenv("SIM_WRITE_SUSPENDED_PARTIAL_CURSOR_PINS") == "1" && !covered[event.Outcome] {
					if err := generated.Save(filepath.Join("testdata", "regressions", "suspended-partial-"+event.Outcome+".json")); err != nil {
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
