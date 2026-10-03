package reconcile

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/lease"
	"js-wf/provision"
	"js-wf/testcluster"
)

type prefixReadPort struct{ stage string }

func (p prefixReadPort) GetInvocation(_ context.Context, seq uint64) (*jetstream.RawStreamMsg, error) {
	if p.stage == "first_read" || (seq == 3 && p.stage == "read") {
		return nil, nats.ErrTimeout
	}
	return &jetstream.RawStreamMsg{Subject: fmt.Sprintf("wf.inv.test.prefix-%d", seq), Sequence: seq}, nil
}
func (p prefixReadPort) LastInvocationSequence(context.Context) (uint64, error) { return 3, nil }
func (p prefixReadPort) JournalExists(_ context.Context, subject string) (bool, error) {
	if subject == "wf.jrn.test.prefix-3" {
		if p.stage == "journal" {
			return false, context.DeadlineExceeded
		}
		return false, nil
	}
	return true, nil
}
func (p prefixReadPort) EnqueueStart(context.Context, string, string, uint64) error {
	return nats.ErrTimeout
}
func TestStartPartialCursorDoesNotSkipUnconfirmedInvocation(t *testing.T) {
	for _, stage := range []string{"first_read", "read", "journal", "enqueue"} {
		t.Run(stage, func(t *testing.T) {
			result, err := NewStartScanWithPort(prefixReadPort{stage}).Scan(context.Background(), 1, 500, false)
			if err == nil {
				t.Fatal("injected error not observed")
			}
			expected := uint64(3)
			if stage == "first_read" {
				expected = 0
			}
			if result.RetrySequence != expected {
				t.Fatalf("retry=%d want%d result=%+v", result.RetrySequence, expected, result)
			}
		})
	}
}

type prefixLoopPort struct {
	stop          context.CancelFunc
	scanContext   context.Context
	lost          bool
	renews, saves int
}

func (p *prefixLoopPort) Prepare(context.Context) error                              { return nil }
func (p *prefixLoopPort) Acquire(context.Context, string, string) (LoopLease, error) { return p, nil }
func (p *prefixLoopPort) LoadCursor(context.Context, string) (uint64, uint64, error) {
	return 1, 7, nil
}
func (p *prefixLoopPort) SaveCursor(ctx context.Context, _ string, next, revision uint64) (uint64, error) {
	if ctx.Err() != nil || next != 3 || revision != 7 {
		return 0, fmt.Errorf("invalid checkpoint context/cursor/revision")
	}
	p.saves++
	return 8, nil
}
func (p *prefixLoopPort) Wait(context.Context, time.Duration) error { p.stop(); return nil }
func (p *prefixLoopPort) Renew(ctx context.Context) error {
	p.renews++
	if p.renews == 2 {
		if p.scanContext.Err() == nil || ctx.Err() != nil {
			return errors.New("checkpoint reused live/expired scan context")
		}
		if p.lost {
			return lease.ErrLost
		}
	}
	return nil
}
func (p *prefixLoopPort) Release(context.Context) error { return nil }
func TestStartPartialCursorRequiresFreshOwnershipAndDoesNotSaveFatalOrCancelledPass(t *testing.T) {
	for _, mode := range []string{"owned", "lost", "fatal", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			ctx, stop := context.WithCancel(context.Background())
			defer stop()
			port := &prefixLoopPort{stop: stop, lost: mode == "lost"}
			permanent := errors.New("corrupt invocation")
			err := RunLoopWithPort(ctx, port, "prefix", "start", time.Second, 500, func(attempt context.Context, _ uint64, _ int, _ bool) (ScanResult, error) {
				port.scanContext = attempt
				if mode == "cancelled" {
					stop()
				}
				if mode == "fatal" {
					return ScanResult{RetrySequence: 3}, permanent
				}
				return ScanResult{RetrySequence: 3}, context.DeadlineExceeded
			})
			if mode == "fatal" {
				if !errors.Is(err, permanent) {
					t.Fatalf("fatal:%v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			expected := 0
			if mode == "owned" {
				expected = 1
			}
			if port.saves != expected {
				t.Fatalf("saves=%d want%d renews=%d", port.saves, expected, port.renews)
			}
		})
	}
}

type interruptedRealStartPort struct {
	StartScanPort
	reads                   int
	uncertain, acknowledged int
}

func (p *interruptedRealStartPort) GetInvocation(ctx context.Context, seq uint64) (*jetstream.RawStreamMsg, error) {
	p.reads++
	if p.reads == 4 {
		return nil, nats.ErrTimeout
	}
	return p.StartScanPort.GetInvocation(ctx, seq)
}
func (p *interruptedRealStartPort) EnqueueStart(ctx context.Context, typ, id string, seq uint64) error {
	if err := p.StartScanPort.EnqueueStart(ctx, typ, id, seq); err != nil {
		return err
	}
	if p.uncertain == 0 {
		p.uncertain++
		return nats.ErrTimeout
	}
	p.acknowledged++
	return nil
}
func TestStartPartialCursorRepairsRealGapAfterRepeatedReadTimeoutsAndLostEnqueueAck(t *testing.T) {
	cluster, err := testcluster.Start(t.TempDir(), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(context.Background(), 20*time.Second)
	defer stop()
	for ctx.Err() == nil {
		attempt, done := context.WithTimeout(ctx, 2*time.Second)
		_, err = js.AccountInfo(attempt)
		done()
		if err == nil {
			attempt, done = context.WithTimeout(ctx, 2*time.Second)
			err = provision.Ensure(attempt, js, 3)
			done()
		}
		if err == nil {
			break
		}
		var api *jetstream.APIError
		placement := errors.As(err, &api) && api.ErrorCode == 10005
		if !retryableReconcileError(err) && !placement {
			t.Fatal(err)
		}
		t.Logf("bounded startup retry:%v", err)
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	for seq := 1; seq <= 8; seq++ {
		if _, err := js.Publish(ctx, fmt.Sprintf("wf.inv.test.real-prefix-%d", seq), []byte("null")); err != nil {
			t.Fatal(err)
		}
		if seq < 8 {
			if _, err := js.Publish(ctx, fmt.Sprintf("wf.jrn.test.real-prefix-%d", seq), []byte(`{"kind":"Started","index":0,"epoch":1,"worker_id":"fixture"}`)); err != nil {
				t.Fatal(err)
			}
		}
	}
	port := &interruptedRealStartPort{StartScanPort: NewStartScan(js).port}
	scan := NewStartScanWithPort(port)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var cursors []uint64
	err = runLoop(runCtx, js, "prefix-contract", "start", 10*time.Millisecond, 500, func(attempt context.Context, next uint64, budget int, dry bool) (ScanResult, error) {
		cursors = append(cursors, next)
		port.reads = 0
		result, err := scan.Scan(attempt, next, budget, dry)
		if port.acknowledged > 0 {
			cancel()
		}
		return result, err
	})
	if err != nil {
		t.Fatal(err)
	}
	if port.uncertain != 1 || port.acknowledged != 1 {
		t.Fatalf("uncertain=%d acknowledged=%d cursors=%v", port.uncertain, port.acknowledged, cursors)
	}
	expected := []uint64{1, 4, 7, 8}
	if fmt.Sprint(cursors) != fmt.Sprint(expected) {
		t.Fatalf("cursors=%v want%v", cursors, expected)
	}
	state, err := js.KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	entry, err := state.Get(ctx, "scan.start")
	if err != nil {
		t.Fatal(err)
	}
	next, err := strconv.ParseUint(string(entry.Value()), 10, 64)
	if err != nil || next != 8 {
		t.Fatalf("retained cursor=%d err=%v", next, err)
	}
	run, err := js.Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	info, err := run.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("committed retry duplicated/dropped run:%+v", info.State)
	}
	raw, err := run.GetMsg(ctx, info.State.FirstSeq)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw.Data) != "test.real-prefix-8" {
		t.Fatalf("wrong repair identity:%q", raw.Data)
	}
	t.Logf("actual_cursor_sequence=%v retained_cursor=%d uncertain_enqueue=1 acknowledged_retry=1 retained_run_messages=1", cursors, next)
}
