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
	"js-wf/provision"
	"js-wf/testcluster"
)

type interruptedRealTimerPort struct {
	TimerScanPort
	FallbackTimerScanPort
	limit                                    uint64
	uncertain, acknowledged, deleteUncertain int
}

func (p *interruptedRealTimerPort) GetInvocation(ctx context.Context, seq uint64) (*jetstream.RawStreamMsg, error) {
	if seq > p.limit {
		return nil, nats.ErrTimeout
	}
	return p.TimerScanPort.GetInvocation(ctx, seq)
}
func (p *interruptedRealTimerPort) GetTimer(ctx context.Context, seq uint64) (*jetstream.RawStreamMsg, error) {
	if seq > p.limit {
		return nil, nats.ErrTimeout
	}
	return p.FallbackTimerScanPort.GetTimer(ctx, seq)
}
func (p *interruptedRealTimerPort) hideEnqueueAck(err error) error {
	if err != nil {
		return err
	}
	if p.uncertain == 0 {
		p.uncertain++
		return nats.ErrTimeout
	}
	p.acknowledged++
	return nil
}
func (p *interruptedRealTimerPort) EnqueueTimer(ctx context.Context, typ, id string, seq uint64) error {
	return p.hideEnqueueAck(p.TimerScanPort.EnqueueTimer(ctx, typ, id, seq))
}
func (p *interruptedRealTimerPort) PublishWakeup(ctx context.Context, msg *nats.Msg, id string) error {
	return p.hideEnqueueAck(p.FallbackTimerScanPort.PublishWakeup(ctx, msg, id))
}
func (p *interruptedRealTimerPort) DeleteTimer(ctx context.Context, seq uint64) error {
	if err := p.FallbackTimerScanPort.DeleteTimer(ctx, seq); err != nil {
		return err
	}
	if p.deleteUncertain == 0 {
		p.deleteUncertain++
		return nats.ErrTimeout
	}
	return nil
}
func TestTimerPartialCursorRepairsRealNativeAndFallbackGaps(t *testing.T) {
	for _, kind := range []string{"timer", "fallback-timer"} {
		t.Run(kind, func(t *testing.T) {
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
					if kind == "timer" {
						err = provision.Ensure(attempt, js, 3)
					} else {
						err = provision.EnsureFallback(attempt, js, 3)
					}
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
				id := fmt.Sprintf("real-prefix-%d", seq)
				if kind == "fallback-timer" {
					fire := time.Now().Add(48 * time.Hour)
					if seq == 8 {
						fire = time.Now().Add(-time.Second)
					}
					if _, err = js.Publish(ctx, "wf.timer.test."+id+".10.1", []byte(fmt.Sprintf(`{"fire_at":%q}`, fire.UTC().Format(time.RFC3339Nano)))); err != nil {
						t.Fatal(err)
					}
				} else {
					if _, err = js.Publish(ctx, "wf.inv.test."+id, []byte("null")); err != nil {
						t.Fatal(err)
					}
					entries := []string{`{"kind":"Started","index":0,"epoch":1,"worker_id":"fixture"}`, `{"kind":"Completed","index":1,"epoch":1,"worker_id":"fixture"}`}
					if seq == 8 {
						entries[1] = fmt.Sprintf(`{"kind":"StepRequested","index":1,"epoch":1,"worker_id":"fixture","payload":{"kind":"timer","fire_at":%q}}`, time.Now().Add(-time.Second).UTC().Format(time.RFC3339Nano))
					}
					for _, entry := range entries {
						if _, err = js.Publish(ctx, "wf.jrn.test."+id, []byte(entry)); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			port := &interruptedRealTimerPort{TimerScanPort: NewTimerScan(js).port, FallbackTimerScanPort: NewFallbackTimerScanPort(js)}
			scan := NewTimerScanWithPort(port).Scan
			if kind == "fallback-timer" {
				s := NewFallbackTimerScan(js)
				s.port = port
				scan = s.Scan
			}
			runCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			var cursors []uint64
			err = runLoop(runCtx, js, "timer-prefix-contract", kind, 10*time.Millisecond, 500, func(attempt context.Context, next uint64, budget int, dry bool) (ScanResult, error) {
				cursors = append(cursors, next)
				port.limit = next + 2
				result, err := scan(attempt, next, budget, dry)
				if port.acknowledged > 0 && err == nil {
					cancel()
				}
				return result, err
			})
			if err != nil {
				t.Fatal(err)
			}
			want := []uint64{1, 4, 7, 8}
			if kind == "fallback-timer" {
				want = append(want, 8)
			}
			if fmt.Sprint(cursors) != fmt.Sprint(want) || port.uncertain != 1 || port.acknowledged != 1 {
				t.Fatalf("cursors=%v want%v enqueue_unknown=%d acknowledged=%d", cursors, want, port.uncertain, port.acknowledged)
			}
			state, err := js.KeyValue(ctx, "WF_STATE")
			if err != nil {
				t.Fatal(err)
			}
			entry, err := state.Get(ctx, "scan."+kind)
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
				t.Fatalf("duplicated/dropped retained wakeup:%+v", info.State)
			}
			raw, err := run.GetMsg(ctx, info.State.FirstSeq)
			if err != nil {
				t.Fatal(err)
			}
			if string(raw.Data) != "test.real-prefix-8" {
				t.Fatalf("wrong repair identity:%q", raw.Data)
			}
			if kind == "fallback-timer" {
				timers, err := js.Stream(ctx, "WF_TIMER")
				if err != nil {
					t.Fatal(err)
				}
				timerInfo, err := timers.Info(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = timers.GetMsg(ctx, 8); !errors.Is(err, jetstream.ErrMsgNotFound) {
					t.Fatalf("due timer remained:%v", err)
				}
				if timerInfo.State.Msgs != 7 || port.deleteUncertain != 1 {
					t.Fatalf("future timers removed or delete fault absent:%+v unknown=%d", timerInfo.State, port.deleteUncertain)
				}
			}
			t.Logf("kind=%s actual_cursor_sequence=%v retained_cursor=%d uncertain_enqueue=1 acknowledged_retry=1 uncertain_delete=%d retained_run_messages=1", kind, cursors, next, port.deleteUncertain)
		})
	}
}
