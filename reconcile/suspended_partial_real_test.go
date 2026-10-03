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

type interruptedRealSuspendedPort struct {
	SuspendedScanPort
	limit                   uint64
	uncertain, acknowledged int
}

func (p *interruptedRealSuspendedPort) GetInvocation(ctx context.Context, seq uint64) (*jetstream.RawStreamMsg, error) {
	if seq > p.limit {
		return nil, nats.ErrTimeout
	}
	return p.SuspendedScanPort.GetInvocation(ctx, seq)
}
func (p *interruptedRealSuspendedPort) EnqueueSuspended(ctx context.Context, typ, id string, seq uint64, window int64) error {
	if err := p.SuspendedScanPort.EnqueueSuspended(ctx, typ, id, seq, window); err != nil {
		return err
	}
	if p.uncertain == 0 {
		p.uncertain++
		return nats.ErrTimeout
	}
	p.acknowledged++
	return nil
}
func TestSuspendedPartialCursorRepairsRealGapAfterRepeatedReadTimeoutsAndLostEnqueueAck(t *testing.T) {
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
			for _, entry := range []string{`{"kind":"Started","index":0,"epoch":1,"worker_id":"fixture"}`, `{"kind":"Completed","index":1,"epoch":1,"worker_id":"fixture"}`} {
				if _, err := js.Publish(ctx, fmt.Sprintf("wf.jrn.test.real-prefix-%d", seq), []byte(entry)); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	for _, entry := range []string{`{"kind":"Started","index":0,"epoch":1,"worker_id":"fixture"}`, `{"kind":"Suspended","index":1,"epoch":1,"worker_id":"fixture","payload":{"waiting_on":"continuation:next"}}`} {
		if _, err := js.Publish(ctx, "wf.jrn.test.real-prefix-8", []byte(entry)); err != nil {
			t.Fatal(err)
		}
	}
	port := &interruptedRealSuspendedPort{SuspendedScanPort: NewSuspendedScan(js).port}
	scan := NewSuspendedScanWithPort(port)
	// This contract checks a retry within one production deduplication window.
	scan.Now = func() time.Time { return time.Unix(1700000000, 0).UTC() }
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var cursors []uint64
	err = runLoop(runCtx, js, "prefix-contract", "suspended", 10*time.Millisecond, 500, func(attempt context.Context, next uint64, budget int, dry bool) (ScanResult, error) {
		cursors = append(cursors, next)
		port.limit = next + 2
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
	entry, err := state.Get(ctx, "scan.suspended")
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
