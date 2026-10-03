package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/provision"
	"js-wf/retention"
	"js-wf/testcluster"
)

type interruptedTombstoneScanPort struct {
	inner                     retention.TombstoneScanPort
	state                     jetstream.KeyValue
	mode                      string
	cursors                   []uint64
	deleteAttempts, committed int
	missingRetried            bool
}
type interruptedTombstoneSession struct {
	retention.TombstoneScanSession
	owner *interruptedTombstoneScanPort
	limit uint64
}

func (p *interruptedTombstoneScanPort) Open(ctx context.Context) (retention.TombstoneScanSession, error) {
	cursor := uint64(1)
	entry, err := p.state.Get(ctx, "scan.tombstone")
	if err == nil {
		cursor, err = strconv.ParseUint(string(entry.Value()), 10, 64)
	} else if errors.Is(err, jetstream.ErrKeyNotFound) {
		err = nil
	}
	if err != nil {
		return nil, err
	}
	session, err := p.inner.Open(ctx)
	if err != nil {
		return nil, err
	}
	p.cursors = append(p.cursors, cursor)
	return interruptedTombstoneSession{TombstoneScanSession: session, owner: p, limit: cursor + 2}, nil
}
func (p interruptedTombstoneSession) GetStateMessage(ctx context.Context, seq uint64) (*jetstream.RawStreamMsg, error) {
	if seq > p.limit {
		return nil, nats.ErrTimeout
	}
	message, err := p.TombstoneScanSession.GetStateMessage(ctx, seq)
	if seq == 8 && errors.Is(err, jetstream.ErrMsgNotFound) {
		p.owner.missingRetried = true
	}
	return message, err
}
func (p interruptedTombstoneSession) DeleteState(ctx context.Context, key string, revision uint64) error {
	if key != "test.prefix-8" || revision != 8 {
		return fmt.Errorf("unexpected delete:%s/%d", key, revision)
	}
	p.owner.deleteAttempts++
	if p.owner.mode == "drop" && p.owner.deleteAttempts == 1 {
		return nats.ErrTimeout
	}
	if err := p.TombstoneScanSession.DeleteState(ctx, key, revision); err != nil {
		return err
	}
	p.owner.committed++
	if p.owner.mode == "lost_ack" && p.owner.deleteAttempts == 1 {
		return nats.ErrTimeout
	}
	return nil
}

type tombstoneContractLoop struct {
	*jetStreamLoopPort
	scan   *interruptedTombstoneScanPort
	cancel context.CancelFunc
}

func (p *tombstoneContractLoop) Wait(ctx context.Context, d time.Duration) error {
	if len(p.scan.cursors) >= 4 {
		if _, err := p.scan.state.Get(ctx, "test.prefix-8"); errors.Is(err, jetstream.ErrKeyNotFound) {
			p.cancel()
		}
	}
	return p.jetStreamLoopPort.Wait(ctx, d)
}
func TestTombstonePartialCursorRepairsRealDropAndLostDeleteAck(t *testing.T) {
	for _, mode := range []string{"drop", "lost_ack"} {
		t.Run(mode, func(t *testing.T) {
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
			state, err := js.KeyValue(ctx, "WF_STATE")
			if err != nil {
				t.Fatal(err)
			}
			for seq := 1; seq <= 8; seq++ {
				value := []byte(`{"inv_seq":1,"result":true}`)
				if seq == 8 {
					now := time.Now().UTC()
					value, _ = json.Marshal(retention.Tombstone{Tombstone: true, InvSeq: 1, PurgedAt: now.Add(-time.Hour), ExpiresAt: now.Add(-time.Minute)})
				}
				revision, err := state.Put(ctx, fmt.Sprintf("test.prefix-%d", seq), value)
				if err != nil {
					t.Fatal(err)
				}
				if revision != uint64(seq) {
					t.Fatalf("unexpected initial revision%d want%d", revision, seq)
				}
			}
			scans := &interruptedTombstoneScanPort{inner: retention.NewTombstoneScanPort(js), state: state, mode: mode}
			runCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			loop := &tombstoneContractLoop{jetStreamLoopPort: &jetStreamLoopPort{js: js, ticker: ticker}, scan: scans, cancel: cancel}
			err = RunTombstoneLoopWithPorts(runCtx, loop, scans, "tombstone-prefix-contract", 10*time.Millisecond, 500, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			if fmt.Sprint(scans.cursors) != "[1 4 7 8]" {
				t.Fatalf("cursor progression=%v", scans.cursors)
			}
			if scans.committed != 1 || (mode == "drop" && scans.deleteAttempts != 2) || (mode == "lost_ack" && (scans.deleteAttempts != 1 || !scans.missingRetried)) {
				t.Fatalf("delete outcome:attempts=%d committed=%d missing_retry=%t", scans.deleteAttempts, scans.committed, scans.missingRetried)
			}
			entry, err := state.Get(ctx, "scan.tombstone")
			if err != nil {
				t.Fatal(err)
			}
			cursor, err := strconv.ParseUint(string(entry.Value()), 10, 64)
			if err != nil || cursor != 11 {
				t.Fatalf("retained cursor=%d want11 err=%v", cursor, err)
			}
			if _, err := state.Get(ctx, "test.prefix-8"); !errors.Is(err, jetstream.ErrKeyNotFound) {
				t.Fatalf("expired tombstone survived:%v", err)
			}
			for seq := 1; seq < 8; seq++ {
				entry, err := state.Get(ctx, fmt.Sprintf("test.prefix-%d", seq))
				if err != nil || string(entry.Value()) != `{"inv_seq":1,"result":true}` {
					t.Fatalf("protected state%d changed:%v", seq, err)
				}
			}
			t.Logf("mode=%s actual_cursor_sequence=%v retained_cursor=%d delete_attempts=%d committed_deletes=%d uncertain_delete_retry_confirmed=%t protected_states=7", mode, scans.cursors, cursor, scans.deleteAttempts, scans.committed, mode == "drop" || scans.missingRetried)
		})
	}
}
