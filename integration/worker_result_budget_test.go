package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/provision"
	"js-wf/wf"
	"js-wf/worker"
)

// Missing replies are injected at the public result transport boundary on real
// stores. The priming error forces replay of a durably recorded step/frame.
type missingResultReply struct {
	worker.ResultBlobPort
	journal.SnapshotWritePort
	mode          string
	fired, primed atomic.Bool
	reads         atomic.Int64
	entered       chan error
	deadline      time.Time
	name          string
	attempted     []byte
}

func (p *missingResultReply) response(ctx context.Context, name string, data []byte, commit func() error) error {
	p.name, p.attempted = name, bytes.Clone(data)
	deadline, ok := ctx.Deadline()
	p.deadline = deadline
	if !ok || time.Until(deadline) > 15*time.Second {
		err := fmt.Errorf("result object request has delivery lifetime, not a 15s budget")
		p.entered <- err
		return err
	}
	if strings.HasSuffix(p.mode, "ack_lost") {
		if err := commit(); err != nil {
			p.entered <- err
			return err
		}
	}
	p.entered <- nil
	<-ctx.Done()
	// A custom port can return a bare context error. Worker policy must retry it.
	return ctx.Err()
}
func (p *missingResultReply) PutBytes(ctx context.Context, name string, data []byte) error {
	if p.mode == "step_get_drop" && strings.HasPrefix(name, "terminal-result-") && p.primed.CompareAndSwap(false, true) {
		return fmt.Errorf("%w: fixture replay cut", worker.ErrResultBlobUnknown)
	}
	matches := (strings.HasPrefix(p.mode, "frame_put_") || strings.HasPrefix(p.mode, "step_put_")) && strings.HasPrefix(name, "step-result-") || strings.HasPrefix(p.mode, "terminal_put_") && strings.HasPrefix(name, "terminal-result-")
	if matches && p.fired.CompareAndSwap(false, true) {
		return p.response(ctx, name, data, func() error { return p.ResultBlobPort.PutBytes(ctx, name, data) })
	}
	return p.ResultBlobPort.PutBytes(ctx, name, data)
}
func (p *missingResultReply) GetBytes(ctx context.Context, name string) ([]byte, error) {
	reads := p.reads.Add(1)
	matches := p.mode == "frame_verify_drop" || p.mode == "step_get_drop" || p.mode == "frame_replay_drop" && reads == 2
	if matches && p.fired.CompareAndSwap(false, true) {
		return nil, p.response(ctx, name, nil, func() error { return nil })
	}
	return p.ResultBlobPort.GetBytes(ctx, name)
}
func (p *missingResultReply) PutObject(ctx context.Context, name string, data []byte) error {
	if p.mode == "frame_replay_drop" && strings.HasPrefix(name, "snapshot-") && p.primed.CompareAndSwap(false, true) {
		return errors.New("fixture archive replay cut")
	}
	return p.SnapshotWritePort.PutObject(ctx, name, data)
}
func TestWorkerResultMissingReplyRecovery(t *testing.T) {
	for _, mode := range []string{"frame_put_drop", "frame_put_ack_lost", "frame_verify_drop", "frame_replay_drop", "step_put_drop", "step_put_ack_lost", "step_get_drop", "terminal_put_drop", "terminal_put_ack_lost"} {
		t.Run(mode, func(t *testing.T) {
			all, _ := setup(t)
			ctx, stop := context.WithTimeout(context.Background(), 55*time.Second)
			defer stop()
			const typ, id = "result-budget", "missing-reply"
			port := &missingResultReply{ResultBlobPort: worker.NewResultBlobPort(all[1]), SnapshotWritePort: journal.NewSnapshotPort(all[1]), mode: mode, entered: make(chan error, 1)}
			frameMode := strings.HasPrefix(mode, "frame_")
			large := strings.Repeat("x", wf.MaxInlineResult)
			largeResult, _ := json.Marshal(large)
			var effects, heartbeats, timeouts atomic.Int64
			handler := func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
				if frameMode {
					if err := c.SetState("total", 23); err != nil {
						return nil, err
					}
					if _, err := wf.RunOnce(c, "prefix", 23, func(context.Context, string) (int, error) { effects.Add(1); return 46, nil }); err != nil {
						return nil, err
					}
					v, err := wf.AwaitSignal(c, "buffered")
					if err != nil {
						return nil, err
					}
					if string(v) != "1" {
						return nil, fmt.Errorf("prefix signal=%s", v)
					}
					return nil, wf.Continue(c, "next_v1", 45)
				}
				v, err := wf.RunOnce(c, "large", 1, func(context.Context, string) (string, error) { effects.Add(1); return large, nil })
				if err != nil {
					return nil, err
				}
				return json.Marshal(v)
			}
			stages := map[string]worker.ContinuationHandler{"next_v1": func(c *wf.Context, input, locals json.RawMessage) (json.RawMessage, error) {
				var total int
				if string(input) != "23" || string(locals) != "45" {
					return nil, fmt.Errorf("frame inputs")
				}
				if ok, err := c.GetState("total", &total); err != nil || !ok || total != 23 {
					return nil, fmt.Errorf("frame state=%d err=%v", total, err)
				}
				v, err := wf.AwaitSignal(c, "buffered")
				if err != nil {
					return nil, err
				}
				if string(v) != "2" {
					return nil, fmt.Errorf("suffix signal=%s", v)
				}
				if _, err := wf.RunOnce(c, "suffix", 23, func(context.Context, string) (int, error) { effects.Add(1); return 46, nil }); err != nil {
					return nil, err
				}
				return json.RawMessage(`23`), nil
			}}
			opts := []worker.Option{worker.WithResultBlobPort(port), worker.WithPartitionConcurrency(2), worker.WithOperationObserver(func(e worker.OperationEvent) {
				if e.Operation == "lease_renew_heartbeat" && e.Error == "" {
					heartbeats.Add(1)
				}
				if (e.Operation == "result_blob_put" || e.Operation == "result_blob_get") && strings.Contains(e.Error, context.DeadlineExceeded.Error()) {
					timeouts.Add(1)
				}
			})}
			if frameMode {
				opts = append(opts, worker.WithJournalStore(journal.NewWithJetStreamSnapshotPort(all[1], port)), worker.WithContinuations(typ, stages))
			}
			w, err := worker.New(ctx, all[1], "result-budget-owner", map[string]worker.Handler{typ: handler}, opts...)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			c := client.New(all[0])
			handle, err := c.Start(ctx, typ, id, []byte(`23`))
			if err != nil {
				t.Fatal(err)
			}
			if frameMode {
				for i := 1; i <= 2; i++ {
					if _, err := c.Signal(ctx, typ, id, "buffered", []byte(fmt.Sprint(i)), fmt.Sprint(i)); err != nil {
						t.Fatal(err)
					}
				}
			}
			runCtx, stopRun := context.WithCancel(ctx)
			done := make(chan error, 1)
			go func() { done <- w.RunPartition(runCtx, identity.Partition(typ, id, provision.Partitions)) }()
			defer func() {
				stopRun()
				if err := <-done; err != nil {
					t.Error(err)
				}
			}()
			started := time.Now()
			select {
			case err := <-port.entered:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			stalled := time.Now()
			prefix, _, err := journal.New(all[2]).Read(ctx, typ, id)
			if err != nil || len(prefix) == 0 {
				t.Fatalf("prefix=%d err=%v", len(prefix), err)
			}
			state, err := all[2].KeyValue(ctx, "WF_STATE")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := state.Get(ctx, identity.Key(typ, id)); !errors.Is(err, jetstream.ErrKeyNotFound) {
				t.Fatalf("premature outcome: %v", err)
			}
			if strings.HasSuffix(mode, "ack_lost") {
				raw, err := port.ResultBlobPort.GetBytes(ctx, port.name)
				if err != nil || !bytes.Equal(raw, port.attempted) {
					t.Fatalf("hidden committed object=%v", err)
				}
			}
			if mode == "frame_put_drop" || mode == "step_put_drop" || mode == "terminal_put_drop" {
				if _, err := port.ResultBlobPort.GetBytes(ctx, port.name); !errors.Is(err, jetstream.ErrObjectNotFound) {
					t.Fatalf("dropped write unexpectedly retained: %v", err)
				}
			}
			var confirmedFrameName string
			var confirmedFrame []byte
			if mode == "frame_replay_drop" {
				confirmedFrameName = port.name
				confirmedFrame, err = port.ResultBlobPort.GetBytes(ctx, port.name)
				if err != nil {
					t.Fatal(err)
				}
			}
			timer := time.NewTimer(time.Until(port.deadline.Add(-2 * time.Second)))
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			leasing, err := lease.New(ctx, all[2])
			if err != nil {
				t.Fatal(err)
			}
			if unexpected, err := leasing.Acquire(ctx, typ, id, "competitor"); !errors.Is(err, lease.ErrHeld) {
				if err == nil {
					_ = unexpected.Release(ctx)
				}
				t.Fatalf("owner not fenced past TTL: %v", err)
			}
			if heartbeats.Load() < 2 {
				t.Fatalf("heartbeat renewals=%d", heartbeats.Load())
			}
			result, err := c.Await(ctx, typ, id)
			expected := largeResult
			if frameMode {
				expected = []byte(`23`)
			}
			if err != nil || !bytes.Equal(result, expected) {
				t.Fatalf("result bytes=%d expected=%d err=%v", len(result), len(expected), err)
			}
			recovery := time.Since(started)
			if recovery >= 30*time.Second {
				t.Fatalf("raw recovery=%s exceeds 30s", recovery)
			}
			assertFanoutRunDrain(t, ctx, all[2], map[uint32]bool{identity.Partition(typ, id, provision.Partitions): true})
			records, _, err := journal.New(all[2]).Read(ctx, typ, id)
			if err != nil || len(records) < len(prefix) || !reflect.DeepEqual(prefix, records[:len(prefix)]) {
				t.Fatalf("prefix changed: %v", err)
			}
			if confirmedFrameName != "" {
				raw, err := port.ResultBlobPort.GetBytes(ctx, confirmedFrameName)
				if err != nil || !bytes.Equal(raw, confirmedFrame) {
					t.Fatalf("confirmed frame changed: %v", err)
				}
			}
			wantEffects := int64(1)
			if frameMode || strings.HasPrefix(mode, "step_put_") {
				wantEffects = 2
			}
			if effects.Load() != wantEffects || timeouts.Load() != 1 {
				t.Fatalf("effects=%d want=%d timeouts=%d", effects.Load(), wantEffects, timeouts.Load())
			}
			if records[len(records)-1].Epoch <= prefix[0].Epoch {
				t.Fatal("retry did not advance epoch")
			}
			if _, err := c.Start(ctx, typ, id, []byte(`23`)); !errors.Is(err, client.ErrAlreadyStarted) {
				t.Fatalf("duplicate start=%v", err)
			}
			immutable, err := client.New(all[2]).Await(ctx, typ, id)
			if err != nil || !bytes.Equal(immutable, result) {
				t.Fatalf("immutable result=%v", err)
			}
			leases, err := all[2].KeyValue(ctx, "WF_LEASE")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := leases.Get(ctx, identity.Key(typ, id)); !errors.Is(err, jetstream.ErrKeyNotFound) {
				t.Fatalf("retained lease: %v", err)
			}
			report, err := integrity.Check(ctx, all[2])
			if err != nil || report.Invocations != 1 || report.Terminal != 1 {
				t.Fatalf("integrity=%+v err=%v", report, err)
			}
			t.Logf("mode=%s raw_recovery=%s request_to_terminal=%s inv_seq=%d result_bytes=%d effects=%d heartbeat_renewals=%d timeouts=%d integrity=%+v", mode, recovery, time.Since(stalled), handle.InvSeq, len(result), effects.Load(), heartbeats.Load(), timeouts.Load(), report)
		})
	}
}
