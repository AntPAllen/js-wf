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

// This fixture withholds a transport-port response on real R3 stores. It does
// not claim to drop a NATS wire response or reproduce a server-side stall.
type missingContinuationReply struct {
	journal.SnapshotWritePort
	mode     string
	fired    atomic.Bool
	entered  chan error
	deadline time.Time
}

func (p *missingContinuationReply) response(ctx context.Context, operation string, commit func() error) error {
	if !strings.HasPrefix(p.mode, operation+"_") || !p.fired.CompareAndSwap(false, true) {
		return commit()
	}
	deadline, ok := ctx.Deadline()
	p.deadline = deadline
	if !ok || time.Until(deadline) > 15*time.Second {
		err := fmt.Errorf("continuation %s request has delivery lifetime, not a 15s budget", operation)
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
	return ctx.Err()
}
func (p *missingContinuationReply) PutObject(ctx context.Context, name string, raw []byte) error {
	if strings.HasPrefix(name, "snapshot-") {
		return p.response(ctx, "archive", func() error { return p.SnapshotWritePort.PutObject(ctx, name, raw) })
	}
	return p.SnapshotWritePort.PutObject(ctx, name, raw)
}
func (p *missingContinuationReply) CreateManifest(ctx context.Context, key string, raw []byte) error {
	return p.response(ctx, "manifest", func() error { return p.SnapshotWritePort.CreateManifest(ctx, key, raw) })
}
func (p *missingContinuationReply) PurgeJournal(ctx context.Context, subject string, before uint64) error {
	return p.response(ctx, "purge", func() error { return p.SnapshotWritePort.PurgeJournal(ctx, subject, before) })
}
func (p *missingContinuationReply) PurgeSignals(ctx context.Context, subject string, before uint64) error {
	return p.response(ctx, "signal_purge", func() error { return p.SnapshotWritePort.PurgeSignals(ctx, subject, before) })
}
func TestWorkerContinuationMissingReplyRecovery(t *testing.T) {
	for _, mode := range []string{"archive_drop", "archive_ack_lost", "manifest_drop", "manifest_ack_lost", "purge_drop", "purge_ack_lost", "signal_purge_drop", "signal_purge_ack_lost"} {
		t.Run(mode, func(t *testing.T) {
			all, _ := setup(t)
			ctx, stop := context.WithTimeout(context.Background(), 55*time.Second)
			defer stop()
			const typ, id = "continuation-budget", "missing-reply"
			port := &missingContinuationReply{SnapshotWritePort: journal.NewSnapshotPort(all[1]), mode: mode, entered: make(chan error, 1)}
			store := journal.NewWithJetStreamSnapshotPort(all[1], port)
			var effects, heartbeats, timeouts atomic.Int64
			handler := func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
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
			stages := map[string]worker.ContinuationHandler{"next_v1": func(c *wf.Context, input, locals json.RawMessage) (json.RawMessage, error) {
				var total int
				if string(input) != "23" || string(locals) != "45" {
					return nil, fmt.Errorf("continuation inputs")
				}
				if ok, err := c.GetState("total", &total); err != nil || !ok || total != 23 {
					return nil, fmt.Errorf("continuation state=%d err=%v", total, err)
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
			w, err := worker.New(ctx, all[1], "continuation-budget-owner", map[string]worker.Handler{typ: handler}, worker.WithJournalStore(store), worker.WithContinuations(typ, stages), worker.WithPartitionConcurrency(2), worker.WithOperationObserver(func(e worker.OperationEvent) {
				if e.Operation == "lease_renew_heartbeat" && e.Error == "" {
					heartbeats.Add(1)
				}
				if e.Operation == "continuation_publish" && e.Error == context.DeadlineExceeded.Error() {
					timeouts.Add(1)
				}
			}))
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			c := client.New(all[0])
			handle, err := c.Start(ctx, typ, id, []byte(`23`))
			if err != nil {
				t.Fatal(err)
			}
			for i := 1; i <= 2; i++ {
				if _, err := c.Signal(ctx, typ, id, "buffered", []byte(fmt.Sprint(i)), fmt.Sprint(i)); err != nil {
					t.Fatal(err)
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
			prefix, _, err := journal.New(all[2]).Read(ctx, typ, id)
			if err != nil || len(prefix) == 0 {
				t.Fatalf("prefix=%d err=%v", len(prefix), err)
			}
			var frameName string
			for _, r := range prefix {
				if r.Kind == journal.StepCompleted {
					var payload struct {
						Ref string `json:"result_ref"`
					}
					_ = json.Unmarshal(r.Payload, &payload)
					if payload.Ref != "" {
						frameName = payload.Ref
					}
				}
			}
			if frameName == "" {
				t.Fatal("missing durable frame")
			}
			frame, err := port.GetObject(ctx, frameName)
			if err != nil {
				t.Fatal(err)
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
				t.Fatalf("ownership not maintained past TTL: %v", err)
			}
			if heartbeats.Load() < 2 {
				t.Fatalf("heartbeat renewals=%d", heartbeats.Load())
			}
			result, err := c.Await(ctx, typ, id)
			if err != nil || string(result) != "23" {
				t.Fatalf("result=%s err=%v", result, err)
			}
			recovery := time.Since(started)
			if recovery >= 30*time.Second {
				t.Fatalf("publication recovery=%s exceeds 30s", recovery)
			}
			assertFanoutRunDrain(t, ctx, all[2], map[uint32]bool{identity.Partition(typ, id, provision.Partitions): true})
			records, _, err := journal.New(all[2]).Read(ctx, typ, id)
			if err != nil || len(records) < len(prefix) || !reflect.DeepEqual(prefix, records[:len(prefix)]) {
				t.Fatalf("prefix changed: %v", err)
			}
			after, err := port.GetObject(ctx, frameName)
			if err != nil || !bytes.Equal(frame, after) {
				t.Fatalf("frame changed: %v", err)
			}
			if effects.Load() != 2 || timeouts.Load() != 1 {
				t.Fatalf("effects=%d timeouts=%d", effects.Load(), timeouts.Load())
			}
			if records[len(records)-1].Epoch <= prefix[0].Epoch {
				t.Fatal("retry did not advance fencing epoch")
			}
			if _, err := c.Start(ctx, typ, id, []byte(`23`)); !errors.Is(err, client.ErrAlreadyStarted) {
				t.Fatalf("duplicate start=%v", err)
			}
			immutable, err := client.New(all[2]).Await(ctx, typ, id)
			if err != nil || !bytes.Equal(result, immutable) {
				t.Fatalf("immutable result=%s err=%v", immutable, err)
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
			t.Logf("mode=%s recovery=%s inv_seq=%d effects=%d heartbeat_renewals=%d timeouts=%d integrity=%+v", mode, recovery, handle.InvSeq, effects.Load(), heartbeats.Load(), timeouts.Load(), report)
		})
	}
}
