//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go"
	"js-wf/internal/natsutil"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/reconcile"
)

// Persist the real Start record while withholding every dispatch publication.
// No test deletes a committed dispatch to manufacture the missing-start gap.
func admitMixedMissingStart(t *testing.T, ctx context.Context, js jetstream.JetStream) client.Handle {
	t.Helper()
	dropped := &droppedStartEnqueueJS{JetStream: js}
	dropped.blocked.Store(true)
	h, err := client.New(dropped).Start(ctx, "guardshort", "repair-orphan", []byte(`0`))
	if !errors.Is(err, client.ErrEnqueueUnknown) || h.InvSeq == 0 || dropped.attempts.Load() < 2 {
		t.Fatalf("missing dispatch admission handle=%+v err=%v attempts=%d", h, err, dropped.attempts.Load())
	}
	verifyMixedStartRepairOutcome(t, ctx, js, h, false)
	t.Logf("MIXED_START_GAP_ADMISSION invocation_sequence=%d enqueue_attempts=%d journal=absent terminal=absent", h.InvSeq, dropped.attempts.Load())
	return h
}

func challengeMixedStartRepair(t *testing.T, ctx context.Context, js jetstream.JetStream, h client.Handle) bool {
	t.Helper()
	verifyMixedStartRepairOutcome(t, ctx, js, h, false)
	inv, err := js.Stream(ctx, "WF_INV")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := inv.GetMsg(ctx, h.InvSeq)
	if err != nil || raw.Subject != identity.InvocationSubject(h.Type, h.ID) || string(raw.Data) != "0" {
		t.Fatalf("retained start source %+v err=%v", raw, err)
	}
	proof, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("MIXED_START_RAW_INVOCATION %s", proof)
	run, err := js.Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	before, err := run.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	result, err := reconcile.NewStartScan(js).Scan(ctx, h.InvSeq, 1, false)
	if err != nil {
		t.Fatal("unrelated start scanner error", err)
	}
	after, err := run.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	retained := 0
	for seq := before.State.LastSeq + 1; seq <= after.State.LastSeq; seq++ {
		msg, err := run.GetMsg(ctx, seq)
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if string(msg.Data) != identity.Key(h.Type, h.ID) {
			continue
		}
		retained++
		proof, err := json.Marshal(msg)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("MIXED_START_RAW_DISPATCH %s", proof)
	}
	if result.Inspected == 1 && result.Reenqueued == 1 && retained == 1 {
		t.Log("MIXED_START_SCAN inspected=1 reenqueued=1 retained_dispatch=1")
		return true
	}
	if result.Inspected != 0 || result.Reenqueued != 0 || retained != 0 {
		t.Fatalf("unrelated scanner outcome %+v retained=%d", result, retained)
	}
	t.Log("MIXED_START_SCAN inspected=0 reenqueued=0 retained_dispatch=0")
	return false
}

func verifyMixedStartRepairOutcome(t *testing.T, ctx context.Context, js jetstream.JetStream, h client.Handle, repaired bool) {
	t.Helper()
	var err error
	for until := time.Now().Add(30 * time.Second); time.Now().Before(until) && ctx.Err() == nil; {
		attempt, stop := context.WithTimeout(ctx, 3*time.Second)
		err = readMixedStartRepairOutcome(attempt, js, h, repaired)
		stop()
		if err == nil {
			return
		}
		if !(errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrTimeout) || errors.Is(err, nats.ErrNoResponders) || errors.Is(err, jetstream.ErrNoStreamResponse) || natsutil.IsUnavailable(err)) {
			t.Fatal(err)
		}
		t.Logf("MIXED_START_READ_RETRY err=%v", err)
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("start gap verification did not recover: %v", err)
}

func readMixedStartRepairOutcome(ctx context.Context, js jetstream.JetStream, h client.Handle, repaired bool) error {
	records, _, err := journal.New(js).Read(ctx, h.Type, h.ID)
	if err != nil {
		return fmt.Errorf("orphan journal read: %w", err)
	}
	if repaired {
		if len(records) != 4 || records[0].Kind != journal.Started || records[3].Kind != journal.Completed {
			return fmt.Errorf("repaired start journal: %+v", records)
		}
		return nil
	}
	if len(records) != 0 {
		return fmt.Errorf("orphan unexpectedly journaled: %+v", records)
	}
	state, err := js.KeyValue(ctx, "WF_STATE")
	if err != nil {
		return fmt.Errorf("orphan state bucket: %w", err)
	}
	_, err = state.Get(ctx, identity.Key(h.Type, h.ID))
	if !errors.Is(err, jetstream.ErrKeyNotFound) {
		if err != nil {
			return fmt.Errorf("orphan state read: %w", err)
		}
		return fmt.Errorf("orphan unexpectedly has state")
	}
	run, err := js.Stream(ctx, "WF_RUN")
	if err != nil {
		return fmt.Errorf("orphan run stream: %w", err)
	}
	info, err := run.Info(ctx)
	if err != nil {
		return fmt.Errorf("orphan run snapshot: %w", err)
	}
	for seq := uint64(1); seq <= info.State.LastSeq; seq++ {
		msg, err := run.GetMsg(ctx, seq)
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			continue
		}
		if err != nil {
			return fmt.Errorf("orphan run sequence %d: %w", seq, err)
		}
		if string(msg.Data) == identity.Key(h.Type, h.ID) {
			return fmt.Errorf("orphan unexpectedly dispatched: %+v", msg)
		}
	}
	return nil
}
