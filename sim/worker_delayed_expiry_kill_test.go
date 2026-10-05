package sim

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/provision"
	"js-wf/worker"
)

// Isolate the observed held-lease/NAK/two-owner-loss sequence. Delayed expiry
// visibility is an assumption: the retained native evidence lacks the last
// renewal and server expiry timestamp. This does not model NATS internals or
// execute the full worker handler/heartbeat goroutines.
func runDelayedExpirySuccessiveKills(seed int64, replay *Trace) (trace Trace, runErr error) {
	s := NewScheduler(seed)
	if replay != nil {
		var err error
		s, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	if err := s.SetWorkload("worker_delayed_expiry_successive_kills"); err != nil {
		return trace, err
	}
	defer func() { trace = s.Trace() }()
	delayChoice, err := s.Choose([]string{"1500", "3000", "5000"})
	if err != nil {
		return trace, err
	}
	costChoice, err := s.Choose([]string{"2600", "2800", "3000"})
	if err != nil {
		return trace, err
	}
	delay, _ := strconv.ParseInt(delayChoice, 10, 64)
	cost, _ := strconv.ParseInt(costChoice, 10, 64)
	ctx := context.Background()
	for _, visibility := range []int64{0, delay} {
		base := s.NowMillis()
		id := fmt.Sprintf("seed-%d-delay-%d", seed, visibility)
		kv := NewKVTransport(s, provision.LeaseTTL)
		if err := kv.SetExpiryVisibilityDelay(time.Duration(visibility) * time.Millisecond); err != nil {
			return trace, err
		}
		leases := lease.NewWithKVPort(kv)
		dispatch := NewDispatchTransport(s, worker.DefaultAckWait)
		dispatch.PublishRun("wf.run.0", []byte("short."+id))
		consumer, err := dispatch.Consumer(ctx, 0)
		if err != nil {
			return trace, err
		}
		transport := NewJournalTransport(s)
		store := journal.NewWithPorts(transport, transport)
		var tail uint64
		var dead []*lease.Lease
		acquired, held := 0, 0
		for s.NowMillis()-base < 60000 {
			batch, err := consumer.FetchOne(ctx)
			if errors.Is(err, jetstream.ErrNoMessages) {
				continue
			}
			if err != nil {
				return trace, err
			}
			msg := <-batch.Messages()
			// Only the first owner's expiry is delayed in this hypothesis. The
			// native timeline has one held delivery; persistent delay would
			// add a second held retry and describes a different observation.
			if visibility > 0 && acquired == 1 && held == 1 {
				if err := kv.ClearExpiryVisibilityDelay(); err != nil {
					return trace, err
				}
			}
			owner, err := leases.Acquire(ctx, "short", id, fmt.Sprintf("owner-%d", acquired))
			if errors.Is(err, lease.ErrHeld) {
				held++
				if err := msg.NakWithDelay(5 * time.Second); err != nil {
					return trace, err
				}
				continue
			}
			if err != nil {
				return trace, err
			}
			acquired++
			if acquired == 1 {
				tail, err = store.Append(ctx, "short", id, journal.Entry{Index: 0, Epoch: owner.Epoch(), Kind: journal.Started, WorkerID: "owner-0"}, 0)
				if err != nil {
					return trace, err
				}
				tail, err = store.Append(ctx, "short", id, journal.Entry{Index: 1, Epoch: owner.Epoch(), Kind: journal.StepRequested, WorkerID: "owner-0"}, tail)
				if err != nil {
					return trace, err
				}
			}
			now := s.NowMillis() - base
			kill := int64(-1)
			if acquired == 1 {
				kill = 1000
			} else if now <= 20500 && now+cost >= 20500 {
				kill = 20500
			}
			if kill >= 0 {
				if err := s.AdvanceMillis(kill - now); err != nil {
					return trace, err
				}
				s.RecordTransport(TransportEvent{AtMillis: s.NowMillis(), Operation: "worker_sigkill", Subject: id, Outcome: "before_heartbeat_no_release_no_ack"})
				dead = append(dead, owner)
				continue
			}
			if err := s.AdvanceMillis(cost); err != nil {
				return trace, err
			}
			tail, err = store.Append(ctx, "short", id, journal.Entry{Index: 2, Epoch: owner.Epoch(), Kind: journal.StepCompleted, WorkerID: fmt.Sprintf("owner-%d", acquired-1)}, tail)
			if err != nil {
				return trace, err
			}
			tail, err = store.Append(ctx, "short", id, journal.Entry{Index: 3, Epoch: owner.Epoch(), Kind: journal.Completed, WorkerID: fmt.Sprintf("owner-%d", acquired-1)}, tail)
			if err != nil {
				return trace, err
			}
			if err := owner.Release(ctx); err != nil {
				return trace, err
			}
			if err := msg.Ack(); err != nil {
				return trace, err
			}
			break
		}
		elapsed := s.NowMillis() - base
		want := worker.DefaultAckWait.Milliseconds() + cost
		wantHeld, wantDeaths := 0, 1
		if visibility > 0 {
			// FetchOne polls on a one-second virtual grid after the 20.5s cut.
			want = 2*worker.DefaultAckWait.Milliseconds() + 5000 + 500 + cost
			wantHeld, wantDeaths = 1, 2
		}
		if elapsed != want || held != wantHeld || len(dead) != wantDeaths || dispatch.Pending() != 0 {
			return trace, fmt.Errorf("delay=%d elapsed=%d want=%d held=%d deaths=%d pending=%d", visibility, elapsed, want, held, len(dead), dispatch.Pending())
		}
		outcome := "within_30s"
		if elapsed >= 30000 {
			outcome = "misses_30s"
		}
		s.RecordTransport(TransportEvent{AtMillis: s.NowMillis(), Operation: "diagnostic_latency_gate", Subject: id, Outcome: fmt.Sprintf("%s_%dms", outcome, elapsed)})
		if (visibility == 0 && elapsed >= 30000) || (visibility > 0 && elapsed < 30000) {
			return trace, fmt.Errorf("incorrect diagnostic clock relationship: visibility=%d elapsed=%d", visibility, elapsed)
		}
		records, finalTail, err := store.Read(ctx, "short", id)
		if err != nil || len(records) != 4 || finalTail != tail {
			return trace, fmt.Errorf("journal final: %d/%d err=%v", len(records), finalTail, err)
		}
		for _, old := range dead {
			if err := old.Renew(ctx); !errors.Is(err, lease.ErrLost) {
				return trace, fmt.Errorf("stale owner renewed: %v", err)
			}
			if _, err := store.Append(ctx, "short", id, journal.Entry{Index: 2, Epoch: old.Epoch(), Kind: journal.StepCompleted}, records[1].Sequence); !errors.Is(err, journal.ErrStale) {
				return trace, fmt.Errorf("stale owner append: %v", err)
			}
		}
		after, afterTail, err := store.Read(ctx, "short", id)
		if err != nil || afterTail != tail || !reflect.DeepEqual(after, records) {
			return trace, fmt.Errorf("stale owner changed journal: %v", err)
		}
		if err := dispatch.CheckDrained(); err != nil {
			return trace, err
		}
	}
	if err := s.Finish(); err != nil {
		return trace, err
	}
	return s.Trace(), nil
}

func TestDelayedExpirySuccessiveWorkerKills(t *testing.T) {
	if path := os.Getenv("SIM_DELAYED_EXPIRY_KILLS_OUT"); path != "" {
		trace, err := runDelayedExpirySuccessiveKills(42, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(path); err != nil {
			t.Fatal(err)
		}
		return
	}
	modes := map[string]bool{}
	for seed := range seededSchedules(t) {
		generated, err := runDelayedExpirySuccessiveKills(seed, nil)
		if err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		modes[generated.Decisions[0].Chosen+"/"+generated.Decisions[1].Chosen] = true
		if seed <= 10 {
			again, err := replayTrace(generated)
			if err != nil || !reflect.DeepEqual(again, generated) {
				t.Fatalf("seed=%d replay=%v", seed, err)
			}
		}
	}
	if len(modes) != 9 {
		t.Fatalf("timing combinations missing: %v", modes)
	}
}

func TestKVExpiryVisibilityDelayRequiresUnusedTransport(t *testing.T) {
	ctx := context.Background()
	kv := NewKVTransport(NewScheduler(1), provision.LeaseTTL)
	for _, delay := range []time.Duration{-time.Millisecond, time.Nanosecond} {
		if err := kv.SetExpiryVisibilityDelay(delay); err == nil {
			t.Fatalf("accepted delay %s", delay)
		}
	}
	if _, err := kv.Create(ctx, "held", []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := kv.ClearExpiryVisibilityDelay(); err == nil {
		t.Fatal("cleared delay with a live key")
	}
	if err := kv.SetExpiryVisibilityDelay(time.Second); err == nil {
		t.Fatal("accepted change after a write")
	}
}
