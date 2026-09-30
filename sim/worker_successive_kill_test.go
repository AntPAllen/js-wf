package sim

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"js-wf/journal"
	"js-wf/lease"
	"js-wf/provision"

	"github.com/nats-io/nats.go/jetstream"
)

// This isolates the delivery/lease sequence observed in the active SIGKILL
// matrix: a delivery's owner dies before its first heartbeat, and the next
// owner can die before completing the two-second effect. It uses production
// lease and journal decisions, but does not simulate OS processes or the full
// worker handler/heartbeat goroutines. The shorter AckWait is a candidate,
// not a production configuration change.
func runSuccessiveWorkerKills(seed int64, replay *Trace) (trace Trace, runErr error) {
	schedule := NewScheduler(seed)
	if replay != nil {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	if err := schedule.SetWorkload("worker_successive_kills"); err != nil {
		return trace, err
	}
	defer func() { trace = schedule.Trace() }()
	secondKill, err := schedule.Choose([]string{"16000", "21000"})
	if err != nil {
		return trace, err
	}
	secondKillMillis, err := strconv.ParseInt(secondKill, 10, 64)
	if err != nil {
		return trace, err
	}
	ctx := context.Background()
	// Seeded effect duration brackets the real row's two-second execution.
	effect, err := schedule.Choose([]string{"1800", "2000", "2200"})
	if err != nil {
		return trace, err
	}
	effectMillis, err := strconv.ParseInt(effect, 10, 64)
	if err != nil {
		return trace, err
	}
	for _, ackWait := range []time.Duration{20 * time.Second, 10 * time.Second} {
		base := schedule.NowMillis()
		id := fmt.Sprintf("seed-%d-ack-%d", seed, ackWait.Milliseconds())
		dispatch := NewDispatchTransport(schedule, ackWait)
		dispatch.PublishRun("wf.run.0", []byte(id))
		consumer, err := dispatch.Consumer(ctx, 0)
		if err != nil {
			return trace, err
		}
		leases := lease.NewWithKVPort(NewKVTransport(schedule, provision.LeaseTTL))
		transport := NewJournalTransport(schedule)
		store := journal.NewWithPorts(transport, transport)
		tail, err := store.Append(ctx, "short", id, journal.Entry{Kind: journal.Started}, 0)
		if err != nil {
			return trace, err
		}
		var dead []*lease.Lease
		acquisitions := 0
		held := 0
		for schedule.NowMillis()-base < 60_000 {
			batch, err := consumer.FetchOne(ctx)
			if errors.Is(err, jetstream.ErrNoMessages) {
				continue
			}
			if err != nil {
				return trace, err
			}
			msg := <-batch.Messages()
			owner, err := leases.Acquire(ctx, "short", id, fmt.Sprintf("worker-%d", acquisitions))
			if errors.Is(err, lease.ErrHeld) {
				held++
				// The production worker's held-lease retry delay is five seconds.
				if err := msg.NakWithDelay(5 * time.Second); err != nil {
					return trace, err
				}
				continue
			}
			if err != nil {
				return trace, err
			}
			acquisitions++
			if acquisitions == 1 {
				tail, err = store.Append(ctx, "short", id, journal.Entry{Index: 1, Epoch: owner.Epoch(), Kind: journal.StepRequested}, tail)
				if err != nil {
					return trace, err
				}
			}
			now := schedule.NowMillis() - base
			killAt := int64(-1)
			if acquisitions == 1 {
				killAt = 1000
			} else if now <= secondKillMillis && now+effectMillis > secondKillMillis {
				killAt = secondKillMillis
			}
			if killAt >= 0 {
				if err := schedule.AdvanceMillis(killAt - now); err != nil {
					return trace, err
				}
				schedule.RecordTransport(TransportEvent{AtMillis: schedule.NowMillis(), Operation: "worker_sigkill", Subject: id, Outcome: "before_heartbeat"})
				dead = append(dead, owner)
				// A hard-killed owner sends neither a lease release nor a NAK.
				continue
			}
			if err := schedule.AdvanceMillis(effectMillis); err != nil {
				return trace, err
			}
			tail, err = store.Append(ctx, "short", id, journal.Entry{Index: 2, Epoch: owner.Epoch(), Kind: journal.StepCompleted}, tail)
			if err != nil {
				return trace, err
			}
			tail, err = store.Append(ctx, "short", id, journal.Entry{Index: 3, Epoch: owner.Epoch(), Kind: journal.Completed}, tail)
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
		elapsed := schedule.NowMillis() - base
		want := int64(40_000) + effectMillis
		wantDeaths, wantHeld := 2, 0
		if ackWait == 10*time.Second {
			want = 15_000 + effectMillis
			wantDeaths, wantHeld = 1, 1
			if secondKillMillis == 16_000 {
				want = 30_000 + effectMillis
				wantDeaths, wantHeld = 2, 2
			}
		} else if secondKillMillis == 16_000 {
			want = 20_000 + effectMillis
			wantDeaths = 1
		}
		if elapsed != want || len(dead) != wantDeaths || held != wantHeld || dispatch.Pending() != 0 {
			return trace, fmt.Errorf("AckWait=%s elapsed=%d want=%d deaths=%d held=%d pending=%d", ackWait, elapsed, want, len(dead), held, dispatch.Pending())
		}
		records, finalTail, err := store.Read(ctx, "short", id)
		if err != nil || len(records) != 4 || finalTail != tail {
			return trace, fmt.Errorf("terminal journal: records=%d tail=%d err=%v", len(records), finalTail, err)
		}
		for _, old := range dead {
			if err := old.Renew(ctx); !errors.Is(err, lease.ErrLost) {
				return trace, fmt.Errorf("killed owner renewed: %v", err)
			}
			if _, err := store.Append(ctx, "short", id, journal.Entry{Index: 2, Epoch: old.Epoch(), Kind: journal.StepCompleted}, records[1].Sequence); !errors.Is(err, journal.ErrStale) {
				return trace, fmt.Errorf("killed owner appended: %v", err)
			}
		}
		after, afterTail, err := store.Read(ctx, "short", id)
		if err != nil || afterTail != tail || !reflect.DeepEqual(after, records) {
			return trace, fmt.Errorf("terminal changed after stale owner: %v", err)
		}
	}
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSuccessiveWorkerKillDeliveryRecovery(t *testing.T) {
	if path := os.Getenv("SIM_SUCCESSIVE_KILLS_OUT"); path != "" {
		generated, err := runSuccessiveWorkerKills(42, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := generated.Save(path); err != nil {
			t.Fatal(err)
		}
		return
	}
	modes := map[string]bool{}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		generated, err := runSuccessiveWorkerKills(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "successive-kills-failure.json")
			}
			_ = generated.Save(path)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		modes[generated.Decisions[0].Chosen+"/"+generated.Decisions[1].Chosen] = true
		if seed <= 10 {
			replayed, err := replayTrace(generated)
			if err != nil || !reflect.DeepEqual(replayed, generated) {
				t.Fatalf("seed %d replay: %v", seed, err)
			}
		}
	}
	if len(modes) != 6 {
		t.Fatalf("effect duration coverage: %v", modes)
	}
	var previous []byte
	for i := 0; i < 2; i++ {
		path := filepath.Join(t.TempDir(), "successive-kills.json")
		cmd := exec.Command(os.Args[0], "-test.run=^TestSuccessiveWorkerKillDeliveryRecovery$")
		cmd.Env = append(os.Environ(), "SIM_SUCCESSIVE_KILLS_OUT="+path)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child %d: %v: %s", i, err, output)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if i == 1 && !bytes.Equal(data, previous) {
			t.Fatal("successive-kill trace changed across processes")
		}
		previous = data
	}
}
