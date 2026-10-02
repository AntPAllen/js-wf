package sim

import (
	"bytes"
	"context"

	"errors"
	"fmt"

	"js-wf/lease"
	"js-wf/provision"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"
)

type heartbeatReusePort struct {
	lease.KVPort
	schedule    *Scheduler
	updates     int
	replyDelay  int64
	clockOffset time.Duration
}

func (p *heartbeatReusePort) Now() time.Time { return p.KVPort.Now().Add(p.clockOffset) }
func (p *heartbeatReusePort) Update(ctx context.Context, key string, value []byte, rev uint64) (uint64, error) {
	p.updates++
	next, err := p.KVPort.Update(ctx, key, value, rev)
	if err == nil && p.replyDelay > 0 {
		delay := p.replyDelay
		p.replyDelay = 0
		if err := p.schedule.AdvanceMillis(delay); err != nil {
			return 0, err
		}
	}
	return next, err
}
func runLeaseHeartbeatReuse(seed int64, replay *Trace) (trace Trace, runErr error) {
	var schedule *Scheduler
	if replay == nil {
		schedule = NewScheduler(seed)
	} else {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	if err := schedule.SetWorkload("lease_heartbeat_reuse"); err != nil {
		return trace, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"busy", "idle_boundary", "delayed_ack", "paused_successor", "lost", "backward_clock", "canceled"})
	if err != nil {
		return trace, err
	}
	ctx := context.Background()
	kv := NewKVTransport(schedule, provision.LeaseTTL)
	port := &heartbeatReusePort{KVPort: kv, schedule: schedule}
	owner, err := lease.NewWithKVPort(port).Acquire(ctx, "test", "heartbeat", "old")
	if err != nil {
		return trace, err
	}
	before := port.updates
	if mode == "lost" {
		if err := owner.Release(ctx); err != nil {
			return trace, err
		}
	}
	if mode == "idle_boundary" {
		if err := schedule.AdvanceMillis(3000); err != nil {
			return trace, err
		}
	}
	if mode == "paused_successor" {
		if err := schedule.AdvanceMillis(provision.LeaseTTL.Milliseconds()); err != nil {
			return trace, err
		}
		if _, err := lease.NewWithKVPort(kv).Acquire(ctx, "test", "heartbeat", "new"); err != nil {
			return trace, err
		}
	}
	if mode == "delayed_ack" {
		port.replyDelay = 10000
		if err := owner.Renew(ctx); err != nil {
			return trace, err
		}
		before = port.updates
	}
	if mode == "backward_clock" {
		port.clockOffset = -time.Second
	}
	if mode == "canceled" {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		ctx = canceled
	}
	updated, err := owner.RenewIfIdle(ctx, 3*time.Second)
	switch mode {
	case "lost":
		if updated || !errors.Is(err, lease.ErrLost) || port.updates != before {
			return trace, fmt.Errorf("lost heartbeat reused ownership: updated=%v err=%v", updated, err)
		}
	case "paused_successor":
		if !updated || !errors.Is(err, lease.ErrLost) || port.updates != before+1 {
			return trace, fmt.Errorf("paused heartbeat skipped fencing: updated=%v err=%v", updated, err)
		}
		entry, getErr := kv.Get(context.Background(), "test.heartbeat")
		if getErr != nil || !bytes.Contains(entry.Value, []byte(`"worker":"new"`)) {
			return trace, fmt.Errorf("heartbeat changed successor: %s %v", entry.Value, getErr)
		}
	case "canceled":
		if updated || !errors.Is(err, context.Canceled) || port.updates != before {
			return trace, fmt.Errorf("canceled heartbeat made transport call: %v %v", updated, err)
		}
	case "busy":
		if updated || err != nil || port.updates != before {
			return trace, fmt.Errorf("redundant heartbeat wrote KV: updated=%v err=%v", updated, err)
		}
		// Append-facing renewal remains unconditional even at the same instant.
		if err := owner.Renew(ctx); err != nil || port.updates != before+1 {
			return trace, fmt.Errorf("append renewal skipped: %v", err)
		}
	default:
		if !updated || err != nil || port.updates != before+1 {
			return trace, fmt.Errorf("required heartbeat skipped: mode=%s updated=%v err=%v", mode, updated, err)
		}
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_heartbeat_reuse", Outcome: mode, AtMillis: schedule.NowMillis()})
	return trace, schedule.Finish()
}
func TestSeededLeaseHeartbeatReuseReplay(t *testing.T) {
	if os.Getenv("SIM_HEARTBEAT_REUSE_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runLeaseHeartbeatReuse(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_HEARTBEAT_REUSE_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	modes := map[string]bool{}
	for seed := range seededSchedules(t) {
		generated, err := runLeaseHeartbeatReuse(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "lease-heartbeat-reuse-failure.json")
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatal(saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		modes[generated.Decisions[0].Chosen] = true
		if seed <= 10 {
			replayed, err := runLeaseHeartbeatReuse(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d lease-heartbeat-reuse replay: %v", seed, err)
			}
		}
	}
	if len(modes) != 7 {
		t.Fatalf("missing heartbeat reuse modes: %v", modes)
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("lease-heartbeat-reuse-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededLeaseHeartbeatReuseReplay$")
		cmd.Env = append(os.Environ(), "SIM_HEARTBEAT_REUSE_HELPER=1", "SIM_HEARTBEAT_REUSE_OUT="+files[i], "FAULT_SEED=42")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child %d: %v: %s", i, err, output)
		}
	}
	first, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(files[1])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("lease-heartbeat-reuse trace changed across processes")
	}
}
