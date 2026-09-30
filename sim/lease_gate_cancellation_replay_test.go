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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/lease"
	"js-wf/provision"
)

type heldLeaseUpdatePort struct {
	lease.KVPort
	hold     atomic.Bool
	entered  chan struct{}
	released chan struct{}
	failed   bool
	calls    atomic.Int64
	requests atomic.Int64
}

func (p *heldLeaseUpdatePort) Create(ctx context.Context, key string, data []byte) (uint64, error) {
	p.requests.Add(1)
	return p.KVPort.Create(ctx, key, data)
}
func (p *heldLeaseUpdatePort) Get(ctx context.Context, key string) (lease.KVEntry, error) {
	p.requests.Add(1)
	return p.KVPort.Get(ctx, key)
}
func (p *heldLeaseUpdatePort) Delete(ctx context.Context, key string, revision uint64) error {
	p.requests.Add(1)
	return p.KVPort.Delete(ctx, key, revision)
}

func (p *heldLeaseUpdatePort) Update(ctx context.Context, key string, data []byte, revision uint64) (uint64, error) {
	p.calls.Add(1)
	p.requests.Add(1)
	if p.hold.CompareAndSwap(true, false) {
		close(p.entered)
		<-p.released
		if p.failed {
			return 0, ErrTransportLost
		}
	}
	return p.KVPort.Update(ctx, key, data, revision)
}

func runLeaseGateCancellation(seed int64, replay *Trace) (trace Trace, runErr error) {
	schedule := NewScheduler(seed)
	if replay != nil {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	if err := schedule.SetWorkload("lease_gate_cancellation"); err != nil {
		return trace, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"success", "failed_renew"})
	if err != nil {
		return trace, err
	}
	operation, err := schedule.Choose([]string{"renew", "release", "cleanup"})
	if err != nil {
		return trace, err
	}
	kind, err := schedule.Choose([]string{"cancel", "deadline"})
	if err != nil {
		return trace, err
	}
	kv := NewKVTransport(schedule, provision.LeaseTTL)
	port := &heldLeaseUpdatePort{KVPort: kv, entered: make(chan struct{}), released: make(chan struct{}), failed: mode == "failed_renew"}
	owner, err := lease.NewWithKVPort(port).Acquire(context.Background(), "test", "gate", "owner")
	if err != nil {
		return trace, err
	}
	port.hold.Store(true)
	var once sync.Once
	unblock := func() { once.Do(func() { close(port.released) }) }
	defer unblock()
	leader := make(chan error, 1)
	go func() { leader <- owner.Renew(context.Background()) }()
	select {
	case <-port.entered:
	case <-time.After(time.Second):
		return trace, fmt.Errorf("held lease renewal not entered")
	}
	schedule.RecordTransport(TransportEvent{Operation: "lease_gate_owner", Outcome: "held", AtMillis: schedule.NowMillis()})
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	waiterCtx := &cacheWaitContext{Context: base, waiting: make(chan struct{}), deadline: kind == "deadline"}
	waiter := make(chan error, 1)
	call := func(ctx context.Context) error {
		switch operation {
		case "renew":
			return owner.Renew(ctx)
		case "release":
			return owner.Release(ctx)
		default:
			return owner.Cleanup(ctx)
		}
	}
	go func() { waiter <- call(waiterCtx) }()
	select {
	case <-waiterCtx.waiting:
	case <-time.After(time.Second):
		return trace, fmt.Errorf("lease waiter never entered cancellable wait")
	}
	if err := schedule.AdvanceMillis(1); err != nil {
		return trace, err
	}
	cancel()
	want := context.Canceled
	if kind == "deadline" {
		want = context.DeadlineExceeded
	}
	select {
	case err := <-waiter:
		if !errors.Is(err, want) {
			return trace, fmt.Errorf("lease waiter result: %v want %v", err, want)
		}
	case <-time.After(time.Second):
		return trace, fmt.Errorf("canceled lease waiter remained behind held renewal")
	}
	if port.calls.Load() != 2 || port.requests.Load() != 3 {
		return trace, fmt.Errorf("canceled waiter reached update transport: calls=%d", port.calls.Load())
	}
	schedule.RecordTransport(TransportEvent{Operation: "lease_gate_waiter", Subject: operation, Outcome: kind, AtMillis: schedule.NowMillis()})
	unblock()
	select {
	case err := <-leader:
		if mode == "success" && err != nil || mode == "failed_renew" && !errors.Is(err, lease.ErrLost) {
			return trace, fmt.Errorf("held renewal result: %v", err)
		}
	case <-time.After(time.Second):
		return trace, fmt.Errorf("held renewal did not finish")
	}
	schedule.RecordTransport(TransportEvent{Operation: "lease_gate_owner", Outcome: mode, AtMillis: schedule.NowMillis()})
	if err := call(waiterCtx); !errors.Is(err, want) {
		return trace, fmt.Errorf("already canceled operation result: %v", err)
	}
	err = owner.Renew(context.Background())
	if mode == "success" {
		if err != nil || port.calls.Load() != 3 || port.requests.Load() != 4 {
			return trace, fmt.Errorf("canceled waiter poisoned live lease: %v calls=%d", err, port.calls.Load())
		}
	} else if !errors.Is(err, lease.ErrLost) || port.calls.Load() != 2 || port.requests.Load() != 3 {
		return trace, fmt.Errorf("uncertain renewal regained ownership: %v calls=%d", err, port.calls.Load())
	}
	schedule.RecordTransport(TransportEvent{Operation: "lease_gate_final", Outcome: mode, Sequence: uint64(port.calls.Load()), AtMillis: schedule.NowMillis()})
	return trace, schedule.Finish()
}

func TestSeededLeaseGateCancellationReplay(t *testing.T) {
	if path := os.Getenv("SIM_LEASE_GATE_OUT"); path != "" {
		trace, err := runLeaseGateCancellation(42, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(path); err != nil {
			t.Fatal(err)
		}
		return
	}
	modes := map[string]bool{}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		trace, err := runLeaseGateCancellation(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "lease-gate-failure.json")
			}
			_ = trace.Save(path)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		modes[trace.Decisions[0].Chosen+"/"+trace.Decisions[1].Chosen+"/"+trace.Decisions[2].Chosen] = true
		if seed <= 10 {
			replayed, err := replayTrace(trace)
			if err != nil || !reflect.DeepEqual(trace, replayed) {
				t.Fatalf("seed %d replay: %v", seed, err)
			}
		}
	}
	if len(modes) != 12 {
		t.Fatalf("lease gate schedule coverage: %v", modes)
	}
	var previous []byte
	for i := 0; i < 2; i++ {
		path := filepath.Join(t.TempDir(), "lease-gate.json")
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededLeaseGateCancellationReplay$")
		cmd.Env = append(os.Environ(), "SIM_LEASE_GATE_OUT="+path)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child: %v: %s", err, output)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if i == 1 && !bytes.Equal(data, previous) {
			t.Fatal("lease gate trace changed across processes")
		}
		previous = data
	}
}
