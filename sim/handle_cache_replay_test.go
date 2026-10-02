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

	"js-wf/internal/handlecache"
)

// Observing Done confirms that Get entered its contended wait before the
// virtual deadline/cancellation is delivered. No host timer drives the model.
type cacheWaitContext struct {
	context.Context
	waiting  chan struct{}
	once     sync.Once
	deadline bool
}

func (c *cacheWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}
func (c *cacheWaitContext) Err() error {
	err := c.Context.Err()
	if err != nil && c.deadline {
		return context.DeadlineExceeded
	}
	return err
}

type cacheResult struct {
	value string
	err   error
}

func runHandleCacheCancellation(seed int64, replay *Trace) (trace Trace, runErr error) {
	schedule := NewScheduler(seed)
	if replay != nil {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	if err := schedule.SetWorkload("handle_cache_cancellation"); err != nil {
		return trace, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"success", "failed_lookup"})
	if err != nil {
		return trace, err
	}
	canceled, err := schedule.Choose([]string{"waiter_0", "waiter_1"})
	if err != nil {
		return trace, err
	}
	kind, err := schedule.Choose([]string{"cancel", "deadline"})
	if err != nil {
		return trace, err
	}
	var cache handlecache.Cache[string]
	var calls atomic.Int64
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	leader := make(chan cacheResult, 1)
	lookupError := errors.New("metadata lookup lost")
	go func() {
		value, err := cache.Get(context.Background(), func(context.Context) (string, error) {
			calls.Add(1)
			close(entered)
			<-release
			if mode == "failed_lookup" {
				return "", lookupError
			}
			return "handle", nil
		})
		leader <- cacheResult{value, err}
	}()
	await := func(ch <-chan struct{}) error {
		select {
		case <-ch:
			return nil
		case <-time.After(time.Second):
			return fmt.Errorf("cache actor did not reach controlled boundary")
		}
	}
	if err := await(entered); err != nil {
		return trace, err
	}
	schedule.RecordTransport(TransportEvent{Operation: "metadata_lookup", Outcome: "held", AtMillis: schedule.NowMillis()})
	results := [2]chan cacheResult{make(chan cacheResult, 1), make(chan cacheResult, 1)}
	contexts := [2]*cacheWaitContext{}
	cancels := [2]context.CancelFunc{}
	for i := range contexts {
		base, cancel := context.WithCancel(context.Background())
		contexts[i] = &cacheWaitContext{Context: base, waiting: make(chan struct{}), deadline: kind == "deadline"}
		cancels[i] = cancel
		defer cancel()
		go func(i int) {
			value, err := cache.Get(contexts[i], func(context.Context) (string, error) { calls.Add(1); return "handle", nil })
			results[i] <- cacheResult{value, err}
		}(i)
	}
	for _, ctx := range contexts {
		if err := await(ctx.waiting); err != nil {
			return trace, err
		}
	}
	victim := 0
	if canceled == "waiter_1" {
		victim = 1
	}
	if err := schedule.AdvanceMillis(1); err != nil {
		return trace, err
	}
	cancels[victim]()
	var result cacheResult
	select {
	case result = <-results[victim]:
	case <-time.After(time.Second):
		return trace, fmt.Errorf("canceled cache waiter remained behind held network lookup")
	}
	wantErr := context.Canceled
	if kind == "deadline" {
		wantErr = context.DeadlineExceeded
	}
	if !errors.Is(result.err, wantErr) || result.value != "" || calls.Load() != 1 {
		return trace, fmt.Errorf("canceled waiter: result=%+v calls=%d", result, calls.Load())
	}
	schedule.RecordTransport(TransportEvent{Operation: "metadata_waiter", Subject: canceled, Outcome: kind, AtMillis: schedule.NowMillis()})
	unblock()
	select {
	case result = <-leader:
	case <-time.After(time.Second):
		return trace, fmt.Errorf("metadata leader stuck")
	}
	if mode == "success" && (result.err != nil || result.value != "handle") || mode == "failed_lookup" && !errors.Is(result.err, lookupError) {
		return trace, fmt.Errorf("leader result: %+v", result)
	}
	schedule.RecordTransport(TransportEvent{Operation: "metadata_lookup", Outcome: mode, AtMillis: schedule.NowMillis()})
	select {
	case result = <-results[1-victim]:
	case <-time.After(time.Second):
		return trace, fmt.Errorf("live cache waiter stuck")
	}
	if result.err != nil || result.value != "handle" {
		return trace, fmt.Errorf("live waiter: %+v", result)
	}
	// A cached handle neither makes another metadata call nor caches the failed
	// first lookup. An already canceled caller cannot observe it as success.
	value, err := cache.Get(context.Background(), func(context.Context) (string, error) { return "", fmt.Errorf("cached handle reloaded") })
	if err != nil || value != "handle" {
		return trace, fmt.Errorf("cached handle: %s %v", value, err)
	}
	wantCalls := int64(1)
	if mode == "failed_lookup" {
		wantCalls = 2
	}
	if calls.Load() != wantCalls {
		return trace, fmt.Errorf("metadata lookup calls=%d want=%d", calls.Load(), wantCalls)
	}
	if _, err := cache.Get(contexts[victim], func(context.Context) (string, error) { return "", nil }); !errors.Is(err, wantErr) {
		return trace, fmt.Errorf("cached canceled caller: %v", err)
	}
	schedule.RecordTransport(TransportEvent{Operation: "metadata_cached", Sequence: uint64(calls.Load()), Outcome: "ok", AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededHandleCacheCancellationReplay(t *testing.T) {
	if path := os.Getenv("SIM_HANDLE_CACHE_OUT"); path != "" {
		trace, err := runHandleCacheCancellation(42, nil)
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
		trace, err := runHandleCacheCancellation(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "handle-cache-failure.json")
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
	if len(modes) != 8 {
		t.Fatalf("cache schedule coverage: %v", modes)
	}
	var previous []byte
	for i := 0; i < 2; i++ {
		path := filepath.Join(t.TempDir(), "cache.json")
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededHandleCacheCancellationReplay$")
		cmd.Env = append(os.Environ(), "SIM_HANDLE_CACHE_OUT="+path)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child: %v: %s", err, output)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if i == 1 && !bytes.Equal(data, previous) {
			t.Fatal("cache trace changed across processes")
		}
		previous = data
	}
}
