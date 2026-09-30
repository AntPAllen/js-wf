//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/provision"
	"js-wf/wf"
	"js-wf/worker"
)

func TestMixedMatrixWorkerProcessChild(t *testing.T) {
	if os.Getenv("WF_MATRIX_WORKER_CHILD") != "1" {
		t.Skip("matrix worker subprocess helper")
	}
	base := os.Getenv("WF_MATRIX_WORKER_BASE")
	id := os.Getenv("WF_MATRIX_WORKER_ID")
	if base == "" || id == "" {
		t.Fatal("missing child identity or artifact path")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	nc, err := nats.Connect(os.Getenv("WF_MATRIX_WORKER_URLS"), nats.MaxReconnects(-1), nats.ReconnectWait(100*time.Millisecond), nats.Timeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	events, err := os.Create(base + "-dispatch.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer events.Close()
	var mu sync.Mutex
	active := map[string]bool{}
	encoder := json.NewEncoder(events)
	observe := func(event worker.DispatchEvent) {
		mu.Lock()
		defer mu.Unlock()
		if err := encoder.Encode(event); err != nil {
			stop()
			return
		}
		key := event.Type + "." + event.ID
		changed := false
		switch event.Stage {
		case "lease_acquired":
			active[key] = true
			changed = true
		case "released", "release_initial_error", "release_cleanup_done":
			delete(active, key)
			changed = true
		}
		if changed {
			data, _ := json.Marshal(struct {
				Active int `json:"active_leases"`
			}{len(active)})
			if err := os.WriteFile(base+"-active.tmp", data, 0600); err != nil {
				stop()
				return
			}
			if err := os.Rename(base+"-active.tmp", base+"-active.json"); err != nil {
				stop()
			}
		}
	}
	handlers := matrixLeaderHandlers()
	handlers["matrixshort"] = func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		value, err := wf.Run(c, "effect", 0, func(effectCtx context.Context) (int, error) {
			timer := time.NewTimer(2 * time.Second)
			defer timer.Stop()
			select {
			case <-effectCtx.Done():
				return 0, effectCtx.Err()
			case <-timer.C:
				return 42, nil
			}
		})
		if err != nil {
			return nil, err
		}
		return json.Marshal(value)
	}
	options := []worker.Option{worker.WithPartitionConcurrency(4), worker.WithDispatchObserver(observe)}
	if raw := os.Getenv("WF_MATRIX_WORKER_ACK_WAIT"); raw != "" {
		ackWait, err := time.ParseDuration(raw)
		if err != nil {
			t.Fatalf("worker AckWait: %v", err)
		}
		options = append(options, worker.WithDispatchTiming(ackWait, 3*time.Second))
		t.Logf("experimental worker AckWait=%s heartbeat=3s", ackWait)
	}
	w, err := worker.New(ctx, js, id, handlers, options...)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	var fleet sync.WaitGroup
	failures := make(chan error, provision.Partitions)
	for partition := uint32(0); partition < provision.Partitions; partition++ {
		fleet.Add(1)
		go func() {
			defer fleet.Done()
			if err := w.RunPartition(ctx, partition); err != nil && ctx.Err() == nil {
				failures <- err
				stop()
			}
		}()
	}
	if err := os.WriteFile(base+"-ready", []byte(id), 0600); err != nil {
		stop()
		fleet.Wait()
		t.Fatal(err)
	}
	<-ctx.Done()
	fleet.Wait()
	select {
	case err := <-failures:
		t.Fatal(err)
	default:
	}
}

type matrixProcessWorker struct {
	cmd        *exec.Cmd
	exited     chan error
	base       string
	id         string
	generation int
}

func startMatrixProcessWorker(ctx context.Context, root string, urls []string, index, generation int) (*matrixProcessWorker, error) {
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	id := fmt.Sprintf("matrix-process-%d-generation-%d", index, generation)
	base := filepath.Join(root, id)
	log, err := os.Create(base + ".log")
	if err != nil {
		return nil, err
	}
	child := exec.Command(executable, "-test.run=^TestMixedMatrixWorkerProcessChild$")
	child.Env = append(os.Environ(), "WF_MATRIX_WORKER_CHILD=1", "WF_MATRIX_WORKER_ID="+id, "WF_MATRIX_WORKER_URLS="+strings.Join(urls, ","), "WF_MATRIX_WORKER_BASE="+base)
	child.Stdout, child.Stderr = log, log
	if err := child.Start(); err != nil {
		log.Close()
		return nil, err
	}
	log.Close()
	process := &matrixProcessWorker{cmd: child, exited: make(chan error, 1), base: base, id: id, generation: generation}
	go func() { defer close(process.exited); process.exited <- child.Wait() }()
	ready, stop := context.WithTimeout(ctx, 10*time.Second)
	defer stop()
	for ready.Err() == nil {
		if _, err := os.Stat(base + "-ready"); err == nil {
			return process, nil
		}
		select {
		case err := <-process.exited:
			return nil, fmt.Errorf("worker %s exited before readiness: %v; log=%s", id, err, base+".log")
		default:
		}
		time.Sleep(20 * time.Millisecond)
	}
	child.Process.Kill()
	<-process.exited
	return nil, fmt.Errorf("worker %s readiness: %w", id, ready.Err())
}

func stopMatrixProcessWorker(process *matrixProcessWorker) {
	if process == nil {
		return
	}
	select {
	case <-process.exited:
		return
	default:
	}
	_ = process.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-process.exited:
	case <-time.After(5 * time.Second):
		process.cmd.Process.Kill()
		<-process.exited
	}
}

func killMatrixProcessWorker(ctx context.Context, root string, urls []string, fleet []*matrixProcessWorker, index int, scheduled time.Time) (matrixLeaderFault, error) {
	process := fleet[index]
	event := matrixLeaderFault{Scheduled: scheduled, Node: -1, WorkerSlot: &index, Worker: process.id, PID: process.cmd.Process.Pid, Killed: time.Now()}
	var active struct {
		Active int `json:"active_leases"`
	}
	if data, err := os.ReadFile(process.base + "-active.json"); err == nil {
		if err := json.Unmarshal(data, &active); err != nil {
			return event, err
		}
		event.ActiveLeases = active.Active
	}
	if err := process.cmd.Process.Kill(); err != nil {
		return event, err
	}
	fleet[index] = nil
	select {
	case err := <-process.exited:
		exit, ok := err.(*exec.ExitError)
		if !ok {
			return event, fmt.Errorf("worker kill exit=%v", err)
		}
		status, ok := exit.Sys().(syscall.WaitStatus)
		if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
			return event, fmt.Errorf("worker did not exit from SIGKILL: %v", err)
		}
	case <-ctx.Done():
		return event, ctx.Err()
	}
	next, err := startMatrixProcessWorker(ctx, root, urls, index, process.generation+1)
	if err != nil {
		fleet[index] = nil
		return event, err
	}
	fleet[index] = next
	event.Healed = time.Now()
	return event, nil
}

// pauseMatrixProcessWorker keeps the same OS process alive but unable to renew
// or append for 45 seconds. Other members of the fleet serve its partitions.
func pauseMatrixProcessWorker(ctx context.Context, fleet []*matrixProcessWorker, index int, scheduled time.Time) (matrixLeaderFault, error) {
	selected, err := selectMatrixActiveWorker(ctx, fleet, index)
	if err != nil {
		return matrixLeaderFault{Scheduled: scheduled, Node: -1}, err
	}
	index = selected
	process := fleet[index]
	event := matrixLeaderFault{Scheduled: scheduled, Node: -1, WorkerSlot: &index, Worker: process.id, PID: process.cmd.Process.Pid, Killed: time.Now()}
	if err := process.cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		return event, err
	}
	// Resume even if confirmation, artifact reads, or the controller fail.
	defer process.cmd.Process.Signal(syscall.SIGCONT)
	stopped, stop := context.WithTimeout(ctx, time.Second)
	err = waitMatrixProcessState(stopped, event.PID, true)
	stop()
	if err != nil {
		return event, fmt.Errorf("confirm stopped worker: %w", err)
	}
	event.Paused = time.Now()
	var active struct {
		Active int `json:"active_leases"`
	}
	if data, err := os.ReadFile(process.base + "-active.json"); err == nil {
		if err := json.Unmarshal(data, &active); err != nil {
			return event, err
		}
		event.ActiveLeases = active.Active
	} else if !os.IsNotExist(err) {
		return event, err
	}
	if _, err := matrixWorkerFencingEvents(process.base+"-dispatch.jsonl", time.Time{}); err != nil {
		return event, err
	}
	timer := time.NewTimer(45 * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return event, ctx.Err()
	case <-timer.C:
	}
	event.Resumed = time.Now()
	if err := process.cmd.Process.Signal(syscall.SIGCONT); err != nil {
		return event, err
	}
	resumed, stop := context.WithTimeout(ctx, time.Second)
	err = waitMatrixProcessState(resumed, event.PID, false)
	stop()
	if err != nil {
		return event, fmt.Errorf("confirm resumed worker: %w", err)
	}
	// An idle pause has no old lease to fence. An active pause must produce an
	// explicit lost-lease/stale-append outcome after the old process resumes.
	if event.ActiveLeases > 0 {
		fenced, stop := context.WithTimeout(ctx, 10*time.Second)
		defer stop()
		for fenced.Err() == nil {
			after, err := matrixWorkerFencingEvents(process.base+"-dispatch.jsonl", event.Resumed)
			if err != nil {
				return event, err
			}
			if after > 0 {
				event.FencingEvents = after
				break
			}
			select {
			case <-fenced.Done():
			case <-time.After(20 * time.Millisecond):
			}
		}
		if event.FencingEvents == 0 {
			return event, fmt.Errorf("active paused worker did not report fencing after resume: %w", fenced.Err())
		}
	}
	event.Healed = time.Now()
	return event, nil
}

func waitMatrixProcessState(ctx context.Context, pid int, stopped bool) error {
	for ctx.Err() == nil {
		data, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
		if err != nil {
			return err
		}
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "State:") {
				fields := strings.Fields(line)
				if len(fields) < 2 {
					return fmt.Errorf("malformed process state: %q", line)
				}
				state := fields[1]
				if state == "Z" || state == "X" {
					return fmt.Errorf("worker exited: %s", state)
				}
				if (state == "T" || state == "t") == stopped {
					return nil
				}
			}
		}
		select {
		case <-ctx.Done():
		case <-time.After(10 * time.Millisecond):
		}
	}
	return ctx.Err()
}

func matrixWorkerFencingEvents(path string, since time.Time) (int, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	count := 0
	// A running child may be halfway through its final JSONL write. Only complete
	// lines are eligible evidence; invalid completed lines fail the fixture.
	lines := strings.Split(string(data), "\n")
	for _, line := range lines[:len(lines)-1] {
		if line == "" {
			continue
		}
		var event worker.DispatchEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			return 0, err
		}
		if event.At.Before(since) {
			continue
		}
		if strings.Contains(event.Error, lease.ErrLost.Error()) || strings.Contains(event.Error, journal.ErrStale.Error()) {
			count++
		}
	}
	return count, nil
}

// Use the seeded candidate order while requiring an execution to fence. A
// bounded wait catches short idle gaps between mixed batches.
func selectMatrixActiveWorker(ctx context.Context, fleet []*matrixProcessWorker, first int) (int, error) {
	ready, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	for ready.Err() == nil {
		for offset := range fleet {
			index := (first + offset) % len(fleet)
			data, err := os.ReadFile(fleet[index].base + "-active.json")
			if os.IsNotExist(err) {
				continue
			}
			if err != nil {
				return 0, err
			}
			var active struct {
				Active int `json:"active_leases"`
			}
			if err := json.Unmarshal(data, &active); err != nil {
				return 0, err
			}
			if active.Active > 0 {
				return index, nil
			}
		}
		select {
		case <-ready.Done():
		case <-time.After(20 * time.Millisecond):
		}
	}
	return 0, fmt.Errorf("no active worker available to pause: %w", ready.Err())
}
