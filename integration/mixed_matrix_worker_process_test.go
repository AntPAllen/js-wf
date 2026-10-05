//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
	"js-wf/internal/natsutil"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/provision"
	"js-wf/testcluster"
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
	optionsConnect := []nats.Option{nats.Name(id), nats.MaxReconnects(-1), nats.ReconnectWait(100 * time.Millisecond), nats.Timeout(time.Second)}
	if os.Getenv("WF_MATRIX_WORKER_PINNED") == "1" {
		optionsConnect = append(optionsConnect, nats.IgnoreDiscoveredServers())
	}
	nc, err := nats.Connect(os.Getenv("WF_MATRIX_WORKER_URLS"), optionsConnect...)
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
	fenceFile, err := os.Create(base + "-fencing.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer fenceFile.Close()
	var mu sync.Mutex
	var observerErr error
	var fenceCount uint64
	fenceEncoder := json.NewEncoder(fenceFile)
	observeFence := func(event worker.FencingEvent) {
		mu.Lock()
		defer mu.Unlock()
		fenceCount++
		err := fenceEncoder.Encode(matrixProcessFencingRecord{PID: os.Getpid(), Sequence: fenceCount, Event: event})
		if err == nil {
			// These are rare fixture observations. Sync each completed record so
			// a later process kill cannot erase an already retained observation.
			err = fenceFile.Sync()
		}
		if err != nil {
			observerErr = err
			stop()
		}
	}
	active := map[string]bool{}
	encoder := json.NewEncoder(events)
	observe := func(event worker.DispatchEvent) {
		mu.Lock()
		defer mu.Unlock()
		if err := encoder.Encode(event); err != nil {
			observerErr = err
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
				observerErr = err
				stop()
				return
			}
			if err := os.Rename(base+"-active.tmp", base+"-active.json"); err != nil {
				observerErr = err
				stop()
			}
		}
		if event.Stage == "lease_acquired" {
			if err := holdMatrixIsolationTarget(ctx, base, event); err != nil {
				observerErr = err
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
	phaseThreeCounter := os.Getenv("WF_PHASE3_COUNTER_CHILD") == "1"
	if phaseThreeCounter {
		handlers = map[string]worker.Handler{"phase3counter": phaseThreeCounterHandler}
	}
	options := []worker.Option{worker.WithPartitionConcurrency(4), worker.WithDispatchObserver(observe), worker.WithFencingObserver(observeFence)}
	if encoding := os.Getenv("WF_MATRIX_WORKER_JOURNAL_ENCODING"); encoding != "" {
		options = append(options, worker.WithJournalEncoding(journal.Encoding(encoding)))
		data, err := json.Marshal(map[string]any{"pid": os.Getpid(), "worker": id, "encoding": encoding})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(base+"-encoding.json", data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if phaseThreeCounter {
		options[0] = worker.WithPartitionConcurrency(1)
	}
	if raw := os.Getenv("WF_MATRIX_WORKER_ACK_WAIT"); raw != "" {
		ackWait, err := time.ParseDuration(raw)
		if err != nil {
			t.Fatalf("worker AckWait: %v", err)
		}
		options = append(options, worker.WithDispatchTiming(ackWait, 3*time.Second))
		t.Logf("experimental worker AckWait=%s heartbeat=3s", ackWait)
	}
	var w *worker.Worker
	if phaseThreeCounter && os.Getenv("WF_PHASE3_REBALANCE_CHILD") == "1" {
		// New deliberately bounds metadata I/O and leaves retry to its caller.
		// Retry only named transport failures inside the existing 10s parent
		// readiness allowance; semantic setup errors still fail immediately.
		ready, stopReady := context.WithTimeout(ctx, 8*time.Second)
		for ready.Err() == nil {
			attempt, stopAttempt := context.WithTimeout(ready, 500*time.Millisecond)
			w, err = worker.New(attempt, js, id, handlers, options...)
			stopAttempt()
			if err == nil {
				break
			}
			if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, jetstream.ErrNoStreamResponse) && !errors.Is(err, nats.ErrNoResponders) && !natsutil.IsUnavailable(err) {
				break
			}
			t.Logf("bounded constructor retry: %v", err)
			select {
			case <-ready.Done():
			case <-time.After(50 * time.Millisecond):
			}
		}
		if w == nil && err == nil {
			err = ready.Err()
		}
		stopReady()
	} else {
		w, err = worker.New(ctx, js, id, handlers, options...)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	var fleet sync.WaitGroup
	failures := make(chan error, provision.Partitions+2)
	if os.Getenv("WF_MATRIX_WORKER_PINNED") == "1" {
		// Ask a live, isolated child for stacks without stopping or reconnecting
		// it. Avoid querying the connection: its mutex might itself be the reason
		// a PING cannot complete.
		dumps := make(chan os.Signal, 1)
		signal.Notify(dumps, syscall.SIGUSR1)
		defer signal.Stop(dumps)
		fleet.Add(1)
		go func() {
			defer fleet.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case <-dumps:
				}
				file, err := os.Create(base + "-goroutines.txt")
				if err == nil {
					err = pprof.Lookup("goroutine").WriteTo(file, 2)
					if closeErr := file.Close(); err == nil {
						err = closeErr
					}
				}
				if err != nil {
					failures <- fmt.Errorf("worker stack capture: %w", err)
					stop()
					return
				}
			}
		}()
		fleet.Add(1)
		go func() {
			defer fleet.Done()
			ticker := time.NewTicker(500 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
				if err := nc.FlushTimeout(time.Second); err != nil {
					continue
				}
				data, _ := json.Marshal(struct {
					At time.Time `json:"at"`
				}{time.Now().UTC()})
				if err := os.WriteFile(base+"-transport.tmp", data, 0600); err != nil {
					failures <- fmt.Errorf("transport marker: %w", err)
					stop()
					return
				}
				if err := os.Rename(base+"-transport.tmp", base+"-transport.json"); err != nil {
					failures <- fmt.Errorf("transport marker: %w", err)
					stop()
					return
				}
			}
		}()
	}
	if os.Getenv("WF_MATRIX_WORKER_CLOCK") == "1" {
		attempt, done := context.WithTimeout(ctx, 2*time.Second)
		clockStream, err := js.Stream(attempt, "MATRIX_CLOCK")
		done()
		if err != nil {
			t.Fatal(err)
		}
		attempt, done = context.WithTimeout(ctx, 2*time.Second)
		err = writeMatrixWorkerClock(attempt, js, clockStream, base, id)
		done()
		if err != nil {
			t.Fatal(err)
		}
		fleet.Add(1)
		go func() {
			defer fleet.Done()
			ticker := time.NewTicker(500 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
				attempt, done := context.WithTimeout(ctx, 2*time.Second)
				err := writeMatrixWorkerClock(attempt, js, clockStream, base, id)
				done()
				if err != nil && ctx.Err() == nil {
					failures <- fmt.Errorf("worker clock proof: %w", err)
					stop()
					return
				}
			}
		}()
	}
	if phaseThreeCounter && os.Getenv("WF_PHASE3_REBALANCE_CHILD") == "1" {
		fleet.Add(1)
		go func() {
			defer fleet.Done()
			if err := w.RunKVAssignments(ctx); err != nil && ctx.Err() == nil {
				failures <- err
				stop()
			}
		}()
	}
	for partition := uint32(0); partition < provision.Partitions; partition++ {
		if phaseThreeCounter && (partition != 0 || os.Getenv("WF_PHASE3_REBALANCE_CHILD") == "1") {
			continue
		}
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
	mu.Lock()
	observedError := observerErr
	observedFences := fenceCount
	mu.Unlock()
	metrics := w.Metrics()
	if metrics.FencingEvents != observedFences {
		t.Fatalf("fencing records=%d counter=%d", observedFences, metrics.FencingEvents)
	}
	data, err := json.MarshalIndent(matrixProcessMetrics{PID: os.Getpid(), Worker: id, Metrics: metrics}, "", "  ")
	if err == nil {
		err = os.WriteFile(base+"-metrics.json", data, 0600)
	}
	if err != nil {
		t.Fatalf("process metrics: %v", err)
	}
	if observedError != nil {
		t.Fatalf("dispatch artifacts: %v", observedError)
	}
	select {
	case err := <-failures:
		t.Fatal(err)
	default:
	}
}

// A final metrics snapshot exists only after graceful exit. A killed process
// may have an incomplete last record; consumers must retain that uncertainty.
type matrixProcessFencingRecord struct {
	PID      int                 `json:"pid"`
	Sequence uint64              `json:"sequence"`
	Event    worker.FencingEvent `json:"event"`
}

type matrixProcessMetrics struct {
	PID     int            `json:"pid"`
	Worker  string         `json:"worker_id"`
	Metrics worker.Metrics `json:"metrics"`
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
	return startMatrixProcessWorkerExecutable(ctx, root, urls, index, generation, executable, false)
}

func startMatrixProcessWorkerExecutable(ctx context.Context, root string, urls []string, index, generation int, executable string, clock bool) (*matrixProcessWorker, error) {
	id := fmt.Sprintf("matrix-process-%d-generation-%d", index, generation)
	base := filepath.Join(root, id)
	if os.Getenv("WF_PHASE3_REBALANCE_CHILD") == "1" {
		id = fmt.Sprintf("phase3-rebalance-%d", index)
	}
	log, err := os.Create(base + ".log")
	if err != nil {
		return nil, err
	}
	child := exec.Command(executable, "-test.run=^TestMixedMatrixWorkerProcessChild$")
	child.Env = append(os.Environ(), "WF_MATRIX_WORKER_CHILD=1", "WF_MATRIX_WORKER_ID="+id, "WF_MATRIX_WORKER_URLS="+strings.Join(urls, ","), "WF_MATRIX_WORKER_BASE="+base)
	if os.Getenv("WF_MATRIX_JOURNAL_ROLLOUT") == matrixProtobufToJSON {
		child.Env = append(child.Env, "WF_MATRIX_WORKER_JOURNAL_ENCODING="+string(matrixRolloutEncoding(generation)))
	}
	if clock {
		child.Env = append(child.Env, "WF_MATRIX_WORKER_CLOCK=1")
	}
	if len(urls) == 1 {
		child.Env = append(child.Env, "WF_MATRIX_WORKER_PINNED=1")
	}
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

// Clock workers have no scheduled process exits. Observe their actual Wait
// result before a later clock check masks the child failure as a stale sample.
// Cleanup cancels this observer before signaling children; the closed exit
// channel still lets stopMatrixProcessWorker recognize an already reaped child.
func waitMatrixProcessWorkerExit(ctx context.Context, process *matrixProcessWorker) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-process.exited:
		if ctx.Err() != nil {
			return ctx.Err()
		}
		logPath := process.base + ".log"
		log, readErr := os.ReadFile(logPath)
		if len(log) > 4096 {
			log = log[len(log)-4096:]
		}
		if err == nil {
			return fmt.Errorf("worker %s exited unexpectedly with success; log=%s read_error=%v tail=%s", process.id, logPath, readErr, log)
		}
		return fmt.Errorf("worker %s exited unexpectedly; log=%s read_error=%v tail=%s: %w", process.id, logPath, readErr, log, err)
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
	event.WorkerKillConfirmed = true
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
func pauseMatrixProcessWorker(ctx context.Context, js jetstream.JetStream, fleet []*matrixProcessWorker, first int, scheduled time.Time) (matrixLeaderFault, error) {
	ready, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	kv, err := js.KeyValue(ready, "WF_LEASE")
	if err != nil {
		return matrixLeaderFault{Scheduled: scheduled, Node: -1}, err
	}
	var process *matrixProcessWorker
	var event matrixLeaderFault
	for ready.Err() == nil {
		index, err := selectMatrixActiveWorker(ready, fleet, first)
		if err != nil {
			return event, err
		}
		process = fleet[index]
		event = matrixLeaderFault{Scheduled: scheduled, Node: -1, WorkerSlot: &index, Worker: process.id, PID: process.cmd.Process.Pid, Killed: time.Now()}
		if err := process.cmd.Process.Signal(syscall.SIGSTOP); err != nil {
			return event, err
		}
		// Every attempted stop must be resumed, including failed confirmation.
		defer process.cmd.Process.Signal(syscall.SIGCONT)
		stopped, stop := context.WithTimeout(ready, time.Second)
		err = waitMatrixProcessState(stopped, event.PID, true)
		stop()
		if err != nil {
			return event, fmt.Errorf("confirm stopped worker: %w", err)
		}
		event.Paused = time.Now()
		keys, err := matrixWorkerActiveLeaseKeys(process.base + "-dispatch.jsonl")
		if err != nil {
			return event, err
		}
		// The marker can lag a completed release. Even the dispatch trace can
		// lag the broker's delete acknowledgement, so check current KV ownership
		// without renewing or changing the stopped worker's lease.
		for _, key := range keys {
			entry, err := kv.Get(ready, key)
			if errors.Is(err, jetstream.ErrKeyNotFound) || errors.Is(err, jetstream.ErrKeyDeleted) {
				continue
			}
			if err != nil {
				return event, err
			}
			var value lease.Value
			if err := json.Unmarshal(entry.Value(), &value); err != nil {
				return event, err
			}
			if value.Worker == process.id && value.Epoch != 0 {
				event.ActiveLeases++
				event.PausedLeases = append(event.PausedLeases, matrixPausedLease{Key: key, Worker: value.Worker, Epoch: value.Epoch, Revision: entry.Revision(), Created: entry.Created(), Observed: time.Now().UTC()})
			}
		}
		if event.ActiveLeases > 0 {
			break
		}
		if err := process.cmd.Process.Signal(syscall.SIGCONT); err != nil {
			return event, err
		}
		if err := waitMatrixProcessState(ready, event.PID, false); err != nil {
			return event, err
		}
		first = (index + 1) % len(fleet)
	}
	if event.ActiveLeases == 0 {
		return event, fmt.Errorf("no retained active lease available to pause: %w", ready.Err())
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
	// Every confirmed pause holds a retained lease and must produce an explicit
	// lost-lease/stale-append outcome after the old process resumes.
	if event.ActiveLeases > 0 {
		fenced, stop := context.WithTimeout(ctx, 10*time.Second)
		defer stop()
		for fenced.Err() == nil {
			after, err := matrixPausedLeaseFencingEvents(process.base+"-fencing.jsonl", event.PID, event.Worker, event.Resumed, event.PausedLeases)
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

// Read complete observer records rather than the asynchronously updated marker.
// A stopped process cannot add records while this snapshot is inspected.
func matrixWorkerActiveLeaseKeys(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	active := make(map[string]bool)
	lines := strings.Split(string(data), "\n")
	for _, line := range lines[:len(lines)-1] {
		if line == "" {
			continue
		}
		var event worker.DispatchEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			return nil, err
		}
		key := identity.Key(event.Type, event.ID)
		switch event.Stage {
		case "lease_acquired":
			active[key] = true
		case "released", "release_initial_error", "release_cleanup_done":
			delete(active, key)
		}
	}
	keys := make([]string, 0, len(active))
	for key := range active {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys, nil
}

// Only an actual fencing record for an observed paused lease can heal the
// pause fault. A release diagnostic or another invocation's loss is insufficient.
func matrixPausedLeaseFencingEvents(path string, pid int, workerID string, since time.Time, leases []matrixPausedLease) (int, error) {
	records, _, err := readTier3ProcessRecords[matrixProcessFencingRecord](path)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	held := map[string]map[uint64]bool{}
	for _, l := range leases {
		if held[l.Key] == nil {
			held[l.Key] = map[uint64]bool{}
		}
		held[l.Key][l.Epoch] = true
	}
	count := 0
	for i, r := range records {
		e := r.Event
		if r.PID != pid || r.Sequence != uint64(i+1) || e.Worker != workerID {
			return 0, fmt.Errorf("paused fencing record identity/sequence mismatch")
		}
		if !e.At.Before(since) && held[identity.Key(e.Type, e.ID)][e.Epoch] {
			count++
		}
	}
	return count, nil
}

func matrixWorkerFencingEvents(path string, since time.Time) (int, error) {
	return matrixWorkerDeliveryFencingEvents(path, since, nil)
}

func matrixWorkerDeliveryFencingEvents(path string, since time.Time, delivery *worker.DispatchEvent) (int, error) {
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
		if delivery != nil && (event.Worker != delivery.Worker || event.Type != delivery.Type || event.ID != delivery.ID || event.RunSequence != delivery.RunSequence || event.Delivery != delivery.Delivery) {
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

type matrixIsolationSelector func(context.Context, []*matrixProcessWorker, int) (int, matrixIsolationTarget, func() error, error)

func isolateMatrixWorkerReplies(ctx context.Context, fleet []*matrixProcessWorker, proxies []*testcluster.ClientProxy, first int, scheduled time.Time, selectors ...matrixIsolationSelector) (matrixLeaderFault, error) {
	selectTarget := matrixIsolationSelector(armMatrixIsolationTarget)
	if len(selectors) > 0 {
		selectTarget = selectors[0]
	}
	index, target, releaseTarget, err := selectTarget(ctx, fleet, first)
	if err != nil {
		return matrixLeaderFault{Scheduled: scheduled, Node: -1}, err
	}
	defer releaseTarget()
	process, proxy := fleet[index], proxies[index]
	event := matrixLeaderFault{Scheduled: scheduled, Node: -1, WorkerSlot: &index, Worker: process.id, PID: process.cmd.Process.Pid, Killed: time.Now(), ActiveLeases: 1, IsolationTarget: &target}
	before := proxy.Stats()
	event.ProxyBefore = &before
	proxy.HoldResponses()
	defer proxy.ResumeResponses()
	// The selected delivery cannot execute or release its acquired lease until
	// the parent has installed the reply hold. Release all acquisition barriers
	// immediately afterward; only the selected proxy remains faulty.
	if err := releaseTarget(); err != nil {
		return event, fmt.Errorf("release isolation acquisition barriers: %w", err)
	}
	// Confirm that replies are held while worker requests still reach the server.
	cut, stop := context.WithTimeout(ctx, 5*time.Second)
	for cut.Err() == nil {
		blocked := proxy.Stats()
		if blocked.ResponsesHeld && blocked.HeldBytes > before.HeldBytes && blocked.ClientToServer > before.ClientToServer {
			event.ProxyBlocked = &blocked
			break
		}
		select {
		case <-cut.Done():
		case <-time.After(20 * time.Millisecond):
		}
	}
	stop()
	if event.ProxyBlocked == nil {
		return event, fmt.Errorf("worker one-way fault not confirmed: before=%+v current=%+v", before, proxy.Stats())
	}
	remaining := time.Until(event.Killed.Add(45 * time.Second))
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return event, ctx.Err()
	case <-timer.C:
	}
	event.Resumed = time.Now()
	proxy.ResumeResponses()
	heal, stop := context.WithTimeout(ctx, 10*time.Second)
	defer stop()
	for heal.Err() == nil {
		data, readErr := os.ReadFile(process.base + "-transport.json")
		if readErr == nil {
			var health struct {
				At time.Time `json:"at"`
			}
			if err := json.Unmarshal(data, &health); err != nil {
				return event, err
			}
			if health.At.After(event.Resumed) {
				healed := proxy.Stats()
				if !healed.ResponsesHeld && healed.ServerToClient > event.ProxyBlocked.ServerToClient {
					event.ProxyHealed = &healed
					event.WorkerPingAt = health.At
					break
				}
			}
		} else if !os.IsNotExist(readErr) {
			return event, readErr
		}
		select {
		case <-heal.Done():
		case <-time.After(20 * time.Millisecond):
		}
	}
	if event.ProxyHealed == nil {
		dumpErr := process.cmd.Process.Signal(syscall.SIGUSR1)
		if dumpErr == nil {
			// Stack capture is diagnostic only and cannot turn this failed gate
			// into success. Give the child a bounded chance to create its file.
			until := time.Now().Add(time.Second)
			for time.Now().Before(until) {
				if _, err := os.Stat(process.base + "-goroutines.txt"); err == nil {
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
		}
		health, readErr := os.ReadFile(process.base + "-transport.json")
		return event, fmt.Errorf("worker PING recovery after reply isolation: %w; proxy=%+v last_success=%s health_read=%v stack_signal=%v", heal.Err(), proxy.Stats(), health, readErr, dumpErr)
	}
	if stats := proxy.Stats(); stats.BufferOverflows != 0 {
		return event, fmt.Errorf("reply-hold buffer overflow invalidates isolation fixture: %+v", stats)
	}
	fences, err := matrixWorkerDeliveryFencingEvents(process.base+"-dispatch.jsonl", event.Killed, &target.Delivery)
	if err != nil {
		return event, err
	}
	event.FencingEvents = fences
	if event.ActiveLeases > 0 && fences == 0 {
		return event, fmt.Errorf("isolated active worker reported no fencing")
	}
	event.Healed = time.Now()
	return event, nil
}

func captureMatrixIsolationDiagnostics(t *testing.T, cluster *testcluster.ProcessCluster, proxies []*testcluster.ClientProxy, root string) {
	t.Helper()
	// Capture kernel send/receive queues before teardown. The NATS pending-byte
	// counter excludes data already accepted by the TCP send buffer.
	sockets, stopSockets := context.WithTimeout(context.Background(), 2*time.Second)
	data, socketErr := exec.CommandContext(sockets, "ss", "-tinp", "dst", "127.0.0.1").CombinedOutput()
	stopSockets()
	if err := os.WriteFile(filepath.Join(root, "sockets.txt"), data, 0600); err != nil {
		t.Logf("socket diagnostic artifact: %v", err)
	}
	if socketErr != nil {
		t.Logf("socket diagnostic: %v", socketErr)
	}
	for index, proxy := range proxies {
		data, err := json.Marshal(proxy.TrafficTrace())
		if err == nil {
			err = os.WriteFile(filepath.Join(root, fmt.Sprintf("proxy-%d-traffic.json", index)), data, 0600)
		}
		if err != nil {
			t.Logf("proxy %d traffic diagnostic: %v", index, err)
		}
	}
	file, err := os.Create(filepath.Join(root, "proxy-goroutines.txt"))
	if err == nil {
		err = pprof.Lookup("goroutine").WriteTo(file, 2)
		if closeErr := file.Close(); err == nil {
			err = closeErr
		}
	}
	if err != nil {
		t.Logf("proxy stack diagnostic: %v", err)
	}
	ctx, stop := context.WithTimeout(context.Background(), 2*time.Second)
	defer stop()
	var captures sync.WaitGroup
	for node := range cluster.Commands {
		for _, kind := range []string{"goroutines", "connections"} {
			captures.Add(1)
			go func() {
				defer captures.Done()
				data, err := cluster.Diagnostic(ctx, node, kind)
				if err == nil {
					err = os.WriteFile(filepath.Join(root, fmt.Sprintf("server-%d-%s.txt", node, kind)), data, 0600)
				}
				if err != nil {
					t.Logf("server %d %s diagnostic: %v", node, kind, err)
				}
			}()
		}
	}
	captures.Wait()
}
