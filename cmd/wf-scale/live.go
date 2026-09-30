package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go/jetstream"
)

type liveConfig struct{ Count, InputBytes int }
type liveValue struct {
	Bytes  int    `json:"bytes"`
	SHA256 string `json:"sha256"`
}
type liveSample struct {
	BackgroundSubjects int              `json:"background_subjects_per_stream"`
	Invocations        int              `json:"invocations"`
	InputBytes         int              `json:"input_bytes"`
	SpilledInputs      int              `json:"spilled_inputs"`
	Effects            int64            `json:"effect_executions"`
	Audit              integrity.Report `json:"live_cohort_audit"`
	P99Seconds         float64          `json:"client_start_to_result_p99_seconds"`
	MaxSeconds         float64          `json:"client_start_to_result_max_seconds"`
	Elapsed            string           `json:"elapsed"`
	Messages           [3][2]uint64     `json:"messages_per_node"`
	Subjects           [3][2]uint64     `json:"subjects_per_node"`
	RSSBytes           [3]uint64        `json:"rss_bytes_per_node"`
}

type liveEvidence struct {
	BackgroundSubjects int                  `json:"background_subjects_per_stream"`
	Scope              string               `json:"scope"`
	Snapshot           integrity.Snapshot   `json:"snapshot"`
	Expected           map[string]liveValue `json:"expected_results"`
}

func liveInput(index, size int) ([]byte, error) {
	value := struct {
		Index   int    `json:"index"`
		Padding string `json:"padding"`
	}{Index: index}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if size < len(raw) {
		return nil, fmt.Errorf("input size %d below JSON overhead %d", size, len(raw))
	}
	value.Padding = strings.Repeat("x", size-len(raw))
	return json.Marshal(value)
}
func inputValue(input []byte) liveValue {
	hash := sha256.Sum256(input)
	return liveValue{Bytes: len(input), SHA256: hex.EncodeToString(hash[:])}
}

// The background subjects are deliberately opaque capacity data. Audit only
// real live invocations; whole-stream counts still include both populations.
func runLiveWorkflows(ctx context.Context, all [3]jetstream.JetStream, root string, background, priorLive, priorEntries int, config liveConfig) (liveSample, error) {
	phase := liveSample{BackgroundSubjects: background, Invocations: config.Count, InputBytes: config.InputBytes}
	if config.Count < 1 || config.InputBytes < 64 || config.InputBytes > 5*1024*1024 {
		return phase, fmt.Errorf("invalid live workload configuration: %+v", config)
	}
	started := time.Now()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	runCtx, stopWorkers := context.WithCancel(ctx)
	var wg sync.WaitGroup
	var workers [3]*worker.Worker
	errorsCh := make(chan error, provision.Partitions)
	defer func() {
		stopWorkers()
		wg.Wait()
		for _, w := range workers {
			if w != nil {
				_ = w.Close()
			}
		}
	}()
	var effects atomic.Int64
	handlers := map[string]worker.Handler{"scalelive": func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
		declared := inputValue(input)
		value, err := wf.Run(c, "hash-input", declared, func(context.Context) (liveValue, error) { effects.Add(1); return inputValue(input), nil })
		if err != nil {
			return nil, err
		}
		return json.Marshal(value)
	}}
	for node := range workers {
		w, err := worker.New(ctx, all[node], fmt.Sprintf("scale-live-node-%d", node), handlers)
		if err != nil {
			return phase, err
		}
		workers[node] = w
	}
	for part := uint32(0); part < provision.Partitions; part++ {
		wg.Add(1)
		go func(part uint32) {
			defer wg.Done()
			if err := workers[part%3].RunPartition(runCtx, part); err != nil {
				errorsCh <- err
				cancel()
			}
		}(part)
	}
	var clients [3]*client.Client
	for node := range clients {
		clients[node] = client.New(all[node])
	}
	type observed struct {
		ID      string
		Handle  client.Handle
		Value   liveValue
		Seconds float64
	}
	results := make([]observed, config.Count)
	jobs := make(chan int)
	var publishers sync.WaitGroup
	var firstErr error
	var once sync.Once
	fail := func(err error) { once.Do(func() { firstErr = err; cancel() }) }
	for publisher := 0; publisher < 16; publisher++ {
		publishers.Add(1)
		go func(publisher int) {
			defer publishers.Done()
			c := clients[publisher%3]
			for index := range jobs {
				input, err := liveInput(index, config.InputBytes)
				if err != nil {
					fail(err)
					return
				}
				id := fmt.Sprintf("checkpoint-%d-%d", background, index)
				expected := inputValue(input)
				began := time.Now()
				handle, err := c.Start(ctx, "scalelive", id, input)
				if err != nil {
					fail(fmt.Errorf("start %s: %w", id, err))
					return
				}
				result, err := c.Await(ctx, "scalelive", id)
				if err != nil {
					fail(fmt.Errorf("await %s: %w", id, err))
					return
				}
				var actual liveValue
				if err := json.Unmarshal(result, &actual); err != nil || actual != expected {
					fail(fmt.Errorf("result %s: got=%+v want=%+v decode=%v", id, actual, expected, err))
					return
				}
				latency := time.Since(began).Seconds()
				// Re-read the immutable result through every pinned peer.
				for node := range clients {
					readCtx, done := context.WithTimeout(ctx, 10*time.Second)
					again, err := clients[node].Await(readCtx, "scalelive", id)
					done()
					if err != nil || !bytes.Equal(again, result) {
						fail(fmt.Errorf("peer %d result %s changed: %v", node, id, err))
						return
					}
				}
				results[index] = observed{ID: id, Handle: handle, Value: expected, Seconds: latency}
			}
		}(publisher)
	}
produce:
	for index := range results {
		select {
		case jobs <- index:
		case <-ctx.Done():
			break produce
		}
	}
	close(jobs)
	publishers.Wait()
	if firstErr != nil {
		return phase, firstErr
	}
	if err := ctx.Err(); err != nil {
		select {
		case err := <-errorsCh:
			return phase, err
		default:
			return phase, err
		}
	}
	run, err := all[0].Stream(ctx, "WF_RUN")
	if err != nil {
		return phase, err
	}
	drainCtx, stopDrain := context.WithTimeout(ctx, 30*time.Second)
	defer stopDrain()
	for {
		attempt, done := context.WithTimeout(drainCtx, 3*time.Second)
		info, err := run.Info(attempt)
		done()
		if err == nil && info.State.Msgs == 0 {
			break
		}
		if drainCtx.Err() != nil {
			return phase, fmt.Errorf("live run queue did not drain: %v", err)
		}
		select {
		case <-time.After(25 * time.Millisecond):
		case <-drainCtx.Done():
		}
	}
	stopWorkers()
	wg.Wait()
	select {
	case err := <-errorsCh:
		return phase, err
	default:
	}
	inv, err := all[0].Stream(ctx, "WF_INV")
	if err != nil {
		return phase, err
	}
	state, err := all[0].KeyValue(ctx, "WF_STATE")
	if err != nil {
		return phase, err
	}
	evidence := liveEvidence{BackgroundSubjects: background, Scope: "live cohort only; background subjects are opaque capacity data", Snapshot: integrity.Snapshot{Journals: map[string][]journal.Record{}, TerminalState: map[string][]byte{}}, Expected: map[string]liveValue{}}
	latencies := make([]float64, len(results))
	for i, result := range results {
		if result.ID == "" {
			return phase, fmt.Errorf("missing workflow %d", i)
		}
		attempt, done := context.WithTimeout(ctx, 10*time.Second)
		message, err := inv.GetMsg(attempt, result.Handle.InvSeq)
		if err != nil {
			done()
			return phase, err
		}
		subject := identity.InvocationSubject("scalelive", result.ID)
		if message.Subject != subject {
			done()
			return phase, fmt.Errorf("invocation identity mismatch for %s", result.ID)
		}
		if message.Header.Get("Wf-Input-SHA256") != result.Value.SHA256 {
			done()
			return phase, fmt.Errorf("retained input hash mismatch for %s", result.ID)
		}
		if message.Header.Get("Wf-Input-Ref") != "" {
			phase.SpilledInputs++
		}
		records, _, err := journal.New(all[0]).Read(attempt, "scalelive", result.ID)
		if err != nil {
			done()
			return phase, err
		}
		value, err := state.Get(attempt, identity.Key("scalelive", result.ID))
		done()
		if err != nil {
			return phase, err
		}
		evidence.Snapshot.Invocations = append(evidence.Snapshot.Invocations, message.Subject)
		evidence.Snapshot.Journals[identity.JournalSubject("scalelive", result.ID)] = records
		evidence.Snapshot.TerminalState[identity.Key("scalelive", result.ID)] = value.Value()
		evidence.Expected[result.ID] = result.Value
		latencies[i] = result.Seconds
	}
	phase.Audit, err = integrity.CheckSnapshot(evidence.Snapshot)
	if err != nil {
		return phase, err
	}
	if phase.Audit != (integrity.Report{Invocations: config.Count, Journals: config.Count, Entries: config.Count * 4, Terminal: config.Count}) {
		return phase, fmt.Errorf("live audit counts=%+v", phase.Audit)
	}
	for node, js := range all {
		for streamIndex, name := range []string{"WF_INV", "WF_JRN"} {
			attempt, done := context.WithTimeout(ctx, 10*time.Second)
			stream, err := js.Stream(attempt, name)
			if err != nil {
				done()
				return phase, err
			}
			info, err := stream.Info(attempt)
			done()
			if err != nil {
				return phase, err
			}
			phase.Messages[node][streamIndex], phase.Subjects[node][streamIndex] = info.State.Msgs, info.State.NumSubjects
			wantSubjects := uint64(background + priorLive + config.Count)
			wantMessages := wantSubjects
			if streamIndex == 1 {
				wantMessages = uint64(background + priorEntries + phase.Audit.Entries)
			}
			if info.Config.Replicas != 3 || info.Config.Storage != jetstream.FileStorage || info.State.Msgs != wantMessages || info.State.NumSubjects != wantSubjects {
				return phase, fmt.Errorf("node %d %s counts/config: msgs=%d/%d subjects=%d/%d", node, name, info.State.Msgs, wantMessages, info.State.NumSubjects, wantSubjects)
			}
		}
	}
	wantSpill := 0
	if config.InputBytes > client.MaxInlineInput {
		wantSpill = config.Count
	}
	if phase.SpilledInputs != wantSpill {
		return phase, fmt.Errorf("spilled inputs=%d want=%d", phase.SpilledInputs, wantSpill)
	}
	phase.Effects = effects.Load()
	if phase.Effects < int64(config.Count) {
		return phase, fmt.Errorf("missing live effects")
	}
	sort.Float64s(latencies)
	phase.P99Seconds = latencies[(len(latencies)*99+99)/100-1]
	phase.MaxSeconds = latencies[len(latencies)-1]
	phase.Elapsed = time.Since(started).String()
	encoded, err := json.Marshal(evidence)
	if err != nil {
		return phase, err
	}
	if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("live-%d-audit.json", background)), encoded, 0644); err != nil {
		return phase, err
	}
	return phase, nil
}
