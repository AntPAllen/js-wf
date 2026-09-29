package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/provision"
	"js-wf/wf"
	"js-wf/worker"
)

// WF_LARGE_INPUT_SCALE=1 runs distinct Object Store input uploads through
// live workers. WF_LARGE_INPUT_COUNT can reduce the 1,000-input default.
func TestLargeInputsAcrossLiveWorkers(t *testing.T) {
	if os.Getenv("WF_LARGE_INPUT_SCALE") == "" {
		t.Skip("set WF_LARGE_INPUT_SCALE=1 for the live large-input proof")
	}
	count := 1000
	if value := os.Getenv("WF_LARGE_INPUT_COUNT"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 || parsed > 1000 {
			t.Fatalf("invalid WF_LARGE_INPUT_COUNT %q", value)
		}
		count = parsed
	}
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	const typ = "large-scale"
	const suffixSize = 1024 * 1024
	suffix := strings.Repeat("x", suffixSize)
	var handlerCalls atomic.Int64
	handler := func(_ *wf.Context, input json.RawMessage) (json.RawMessage, error) {
		var value string
		if err := json.Unmarshal(input, &value); err != nil {
			return nil, err
		}
		if len(value) != 6+suffixSize || value[6:] != suffix {
			return nil, fmt.Errorf("large input has wrong length or content: %d", len(value))
		}
		index, err := strconv.Atoi(value[:6])
		if err != nil || index < 0 || index >= count {
			return nil, fmt.Errorf("invalid large input index %q: %v", value[:6], err)
		}
		handlerCalls.Add(1)
		return json.Marshal(index)
	}
	workerCtx, stopWorkers := context.WithCancel(ctx)
	defer stopWorkers()
	type workerResult struct {
		index int
		err   error
	}
	workerDone := make(chan workerResult, 6)
	for index := 0; index < 6; index++ {
		w, err := worker.New(ctx, all[index%len(all)], fmt.Sprintf("large-input-%d", index), map[string]worker.Handler{typ: handler}, worker.WithPartitionConcurrency(16))
		if err != nil {
			t.Fatal(err)
		}
		go func(index int, w *worker.Worker) {
			workerDone <- workerResult{index: index, err: w.RunAssigned(workerCtx, index, 6)}
		}(index, w)
	}
	clients := [3]*client.Client{client.New(all[0]), client.New(all[1]), client.New(all[2])}
	started := time.Now()
	jobs := make(chan int, 64)
	var group sync.WaitGroup
	var firstErr error
	var firstErrorOnce sync.Once
	for caller := 0; caller < 32; caller++ {
		group.Add(1)
		go func(caller int) {
			defer group.Done()
			c := clients[caller%len(clients)]
			for index := range jobs {
				id := fmt.Sprintf("input-%06d", index)
				input := []byte(`"` + fmt.Sprintf("%06d", index) + suffix + `"`)
				if _, err := c.Start(ctx, typ, id, input); err != nil {
					firstErrorOnce.Do(func() { firstErr = fmt.Errorf("start %s: %w", id, err) })
					continue
				}
			}
		}(caller)
	}
	for index := 0; index < count; index++ {
		select {
		case jobs <- index:
		case <-ctx.Done():
			close(jobs)
			group.Wait()
			t.Fatalf("large input producer: %v", ctx.Err())
		}
	}
	close(jobs)
	group.Wait()
	if firstErr != nil {
		t.Fatal(firstErr)
	}
	startDuration := time.Since(started)
	results := make(chan int, 64)
	group = sync.WaitGroup{}
	for caller := 0; caller < 32; caller++ {
		group.Add(1)
		go func(caller int) {
			defer group.Done()
			c := clients[caller%len(clients)]
			for index := range results {
				id := fmt.Sprintf("input-%06d", index)
				result, err := c.Await(ctx, typ, id)
				if err == nil && string(result) == strconv.Itoa(index) {
					continue
				}
				firstErrorOnce.Do(func() { firstErr = fmt.Errorf("result %s: %s, %v", id, result, err) })
				continue
			}
		}(caller)
	}
	for index := 0; index < count; index++ {
		select {
		case results <- index:
		case <-ctx.Done():
			close(results)
			group.Wait()
			t.Fatalf("large input results: %v", ctx.Err())
		}
	}
	close(results)
	group.Wait()
	if firstErr != nil || handlerCalls.Load() != int64(count) {
		t.Fatalf("large inputs: results=%d/%d first_error=%v", handlerCalls.Load(), count, firstErr)
	}
	completionDuration := time.Since(started)
	for _, name := range []string{"WF_INV", "WF_JRN"} {
		stream, err := all[0].Stream(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		info, err := stream.Info(ctx)
		wantMessages := uint64(count)
		if name == "WF_JRN" {
			wantMessages *= 2
		}
		if err != nil || info.State.Msgs != wantMessages || info.State.NumSubjects != uint64(count) {
			t.Fatalf("%s count: info=%+v want=%d err=%v", name, info, wantMessages, err)
		}
	}
	objects, err := all[2].ObjectStore(ctx, "WF_BLOB")
	if err != nil {
		t.Fatal(err)
	}
	objectInfos, err := objects.List(ctx)
	if err != nil || len(objectInfos) != count {
		t.Fatalf("large input objects=%d want=%d err=%v", len(objectInfos), count, err)
	}
	seenObjects := make(map[string]bool, count)
	for _, info := range objectInfos {
		if !strings.HasPrefix(info.Name, "input-") || seenObjects[info.Name] {
			t.Fatalf("unexpected or duplicate input object %q", info.Name)
		}
		seenObjects[info.Name] = true
	}
	run, err := all[0].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	for {
		info, err := run.Info(ctx)
		if err == nil && info.State.Msgs == 0 {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("large input run queue: info=%+v err=%v", info, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	stopWorkers()
	stopStarted := time.Now()
	stopDeadline := time.NewTimer(45 * time.Second)
	defer stopDeadline.Stop()
	stopped := make(map[int]bool)
	for range 6 {
		select {
		case result := <-workerDone:
			stopped[result.index] = true
			if result.err != nil && !errors.Is(result.err, context.Canceled) {
				t.Fatalf("worker %d: %v", result.index, result.err)
			}
		case <-stopDeadline.C:
			t.Fatalf("worker shutdown after %s: stopped=%v", time.Since(stopStarted), stopped)
		}
	}
	t.Logf("large_inputs=%d input_bytes=%d total_input_bytes=%d partitions=%d workers=6 start_time=%s complete_time=%s stop_time=%s handler_calls=%d", count, suffixSize+8, count*(suffixSize+8), provision.Partitions, startDuration, completionDuration, time.Since(stopStarted), handlerCalls.Load())
}
