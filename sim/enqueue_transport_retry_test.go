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

	"js-wf/client"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type namedEnqueueFailure struct {
	*StartTransport
	remaining   int
	attempted   int
	afterCommit bool
	failure     error
	ids         []string
}

func (p *namedEnqueueFailure) EnqueueRun(ctx context.Context, subject string, data []byte, id string) error {
	p.attempted++
	p.ids = append(p.ids, id)
	if p.remaining > 0 {
		p.remaining--
		if p.afterCommit {
			if err := p.StartTransport.EnqueueRun(ctx, subject, data, id); err != nil {
				return err
			}
		}
		p.StartTransport.event(TransportEvent{Operation: "enqueue_named_error", Subject: subject, Outcome: p.failure.Error(), DataSHA256: digest([]byte(id))})
		return p.failure
	}
	return p.StartTransport.EnqueueRun(ctx, subject, data, id)
}
func runEnqueueTransportRetry(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("enqueue_transport_retry"); err != nil {
		return trace, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"no_responders_before", "no_stream_before", "timeout_after", "deadline_after", "persistent_before", "persistent_after", "semantic_error"})
	if err != nil {
		return trace, err
	}
	model := NewStartTransport(schedule)
	port := &namedEnqueueFailure{StartTransport: model, remaining: 1, failure: nats.ErrNoResponders}
	switch mode {
	case "no_stream_before":
		port.failure = jetstream.ErrNoStreamResponse
	case "timeout_after":
		port.failure = nats.ErrTimeout
		port.afterCommit = true
	case "deadline_after":
		port.failure = context.DeadlineExceeded
		port.afterCommit = true
	case "persistent_before":
		port.remaining = 80
		port.failure = nats.ErrNoStreamResponse
	case "persistent_after":
		port.remaining = 80
		port.failure = jetstream.ErrNoStreamResponse
		port.afterCommit = true
	case "semantic_error":
		port.failure = errors.Join(&jetstream.APIError{Code: 400, ErrorCode: 10052, Description: "invalid stream configuration"}, nats.ErrNoResponders)
	}
	ctx := context.Background()
	c := client.NewWithStartPort(port)
	handle, startErr := c.Start(ctx, "test", "enqueue-retry", []byte(`null`))
	wantAttempts, wantTime := 2, int64(25)
	persistent := mode == "persistent_before" || mode == "persistent_after"
	if persistent {
		wantAttempts, wantTime = 80, 1975
	}
	if mode == "semantic_error" {
		wantAttempts, wantTime = 1, 0
	}
	if persistent || mode == "semantic_error" {
		if !errors.Is(startErr, client.ErrEnqueueUnknown) || handle.InvSeq == 0 {
			return trace, fmt.Errorf("unknown enqueue incorrectly confirmed: handle=%+v err=%v", handle, startErr)
		}
	} else if startErr != nil || handle.InvSeq == 0 || len(model.Runs()) != 1 {
		return trace, fmt.Errorf("transient enqueue not recovered: handle=%+v runs=%d err=%v", handle, len(model.Runs()), startErr)
	}
	if port.attempted != wantAttempts || schedule.NowMillis() != wantTime {
		return trace, fmt.Errorf("retry bound: attempts=%d want=%d time=%d want=%d", port.attempted, wantAttempts, schedule.NowMillis(), wantTime)
	}
	if persistent {
		wantRuns := 0
		if port.afterCommit {
			wantRuns = 1
		}
		if len(model.Runs()) != wantRuns {
			return trace, fmt.Errorf("unknown retained runs=%d want=%d", len(model.Runs()), wantRuns)
		}
		retried, err := c.Start(ctx, "test", "enqueue-retry", []byte(`null`))
		if !errors.Is(err, client.ErrAlreadyStarted) || retried.InvSeq != handle.InvSeq || len(model.Runs()) != 1 {
			return trace, fmt.Errorf("matching repair changed generation/runs: %+v %v", retried, err)
		}
	}
	wantID := fmt.Sprintf("start:test.enqueue-retry:%d", handle.InvSeq)
	for _, id := range port.ids {
		if id != wantID {
			return trace, fmt.Errorf("retry changed message ID: %q want=%q", id, wantID)
		}
	}
	if len(model.invocations) != 1 {
		return trace, fmt.Errorf("retry changed invocation count")
	}
	return trace, schedule.Finish()
}
func TestSeededEnqueueTransportRetryReplay(t *testing.T) {
	if path := os.Getenv("SIM_ENQUEUE_TRANSPORT_OUT"); path != "" {
		seed := int64(42)
		if value := os.Getenv("SIM_ENQUEUE_TRANSPORT_SEED"); value != "" {
			var err error
			seed, err = strconv.ParseInt(value, 10, 64)
			if err != nil {
				t.Fatal(err)
			}
		}
		trace, err := runEnqueueTransportRetry(seed, nil)
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
		generated, err := runEnqueueTransportRetry(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "enqueue-transport-retry-failure.json")
			}
			_ = generated.Save(path)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		modes[generated.Decisions[0].Chosen] = true
		if seed <= 10 {
			replayed, err := runEnqueueTransportRetry(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d replay: %v", seed, err)
			}
		}
	}
	if len(modes) != 7 {
		t.Fatalf("missing enqueue transport modes: %v", modes)
	}
	var previous []byte
	for i := 0; i < 2; i++ {
		path := filepath.Join(t.TempDir(), "enqueue-transport-retry.json")
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededEnqueueTransportRetryReplay$")
		cmd.Env = append(os.Environ(), "SIM_ENQUEUE_TRANSPORT_OUT="+path)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child %d: %v: %s", i, err, output)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if i == 1 && !bytes.Equal(previous, data) {
			t.Fatal("enqueue transport trace changed across processes")
		}
		previous = data
	}
}

func TestEnqueueWithoutMessageIDPreservesUnknownOutcome(t *testing.T) {
	schedule := NewScheduler(1)
	model := NewStartTransport(schedule)
	port := &namedEnqueueFailure{StartTransport: model, remaining: 1, afterCommit: true, failure: jetstream.ErrNoStreamResponse}
	err := client.NewWithStartPort(port).Enqueue(context.Background(), "test", "unkeyed-wakeup", "")
	if !errors.Is(err, jetstream.ErrNoStreamResponse) || port.attempted != 1 || len(model.Runs()) != 1 || schedule.NowMillis() != 0 {
		t.Fatalf("unkeyed unknown publish retried: attempts=%d runs=%d time=%d err=%v", port.attempted, len(model.Runs()), schedule.NowMillis(), err)
	}
}
