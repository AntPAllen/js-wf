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
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Reproduce the observable mixed-version retention failure, without modeling
// unknown Raft internals or assuming a server leader move repairs it.
func runSeededWorkQueueRetention(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("workqueue_retention_33"); err != nil {
		return trace, err
	}
	defer func() { trace = schedule.Trace() }()
	ctx := context.Background()
	model := NewDispatchTransport(schedule, 13*time.Second)
	mode, err := schedule.Choose([]string{"ordinary", "held_retention", "held_retention_lost_ack"})
	if err != nil {
		return trace, err
	}
	const count = 33
	held := mode != "ordinary"
	for i := 0; i < count; i++ {
		model.PublishRun("wf.run.32", []byte(fmt.Sprintf("retained-%02d", i)))
		if held {
			if err := model.QueueFault(DispatchFault{Operation: "retention", Kind: "hold_after_ack"}); err != nil {
				return trace, err
			}
		}
	}
	if mode == "held_retention_lost_ack" {
		if err := model.QueueFault(DispatchFault{Operation: "ack", Kind: "lose_ack_after_commit"}); err != nil {
			return trace, err
		}
	}
	if err := model.CommitRetention(1); err == nil {
		return trace, fmt.Errorf("stream removal committed before consumer ack")
	}
	drained := 0
	model.StopWhenDrained(func() { drained++ })
	consumer, err := model.Consumer(ctx, 32)
	if err != nil {
		return trace, err
	}
	messages := map[string]jetstream.Msg{}
	for i := 0; i < count; i++ {
		batch, err := consumer.FetchOne(ctx)
		if err != nil {
			return trace, err
		}
		msg := <-batch.Messages()
		if msg == nil || batch.Error() != nil {
			return trace, fmt.Errorf("missing delivery %d", i)
		}
		messages[fmt.Sprintf("ack_%02d", i)] = msg
	}
	lost := 0
	var last jetstream.Msg
	for len(messages) > 0 {
		enabled := make([]string, 0, len(messages))
		for name := range messages {
			enabled = append(enabled, name)
		}
		chosen, err := schedule.Choose(enabled)
		if err != nil {
			return trace, err
		}
		last = messages[chosen]
		err = last.Ack()
		if errors.Is(err, ErrTransportLost) {
			lost++
		} else if err != nil {
			return trace, err
		}
		delete(messages, chosen)
	}
	if (mode == "held_retention_lost_ack" && lost != 1) || (mode != "held_retention_lost_ack" && lost != 0) {
		return trace, fmt.Errorf("mode=%s lost acknowledgments=%d", mode, lost)
	}
	pending, ackPending, err := consumer.Info(ctx)
	if err != nil || pending != 0 || ackPending != 0 || model.Pending() != 0 {
		return trace, fmt.Errorf("consumer progress: pending=%d ack_pending=%d err=%v", pending, ackPending, err)
	}
	if err := model.Wait(ctx, 30*time.Second); err != nil {
		return trace, err
	}
	if _, err := consumer.FetchOne(ctx); !errors.Is(err, jetstream.ErrNoMessages) {
		return trace, fmt.Errorf("acknowledged record redelivered: %v", err)
	}
	// A duplicate successful ack cannot silently retire the withheld record.
	if err := last.DoubleAck(ctx); err != nil {
		return trace, err
	}
	sequences := model.RetainedSequences()
	drainErr := model.CheckDrained()
	if held {
		if len(sequences) != count || !errors.Is(drainErr, ErrRunQueueRetained) || drained != 0 {
			return trace, fmt.Errorf("retention checker missed consumer-zero/stream-retained failure: retained=%d callback=%d err=%v", len(sequences), drained, drainErr)
		}
		schedule.RecordTransport(TransportEvent{AtMillis: schedule.NowMillis(), Operation: "retention_checker", Outcome: "expected_failure_after_30s"})
		// Explicit transport completions model a delayed removal. This is not an
		// inferred recovery mechanism for the real mixed-version cluster.
		for len(sequences) > 0 {
			enabled := make([]string, 0, len(sequences))
			for _, seq := range sequences {
				enabled = append(enabled, fmt.Sprintf("retire_%02d", seq))
			}
			chosen, err := schedule.Choose(enabled)
			if err != nil {
				return trace, err
			}
			seq, err := strconv.ParseUint(strings.TrimPrefix(chosen, "retire_"), 10, 64)
			if err != nil {
				return trace, err
			}
			if err := model.CommitRetention(seq); err != nil {
				return trace, err
			}
			sequences = model.RetainedSequences()
		}
	} else if len(sequences) != 0 || drainErr != nil {
		return trace, fmt.Errorf("ordinary ack failed to retire records: %v", drainErr)
	}
	if err := model.CheckDrained(); err != nil {
		return trace, err
	}
	if drained != 1 {
		return trace, fmt.Errorf("drain callback=%d want=1", drained)
	}
	if err := model.CommitRetention(1); err != nil || drained != 1 {
		return trace, fmt.Errorf("duplicate retention callback=%d err=%v", drained, err)
	}
	return trace, schedule.Finish()
}

func TestSeededWorkQueueRetentionReplay(t *testing.T) {
	if os.Getenv("SIM_RETENTION_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededWorkQueueRetention(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_RETENTION_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed := range seededSchedules(t) {
		generated, err := runSeededWorkQueueRetention(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "retention-failure.json")
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatal(saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runSeededWorkQueueRetention(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d retention replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("retention-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededWorkQueueRetentionReplay$")
		cmd.Env = append(os.Environ(), "SIM_RETENTION_HELPER=1", "SIM_RETENTION_OUT="+files[i], "FAULT_SEED=42")
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
		t.Fatal("retention trace changed across processes")
	}
}
