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

	"js-wf/worker"

	"github.com/nats-io/nats.go/jetstream"
)

func runSeededDispatchScenario(seed int64, replay *Trace) (trace Trace, runErr error) {
	var schedule *Scheduler
	if replay == nil {
		schedule = NewScheduler(seed)
	} else {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	if err := schedule.SetWorkload("dispatch_20"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	model := NewDispatchTransport(schedule, 3*time.Second)
	leaderChoice, err := schedule.Choose([]string{"consumer_leader_change", "fetch_leader_change", "no_leader_change"})
	if err != nil {
		return trace, err
	}
	if leaderChoice == "consumer_leader_change" {
		if err := model.QueueFault(DispatchFault{Operation: "consumer", Kind: "leader_changed"}); err != nil {
			return trace, err
		}
	} else if leaderChoice == "fetch_leader_change" {
		if err := model.QueueFault(DispatchFault{Operation: "fetch", Kind: "leader_changed"}); err != nil {
			return trace, err
		}
	}
	const count = 20
	var deliveryPolicy [count]string
	for i := 0; i < count; i++ {
		choice, err := schedule.Choose([]string{"ack", "redeliver", "progress_then_redeliver"})
		if err != nil {
			return trace, err
		}
		deliveryPolicy[i] = choice
		model.PublishRun("wf.run.0", []byte(fmt.Sprintf("case-%02d", i)))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	acked := make(map[string]bool, count)
	redelivered := 0
	var handleErr error
	handle := func(_ context.Context, msg jetstream.Msg) {
		if handleErr != nil {
			cancel()
			return
		}
		id := string(msg.Data())
		var index int
		if _, err := fmt.Sscanf(id, "case-%02d", &index); err != nil || index < 0 || index >= count {
			handleErr = fmt.Errorf("invalid dispatch data %q: %v", id, err)
			cancel()
			return
		}
		metadata, err := msg.Metadata()
		if err != nil {
			handleErr = err
			cancel()
			return
		}
		if metadata.NumDelivered == 1 && deliveryPolicy[index] != "ack" {
			if deliveryPolicy[index] == "progress_then_redeliver" {
				if err := msg.InProgress(); err != nil {
					handleErr = err
					cancel()
				}
			}
			return
		}
		if metadata.NumDelivered > 1 {
			redelivered++
		}
		if acked[id] {
			handleErr = fmt.Errorf("duplicate acknowledged handler %s", id)
			cancel()
			return
		}
		if err := msg.Ack(); err != nil {
			handleErr = err
			cancel()
			return
		}
		acked[id] = true
		if len(acked) == count {
			cancel()
		}
	}
	if err := worker.RunPartitionWithPort(ctx, 0, model, handle, 1); err != nil || handleErr != nil {
		return trace, fmt.Errorf("dispatch loop: %v handler: %v", err, handleErr)
	}
	if len(acked) != count || model.Pending() != 0 || model.Creates() < 1 {
		return trace, fmt.Errorf("dispatch result: acked=%d pending=%d creates=%d", len(acked), model.Pending(), model.Creates())
	}
	if leaderChoice == "fetch_leader_change" && model.Creates() < 2 {
		return trace, fmt.Errorf("fetch leadership change did not recreate consumer")
	}
	wantedRedeliveries := 0
	for _, policy := range deliveryPolicy {
		if policy != "ack" {
			wantedRedeliveries++
		}
	}
	if redelivered != wantedRedeliveries {
		return trace, fmt.Errorf("redeliveries=%d want %d", redelivered, wantedRedeliveries)
	}
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededDispatchModelReplay(t *testing.T) {
	if os.Getenv("SIM_DISPATCH_TRACE_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededDispatchScenario(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_DISPATCH_TRACE_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed := int64(1); seed <= 1000; seed++ {
		generated, err := runSeededDispatchScenario(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-dispatch-sim-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-dispatch.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runSeededDispatchScenario(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d dispatch trace replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("dispatch-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededDispatchModelReplay$")
		cmd.Env = append(os.Environ(), "SIM_DISPATCH_TRACE_HELPER=1", "SIM_DISPATCH_TRACE_OUT="+files[i], "FAULT_SEED=42")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("seeded dispatch child %d: %v: %s", i, err, output)
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
		t.Fatal("seeded dispatch trace changed across processes")
	}
}

func TestDispatchLostAckCommitted(t *testing.T) {
	schedule := NewScheduler(99)
	model := NewDispatchTransport(schedule, 3*time.Second)
	model.PublishRun("wf.run.0", []byte(`one`))
	if err := model.QueueFault(DispatchFault{Operation: "ack", Kind: "lose_ack_after_commit"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var ackErr error
	err := worker.RunPartitionWithPort(ctx, 0, model, func(_ context.Context, msg jetstream.Msg) {
		ackErr = msg.Ack()
		cancel()
	}, 1)
	if err != nil || !errors.Is(ackErr, ErrTransportLost) || model.Pending() != 0 {
		t.Fatalf("lost committed acknowledgment: loop=%v ack=%v pending=%d", err, ackErr, model.Pending())
	}
}

func TestDispatchDurableRestartAndSharedConsumers(t *testing.T) {
	ctx := context.Background()
	model := NewDispatchTransport(NewScheduler(100), time.Second)
	model.PublishRun("wf.run.0", []byte(`first`))
	before, err := model.Consumer(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	firstBatch, err := before.FetchOne(ctx)
	if err != nil {
		t.Fatal(err)
	}
	first := <-firstBatch.Messages()
	firstMetadata, err := first.Metadata()
	if err != nil || firstMetadata.NumDelivered != 1 {
		t.Fatalf("first delivery=%+v err=%v", firstMetadata, err)
	}
	after, err := model.Consumer(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := model.Wait(ctx, time.Second); err != nil {
		t.Fatal(err)
	}
	secondBatch, err := after.FetchOne(ctx)
	if err != nil {
		t.Fatal(err)
	}
	second := <-secondBatch.Messages()
	secondMetadata, err := second.Metadata()
	if err != nil || secondMetadata.NumDelivered != 2 || secondMetadata.Sequence.Stream != firstMetadata.Sequence.Stream {
		t.Fatalf("durable redelivery=%+v first=%+v err=%v", secondMetadata, firstMetadata, err)
	}
	if err := second.Ack(); err != nil {
		t.Fatal(err)
	}
	model.PublishRun("wf.run.0", []byte(`second`))
	model.PublishRun("wf.run.0", []byte(`third`))
	peer, err := model.Consumer(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	oneBatch, err := after.FetchOne(ctx)
	if err != nil {
		t.Fatal(err)
	}
	twoBatch, err := peer.FetchOne(ctx)
	if err != nil {
		t.Fatal(err)
	}
	one, two := <-oneBatch.Messages(), <-twoBatch.Messages()
	metaOne, errOne := one.Metadata()
	metaTwo, errTwo := two.Metadata()
	if errOne != nil || errTwo != nil || metaOne.Sequence.Stream == metaTwo.Sequence.Stream || metaOne.NumDelivered != 1 || metaTwo.NumDelivered != 1 {
		t.Fatalf("shared consumer deliveries=%+v %+v errors=%v %v", metaOne, metaTwo, errOne, errTwo)
	}
	if err := one.Ack(); err != nil {
		t.Fatal(err)
	}
	if err := two.Ack(); err != nil {
		t.Fatal(err)
	}
	if model.Pending() != 0 || model.Creates() != 3 {
		t.Fatalf("durable state pending=%d creates=%d", model.Pending(), model.Creates())
	}
}
