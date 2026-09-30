package sim

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"js-wf/assignment"
	"js-wf/provision"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"
)

// Manual owner lists cannot detect a dead process. Model a retained terminal
// redelivery while its last assigned worker has disappeared.
func runSeededDeadOwnerDrain(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("manual_owner_drain"); err != nil {
		return trace, err
	}
	defer func() { trace = schedule.Trace() }()
	ctx := context.Background()
	assignments := modeledAssignments{NewKVTransport(schedule, 0)}
	for p := uint32(0); p < provision.Partitions; p++ {
		if _, err := assignments.Assign(ctx, p, "dead", 0); err != nil {
			return trace, err
		}
	}
	// Keeping the dead process in a manual membership list preserves half its
	// partitions. This is an expected failing control, not an automatic takeover.
	if _, err := assignment.Rebalance(ctx, assignments, []string{"dead", "live"}, false); err != nil {
		return trace, err
	}
	choices := make([]string, 32)
	for p := range choices {
		choices[p] = strconv.Itoa(p)
	}
	chosen, err := schedule.Choose(choices)
	if err != nil {
		return trace, err
	}
	partition, err := strconv.ParseUint(chosen, 10, 32)
	if err != nil {
		return trace, err
	}
	queue := NewDispatchTransport(schedule, 13*time.Second)
	queue.PublishRun(fmt.Sprintf("wf.run.%d", partition), []byte("terminal-redelivery"))
	consumer, err := queue.Consumer(ctx, uint32(partition))
	if err != nil {
		return trace, err
	}
	batch, err := consumer.FetchOne(ctx)
	if err != nil {
		return trace, err
	}
	if msg := <-batch.Messages(); msg == nil || batch.Error() != nil {
		return trace, fmt.Errorf("missing initial delivery")
	}
	if err := schedule.AdvanceMillis(45000); err != nil {
		return trace, err
	}
	stale, err := assignment.Rebalance(ctx, assignments, []string{"dead", "live"}, false)
	if err != nil || stale.Moved != 0 {
		return trace, fmt.Errorf("stale manual list changed: %+v %v", stale, err)
	}
	owner, _, err := assignments.GetLatest(ctx, uint32(partition))
	if err != nil || owner != "dead" {
		return trace, fmt.Errorf("control owner=%s err=%v", owner, err)
	}
	if !errors.Is(queue.CheckDrained(), ErrRunQueueRetained) {
		return trace, fmt.Errorf("dead-owner drain mutation survived")
	}
	pending, ackPending, err := consumer.Info(ctx)
	if err != nil || pending != 0 || ackPending != 1 {
		return trace, fmt.Errorf("control pending=%d ack=%d err=%v", pending, ackPending, err)
	}
	schedule.RecordTransport(TransportEvent{Operation: "manual_owner_checker", Outcome: "expected_dead_owner_stall", AtMillis: schedule.NowMillis()})
	fault, err := schedule.Choose([]string{"clean", "drop_move", "lose_move_ack"})
	if err != nil {
		return trace, err
	}
	if fault != "clean" {
		kind := KVDropBeforeCommit
		if fault == "lose_move_ack" {
			kind = KVLoseAckAfterCommit
		}
		if err := assignments.QueueFault(KVFault{Operation: "update", Kind: kind}); err != nil {
			return trace, err
		}
	}
	_, err = assignment.Rebalance(ctx, assignments, []string{"live"}, false)
	if fault == "clean" && err != nil || fault != "clean" && !errors.Is(err, ErrTransportLost) {
		return trace, fmt.Errorf("move fault=%s err=%v", fault, err)
	}
	if _, err := assignment.Rebalance(ctx, assignments, []string{"live"}, false); err != nil {
		return trace, err
	}
	owner, _, err = assignments.GetLatest(ctx, uint32(partition))
	if err != nil || owner != "live" {
		return trace, fmt.Errorf("unrecovered owner=%s err=%v", owner, err)
	}
	batch, err = consumer.FetchOne(ctx)
	if err != nil {
		return trace, err
	}
	msg := <-batch.Messages()
	if msg == nil || batch.Error() != nil {
		return trace, fmt.Errorf("missing terminal redelivery")
	}
	metadata, err := msg.Metadata()
	if err != nil || metadata.NumDelivered != 2 {
		return trace, fmt.Errorf("redelivery metadata=%+v err=%v", metadata, err)
	}
	if err := msg.DoubleAck(ctx); err != nil {
		return trace, err
	}
	if err := queue.CheckDrained(); err != nil {
		return trace, err
	}
	return trace, schedule.Finish()
}

func TestSeededDeadOwnerDrainReplay(t *testing.T) {
	if os.Getenv("SIM_DEAD_OWNER_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededDeadOwnerDrain(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_DEAD_OWNER_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		generated, err := runSeededDeadOwnerDrain(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "dead-owner-failure.json")
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatal(saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runSeededDeadOwnerDrain(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d dead-owner replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("dead-owner-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededDeadOwnerDrainReplay$")
		cmd.Env = append(os.Environ(), "SIM_DEAD_OWNER_HELPER=1", "SIM_DEAD_OWNER_OUT="+files[i], "FAULT_SEED=42")
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
		t.Fatal("dead-owner trace changed across processes")
	}
}
