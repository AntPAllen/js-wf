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

	"js-wf/assignment"
	"js-wf/lease"
	"js-wf/provision"
)

type pausedCoordinatorAssignments struct {
	assignment.RebalancePort
	cut                      string
	partition                uint32
	hook                     func() error
	afterTakeover            bool
	postAttempts, postWrites int
}

func (p *pausedCoordinatorAssignments) GetLatest(ctx context.Context, partition uint32) (string, uint64, error) {
	owner, revision, err := p.RebalancePort.GetLatest(ctx, partition)
	if err == nil && p.cut == "before_renew" && partition == 63 && p.hook != nil {
		hook := p.hook
		p.hook = nil
		if err := hook(); err != nil {
			return "", 0, err
		}
	}
	return owner, revision, err
}
func (p *pausedCoordinatorAssignments) Assign(ctx context.Context, partition uint32, owner string, revision uint64) (uint64, error) {
	if p.cut == "after_renew" && partition == p.partition && p.hook != nil {
		hook := p.hook
		p.hook = nil
		if err := hook(); err != nil {
			return 0, err
		}
	}
	if p.afterTakeover {
		p.postAttempts++
	}
	next, err := p.RebalancePort.Assign(ctx, partition, owner, revision)
	if p.afterTakeover && err == nil {
		p.postWrites++
	}
	return next, err
}
func runPausedCoordinator(seed int64, replay *Trace) (Trace, error) {
	return runPausedCoordinatorMode(seed, replay, false)
}

func runUnchangedCoordinator(seed int64, replay *Trace) (Trace, error) {
	return runPausedCoordinatorMode(seed, replay, true)
}

func runPausedCoordinatorMode(seed int64, replay *Trace, unchanged bool) (trace Trace, runErr error) {
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
	workload := "paused_membership_coordinator"
	if unchanged {
		workload = "unchanged_membership_coordinator"
	}
	if err := schedule.SetWorkload(workload); err != nil {
		return trace, err
	}
	defer func() { trace = schedule.Trace() }()
	cut, err := schedule.Choose([]string{"before_renew", "after_renew"})
	if err != nil {
		return trace, err
	}
	selected, err := schedule.Choose([]string{"22", "42", "62"})
	if err != nil {
		return trace, err
	}
	partition, err := strconv.Atoi(selected)
	if err != nil {
		return trace, err
	}
	ctx := context.Background()
	registry := NewKVTransport(schedule, provision.LeaseTTL)
	retained := modeledAssignments{NewKVTransport(schedule, 0)}
	paused := &pausedCoordinatorAssignments{RebalancePort: retained, cut: cut, partition: uint32(partition)}
	members := assignment.NewMembershipWithPort(registry)
	a, err := members.Controller(ctx, "a", paused)
	if err != nil {
		return trace, err
	}
	if err := a.Step(ctx); err != nil {
		return trace, err
	}
	b, err := members.Controller(ctx, "b", retained)
	if err != nil {
		return trace, err
	}
	c, err := members.Controller(ctx, "c", retained)
	if err != nil {
		return trace, err
	}
	if err := b.Step(ctx); err != nil {
		return trace, err
	}
	if err := c.Step(ctx); err != nil {
		return trace, err
	}
	if unchanged {
		// The incumbent will plan moves away from c, but the successor would
		// keep the paused partition on c even without changing its revision.
		for p := uint32(0); p < provision.Partitions; p++ {
			_, rev, err := retained.GetLatest(ctx, p)
			if err != nil {
				return trace, err
			}
			if _, err := retained.Assign(ctx, p, "c", rev); err != nil {
				return trace, err
			}
		}
	}
	paused.hook = func() error {
		schedule.RecordTransport(TransportEvent{Operation: "pause_coordinator", Outcome: cut, Sequence: uint64(partition), AtMillis: schedule.NowMillis()})
		// Only c keeps heartbeating while a and b expire on the server clock.
		for range 4 {
			if err := schedule.AdvanceMillis(3000); err != nil {
				return err
			}
			if err := c.Step(ctx); err != nil {
				return err
			}
		}
		for p := uint32(0); p < provision.Partitions; p++ {
			owner, _, err := retained.GetLatest(ctx, p)
			if err != nil || owner != "c" {
				return fmt.Errorf("successor did not converge partition %d: %s %v", p, owner, err)
			}
		}
		paused.afterTakeover = true
		return nil
	}
	if err := a.Step(ctx); !errors.Is(err, lease.ErrLost) {
		return trace, fmt.Errorf("paused coordinator not fenced: %v", err)
	}
	wantAttempts := 0
	if cut == "after_renew" {
		wantAttempts = 1
	}
	if paused.postAttempts != wantAttempts || paused.postWrites != 0 {
		return trace, fmt.Errorf("stale assignment writes: attempts=%d want=%d writes=%d", paused.postAttempts, wantAttempts, paused.postWrites)
	}
	before := retained.revision
	a.Close()
	b.Close()
	if err := c.Step(ctx); err != nil {
		return trace, fmt.Errorf("old cleanup damaged coordinator: %v", err)
	}
	if retained.revision != before {
		return trace, fmt.Errorf("stale controller changed successor assignments")
	}
	live, err := members.Live(ctx)
	if err != nil || !reflect.DeepEqual(live, []string{"c"}) {
		return trace, fmt.Errorf("live membership=%v %v", live, err)
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_paused_coordinator", Outcome: cut, Sequence: uint64(paused.postAttempts), AtMillis: schedule.NowMillis()})
	return trace, schedule.Finish()
}
func TestSeededPausedCoordinatorReplay(t *testing.T) {
	if os.Getenv("SIM_PAUSED_COORDINATOR_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runPausedCoordinator(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_PAUSED_COORDINATOR_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	modes := map[string]bool{}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		generated, err := runPausedCoordinator(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-paused-coordinator-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-membership.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		modes[generated.Decisions[0].Chosen+"/"+generated.Decisions[1].Chosen] = true
		if seed <= 10 {
			replayed, err := runPausedCoordinator(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d paused coordinator replay: %v", seed, err)
			}
		}
	}
	if len(modes) != 6 {
		t.Fatalf("missing coordinator cut modes: %v", modes)
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("paused-coordinator-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededPausedCoordinatorReplay$")
		cmd.Env = append(os.Environ(), "SIM_PAUSED_COORDINATOR_HELPER=1", "SIM_PAUSED_COORDINATOR_OUT="+files[i], "FAULT_SEED=42")
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
		t.Fatal("membership trace changed across processes")
	}
}
