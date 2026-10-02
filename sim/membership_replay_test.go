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

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/assignment"
	"js-wf/lease"
	"js-wf/provision"
)

type modeledAssignments struct{ *KVTransport }

func (p modeledAssignments) GetLatest(ctx context.Context, partition uint32) (string, uint64, error) {
	entry, err := p.Get(ctx, fmt.Sprintf("p%02d", partition))
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return "", 0, nil
	}
	return string(entry.Value), entry.Revision, err
}
func (p modeledAssignments) Assign(ctx context.Context, partition uint32, owner string, revision uint64) (uint64, error) {
	key := fmt.Sprintf("p%02d", partition)
	var seq uint64
	var err error
	if revision == 0 {
		seq, err = p.Create(ctx, key, []byte(owner))
	} else {
		seq, err = p.Update(ctx, key, []byte(owner), revision)
	}
	if errors.Is(err, jetstream.ErrKeyExists) || errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
		return 0, assignment.ErrConflict
	}
	return seq, err
}

func runSeededMembership(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("automatic_membership"); err != nil {
		return trace, err
	}
	defer func() { trace = schedule.Trace() }()
	ctx := context.Background()
	registry := NewKVTransport(schedule, provision.LeaseTTL)
	assignments := modeledAssignments{NewKVTransport(schedule, 0)}
	members := assignment.NewMembershipWithPort(registry)
	a, err := members.Controller(ctx, "a", assignments)
	if err != nil {
		return trace, err
	}
	if err := a.Step(ctx); err != nil {
		return trace, err
	}
	if _, err := members.Register(ctx, "a"); !errors.Is(err, lease.ErrHeld) {
		return trace, fmt.Errorf("duplicate membership accepted: %v", err)
	}
	b, err := members.Controller(ctx, "b", assignments)
	if err != nil {
		return trace, err
	}
	if err := b.Step(ctx); err != nil {
		return trace, err
	}
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
	err = a.Step(ctx)
	if (fault == "clean" && err != nil) || (fault != "clean" && !errors.Is(err, ErrTransportLost)) {
		return trace, fmt.Errorf("join fault=%s err=%v", fault, err)
	}
	// The runner stops a after any uncertain pass; the clean case models a hard
	// process kill at the same boundary. Neither path releases retained leases.
	for i := 0; i < 4; i++ {
		if err := schedule.AdvanceMillis((3 * time.Second).Milliseconds()); err != nil {
			return trace, err
		}
		if err := b.Step(ctx); err != nil {
			return trace, err
		}
	}
	if err := a.Step(ctx); !errors.Is(err, lease.ErrLost) {
		return trace, fmt.Errorf("expired process stepped: %v", err)
	}
	a.Close()
	if err := b.Step(ctx); err != nil {
		return trace, fmt.Errorf("old cleanup damaged successor: %v", err)
	}
	live, err := members.Live(ctx)
	if err != nil || len(live) != 1 || live[0] != "b" {
		return trace, fmt.Errorf("live=%v err=%v", live, err)
	}
	for p := uint32(0); p < provision.Partitions; p++ {
		owner, revision, err := assignments.GetLatest(ctx, p)
		if err != nil || owner != "b" || revision == 0 {
			return trace, fmt.Errorf("unrecovered partition %d=%s/%d err=%v", p, owner, revision, err)
		}
	}
	// Successive passes over unchanged membership must not rewrite assignments.
	revision := assignments.revision
	if err := b.Step(ctx); err != nil {
		return trace, err
	}
	if assignments.revision != revision {
		return trace, fmt.Errorf("stable membership rewrote assignments")
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_membership_takeover", Sequence: uint64(provision.Partitions), Outcome: "ok", DataSHA256: digest([]byte(strconv.FormatUint(revision, 10))), AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededMembershipReplay(t *testing.T) {
	if os.Getenv("SIM_MEMBERSHIP_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededMembership(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_MEMBERSHIP_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed := range seededSchedules(t) {
		generated, err := runSeededMembership(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-membership-failure-")
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
		if seed <= 10 {
			replayed, err := runSeededMembership(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d membership replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("membership-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededMembershipReplay$")
		cmd.Env = append(os.Environ(), "SIM_MEMBERSHIP_HELPER=1", "SIM_MEMBERSHIP_OUT="+files[i], "FAULT_SEED=42")
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
