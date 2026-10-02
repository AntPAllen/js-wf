package sim

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"js-wf/assignment"
	"js-wf/lease"
	"js-wf/provision"
)

type sessionAssignmentPort struct {
	modeledAssignments
	reads    int
	passDone chan struct{}
}

func (p *sessionAssignmentPort) GetLatest(ctx context.Context, part uint32) (string, uint64, error) {
	owner, rev, err := p.modeledAssignments.GetLatest(ctx, part)
	p.reads++
	if p.reads == 2*int(provision.Partitions) {
		close(p.passDone)
		<-ctx.Done()
		return owner, rev, ctx.Err()
	}
	return owner, rev, err
}

func runMembershipSessionRecovery(seed int64, replay *Trace) (trace Trace, runErr error) {
	s := NewScheduler(seed)
	if replay != nil {
		var err error
		s, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	if err := s.SetWorkload("membership_session_recovery"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = s.Trace() }()
	fault, err := s.Choose([]string{"drop_before_commit", "lose_ack_after_commit"})
	if err != nil {
		return trace, err
	}
	registry := NewKVTransport(s, provision.LeaseTTL)
	owners := modeledAssignments{NewKVTransport(s, 0)}
	members := assignment.NewMembershipWithPort(registry)
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	controller, err := members.Controller(ctx, "a", owners)
	if err != nil {
		return trace, err
	}
	if err := controller.Step(ctx); err != nil {
		return trace, err
	}
	old, err := registry.Get(ctx, fmt.Sprintf("member.%x", sha256.Sum256([]byte("a"))))
	if err != nil {
		return trace, err
	}
	var oldValue lease.Value
	if err := json.Unmarshal(old.Value, &oldValue); err != nil {
		return trace, err
	}
	// Replace the port only for the replacement claim/rebalance pass.
	// Controller's original port is wrapped before construction below.
	controller.Close()
	port := &sessionAssignmentPort{modeledAssignments: owners, passDone: make(chan struct{})}
	controller, err = members.Controller(ctx, "a", port)
	if err != nil {
		return trace, err
	}
	old, err = registry.Get(ctx, fmt.Sprintf("member.%x", sha256.Sum256([]byte("a"))))
	if err != nil {
		return trace, err
	}
	if err := json.Unmarshal(old.Value, &oldValue); err != nil {
		return trace, err
	}
	if err := registry.QueueFault(KVFault{Operation: "update", Kind: KVFaultKind(fault)}); err != nil {
		return trace, err
	}
	stopping, join := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	calls := 0
	var events []assignment.SessionEvent
	workerDone := errors.New("modeled worker stopped after replacement pass")
	go func() {
		done <- controller.RunWithWorkers(ctx, func(session context.Context) error {
			calls++
			if calls == 1 {
				<-session.Done()
				close(stopping)
				<-join
				return nil
			}
			<-port.passDone
			return workerDone
		}, func(e assignment.SessionEvent) { events = append(events, e) })
	}()
	select {
	case <-stopping:
	case <-ctx.Done():
		return trace, ctx.Err()
	}
	// While old workers are still joining, no cleanup or new registration can run.
	held, err := registry.Get(ctx, fmt.Sprintf("member.%x", sha256.Sum256([]byte("a"))))
	var heldValue lease.Value
	_ = json.Unmarshal(held.Value, &heldValue)
	if err != nil || heldValue != oldValue {
		close(join)
		<-done
		return trace, fmt.Errorf("registration changed before workers joined: %+v %v", heldValue, err)
	}
	close(join)
	if err := <-done; !errors.Is(err, workerDone) {
		return trace, fmt.Errorf("session result: %v", err)
	}
	if calls != 2 || len(events) != 2 || events[0].Phase != "stopped" || events[1].Phase != "rejoined" || events[1].Epoch <= oldValue.Epoch || events[1].PreviousEpoch != oldValue.Epoch {
		return trace, fmt.Errorf("session events=%+v calls=%d", events, calls)
	}
	if err := controller.Step(ctx); !errors.Is(err, lease.ErrLost) {
		return trace, fmt.Errorf("old session resumed: %v", err)
	}
	for p := uint32(0); p < provision.Partitions; p++ {
		owner, revision, err := owners.GetLatest(ctx, p)
		if err != nil || owner != "a" || revision == 0 {
			return trace, fmt.Errorf("replacement claim partition%d=%s/%d %v", p, owner, revision, err)
		}
	}
	s.RecordTransport(TransportEvent{Operation: "check_membership_session", Outcome: fault, Sequence: events[1].Epoch, AtMillis: s.NowMillis()})
	if err := s.Finish(); err != nil {
		return trace, err
	}
	return s.Trace(), nil
}

func TestSeededMembershipSessionRecovery(t *testing.T) {
	covered := map[string]bool{}
	for seed := range seededSchedules(t) {
		generated, err := runMembershipSessionRecovery(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "membership-session.json")
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("seed%d: %v save:%v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		for _, e := range generated.Transport {
			if e.Operation == "check_membership_session" {
				if os.Getenv("SIM_WRITE_MEMBERSHIP_SESSION_PINS") == "1" && !covered[e.Outcome] {
					if err := generated.Save(filepath.Join("testdata", "regressions", "membership-session-"+e.Outcome+".json")); err != nil {
						t.Fatal(err)
					}
				}
				covered[e.Outcome] = true
			}
		}
		if seed <= 10 {
			replayed, err := runMembershipSessionRecovery(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("seed%d replay: %v", seed, err)
			}
		}
	}
	if len(covered) != 2 {
		t.Fatalf("missing renewal outcomes: %v", covered)
	}
}
