package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/assignment"
	"js-wf/client"
	"js-wf/history"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/provision"
	"js-wf/wf"
	"js-wf/worker"
)

func automaticMembershipHandler(blockID, marker string) worker.Handler {
	return func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
		var id string
		if err := json.Unmarshal(input, &id); err != nil {
			return nil, err
		}
		value := 0
		for i := 0; i < 10; i++ {
			next, err := wf.Run(c, fmt.Sprintf("increment-%02d", i), value, func(ctx context.Context) (int, error) {
				if i == 0 && id == blockID {
					if err := os.WriteFile(marker, []byte(id), 0600); err != nil {
						return 0, err
					}
					<-ctx.Done()
					return 0, ctx.Err()
				}
				return value + 1, nil
			})
			if err != nil {
				return nil, err
			}
			value = next
		}
		return json.Marshal(value)
	}
}

func TestAutomaticMembershipWorkerChild(t *testing.T) {
	if os.Getenv("WF_AUTO_MEMBERSHIP_CHILD") != "1" {
		t.Skip("automatic worker subprocess helper")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	nc, err := nats.Connect(os.Getenv("WF_AUTO_MEMBERSHIP_URL"), nats.NoReconnect(), nats.IgnoreDiscoveredServers())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	members, err := assignment.EnsureMembership(ctx, js, 3)
	if err != nil {
		t.Fatal(err)
	}
	owners, err := assignment.New(ctx, js)
	if err != nil {
		t.Fatal(err)
	}
	controller, err := members.Controller(ctx, "a", owners)
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	w, err := worker.New(ctx, js, "a", map[string]worker.Handler{"auto": automaticMembershipHandler(os.Getenv("WF_AUTO_MEMBERSHIP_TARGET"), os.Getenv("WF_AUTO_MEMBERSHIP_MARKER"))})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	done := make(chan error, 2)
	go func() { done <- controller.Run(ctx) }()
	go func() { done <- w.RunKVAssignments(ctx) }()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Error(err)
	}
	cancel()
	<-done
}

func TestAutomaticMembershipRecoversKilledCoordinatorWithWorkflowTraffic(t *testing.T) {
	all, cluster := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	members, err := assignment.EnsureMembership(ctx, all[0], 3)
	if err != nil {
		t.Fatal(err)
	}
	owners, err := assignment.New(ctx, all[0])
	if err != nil {
		t.Fatal(err)
	}
	target := ""
	for i := 0; target == ""; i++ {
		id := fmt.Sprintf("target-%d", i)
		if identity.Partition("auto", id, provision.Partitions) == 0 {
			target = id
		}
	}
	marker := filepath.Join(t.TempDir(), "effect-entered")
	command := exec.Command(os.Args[0], "-test.run=^TestAutomaticMembershipWorkerChild$", "-test.timeout=2m")
	command.Env = append(os.Environ(), "WF_AUTO_MEMBERSHIP_CHILD=1", "WF_AUTO_MEMBERSHIP_URL="+cluster.Servers[0].ClientURL(), "WF_AUTO_MEMBERSHIP_TARGET="+target, "WF_AUTO_MEMBERSHIP_MARKER="+marker)
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	childDone := make(chan error, 1)
	go func() { childDone <- command.Wait() }()
	joined := false
	defer func() {
		if !joined {
			_ = command.Process.Kill()
			<-childDone
		}
		if t.Failed() {
			t.Logf("automatic child output: %s", output.String())
		}
	}()
	waitOwners := func(wantA, wantB int) {
		t.Helper()
		for ctx.Err() == nil {
			counts := map[string]int{}
			for p := uint32(0); p < provision.Partitions; p++ {
				owner, _, err := owners.GetLatest(ctx, p)
				if err != nil {
					t.Fatal(err)
				}
				counts[owner]++
			}
			if counts["a"] == wantA && counts["b"] == wantB {
				return
			}
			time.Sleep(25 * time.Millisecond)
		}
		t.Fatal("automatic ownership failed to converge", ctx.Err())
	}
	waitOwners(64, 0)
	recorded := &history.Recorder{}
	observed := client.NewObserved(all[2], recorded)
	input, _ := json.Marshal(target)
	if _, err := observed.Start(ctx, "auto", target, input); err != nil {
		t.Fatal(err)
	}
	for ctx.Err() == nil {
		if data, err := os.ReadFile(marker); err == nil && string(data) == target {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	records, _, err := journal.New(all[2]).Read(ctx, "auto", target)
	if err != nil || len(records) != 2 || records[1].Kind != journal.StepRequested {
		t.Fatalf("kill boundary records=%+v err=%v", records, err)
	}
	oldEpoch := records[0].Epoch
	leaseKV, err := all[2].KeyValue(ctx, "WF_LEASE")
	if err != nil {
		t.Fatal(err)
	}
	entry, err := leaseKV.Get(ctx, identity.Key("auto", target))
	if err != nil {
		t.Fatal(err)
	}
	var owner lease.Value
	if json.Unmarshal(entry.Value(), &owner) != nil || owner.Worker != "a" || owner.Epoch != oldEpoch {
		t.Fatalf("target not held by child: %s", entry.Value())
	}
	bMembers, err := assignment.EnsureMembership(ctx, all[1], 3)
	if err != nil {
		t.Fatal(err)
	}
	bOwners, err := assignment.New(ctx, all[1])
	if err != nil {
		t.Fatal(err)
	}
	controller, err := bMembers.Controller(ctx, "b", bOwners)
	if err != nil {
		t.Fatal(err)
	}
	w, err := worker.New(ctx, all[1], "b", map[string]worker.Handler{"auto": automaticMembershipHandler("", "")})
	if err != nil {
		t.Fatal(err)
	}
	bCtx, stopB := context.WithCancel(ctx)
	doneB := make(chan error, 2)
	go func() { doneB <- controller.Run(bCtx) }()
	go func() { doneB <- w.RunKVAssignments(bCtx) }()
	defer func() {
		stopB()
		for i := 0; i < 2; i++ {
			if err := <-doneB; err != nil && !errors.Is(err, context.Canceled) {
				t.Error(err)
			}
		}
		controller.Close()
		w.Close()
	}()
	waitOwners(32, 32)
	// Partition zero stays with a after the balanced join, so the target cannot
	// be completed by b before this hard process failure.
	ids := []string{target}
	for i := 0; i < 64; i++ {
		id := fmt.Sprintf("traffic-%02d", i)
		input, _ := json.Marshal(id)
		if _, err := observed.Start(ctx, "auto", id, input); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	membersKV, err := all[2].KeyValue(ctx, assignment.MembershipBucket)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := membersKV.Get(ctx, "coordinator.assign")
	if err != nil {
		t.Fatal(err)
	}
	if json.Unmarshal(coordinator.Value(), &owner) != nil || owner.Worker != "a" {
		t.Fatalf("wrong coordinator killed: %s", coordinator.Value())
	}
	killedAt := time.Now()
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	waitErr := <-childDone
	joined = true
	status, ok := command.ProcessState.Sys().(syscall.WaitStatus)
	if waitErr == nil || !ok || status.Signal() != syscall.SIGKILL {
		t.Fatalf("child not SIGKILLed: %v %v", command.ProcessState, waitErr)
	}
	waitOwners(0, 64)
	for _, id := range ids {
		result, err := observed.Await(ctx, "auto", id)
		if err != nil || string(result) != "10" {
			t.Fatalf("result %s=%s err=%v", id, result, err)
		}
		if id == target && time.Since(killedAt) >= 30*time.Second {
			t.Fatalf("kill-to-terminal exceeded 30s: %s", time.Since(killedAt))
		}
	}
	live, err := members.Live(ctx)
	if err != nil || len(live) != 1 || live[0] != "b" {
		t.Fatalf("dead member retained: %v %v", live, err)
	}
	records, _, err = journal.New(all[2]).Read(ctx, "auto", target)
	if err != nil {
		t.Fatal(err)
	}
	higher := false
	for _, record := range records {
		if record.Kind == journal.StepCompleted && record.Epoch > oldEpoch {
			higher = true
		}
	}
	if !higher {
		t.Fatal("replacement outcome did not use a higher epoch")
	}
	if result, err := history.CheckStarts(recorded.Snapshot(), 10*time.Second); err != nil || result != porcupine.Ok {
		t.Fatalf("start history: %v %v", result, err)
	}
	if result, err := history.CheckResults(recorded.Snapshot(), 10*time.Second); err != nil || result != porcupine.Ok {
		t.Fatalf("result history: %v %v", result, err)
	}
	audit, err := integrity.Check(ctx, all[2])
	if err != nil {
		t.Fatal(err)
	}
	if audit.Invocations != len(ids) || audit.Terminal != len(ids) {
		t.Fatalf("incomplete audit %+v", audit)
	}
	t.Logf("automatic SIGKILL takeover: invocations=%d entries=%d kill_to_all_results=%s", len(ids), audit.Entries, time.Since(killedAt))
}
