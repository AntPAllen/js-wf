package sim

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"js-wf/identity"
	"js-wf/retention"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func runSeededTombstoneSweep(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("tombstone_sweep"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"absent", "held", "reused", "not_expired", "dry_run", "revision_changed", "delete_drop", "delete_ack_lost"})
	if err != nil {
		return trace, err
	}
	ctx := context.Background()
	invocations := NewStartTransport(schedule)
	state := NewKVTransport(schedule, 0)
	port := TombstoneSweepTransport{Invocations: invocations, State: state}
	const typ, id = "test", "retired"
	key := identity.Key(typ, id)
	subject := identity.InvocationSubject(typ, id)
	if mode == "reused" {
		if _, err := invocations.PublishInvocation(ctx, &nats.Msg{Subject: identity.InvocationSubject(typ, "old"), Data: []byte(`null`)}); err != nil {
			return trace, err
		}
	}
	if mode == "held" || mode == "reused" {
		if _, err := invocations.PublishInvocation(ctx, &nats.Msg{Subject: subject, Data: []byte(`null`)}); err != nil {
			return trace, err
		}
	}
	now := time.Unix(0, 0).UTC().Add(2 * time.Hour)
	expires := now.Add(-time.Hour)
	if mode == "not_expired" {
		expires = now.Add(time.Hour)
	}
	value, err := json.Marshal(retention.Tombstone{Tombstone: true, InvSeq: 1, PurgedAt: now.Add(-2 * time.Hour), ExpiresAt: expires})
	if err != nil {
		return trace, err
	}
	revision, err := state.Create(ctx, key, value)
	if err != nil {
		return trace, err
	}
	if mode == "revision_changed" {
		if _, err := state.Update(ctx, key, []byte(`{"inv_seq":2,"result":true}`), revision); err != nil {
			return trace, err
		}
	}
	if mode == "delete_drop" || mode == "delete_ack_lost" {
		fault := KVDropBeforeCommit
		if mode == "delete_ack_lost" {
			fault = KVLoseAckAfterCommit
		}
		if err := state.QueueFault(KVFault{Operation: "delete", Kind: fault}); err != nil {
			return trace, err
		}
	}
	expired, eligible, deleted, sweepErr := retention.SweepCandidate(ctx, port, key, value, revision, now, mode == "dry_run")
	wantExpired := mode != "not_expired"
	wantEligible := wantExpired && mode != "held"
	wantDeleted := mode == "absent" || mode == "reused"
	if expired != wantExpired || eligible != wantEligible || deleted != wantDeleted {
		return trace, fmt.Errorf("seed %d mode %s sweep=(%v,%v,%v) want=(%v,%v,%v)", seed, mode, expired, eligible, deleted, wantExpired, wantEligible, wantDeleted)
	}
	if (sweepErr != nil) != (mode == "delete_drop" || mode == "delete_ack_lost") {
		return trace, fmt.Errorf("seed %d mode %s unexpected sweep error: %v", seed, mode, sweepErr)
	}
	current, getErr := state.Get(ctx, key)
	missing := errors.Is(getErr, jetstream.ErrKeyNotFound)
	if missing != (wantDeleted || mode == "delete_ack_lost") || getErr != nil && !missing {
		return trace, fmt.Errorf("seed %d mode %s current=%+v err=%v", seed, mode, current, getErr)
	}
	if mode == "revision_changed" && (string(current.Value) != `{"inv_seq":2,"result":true}` || current.Revision <= revision) {
		return trace, fmt.Errorf("seed %d changed revision lost: %+v", seed, current)
	}
	if mode == "delete_drop" {
		_, _, repaired, err := retention.SweepCandidate(ctx, port, key, current.Value, current.Revision, now, false)
		if err != nil || !repaired {
			return trace, fmt.Errorf("seed %d dropped delete not repaired: %v", seed, err)
		}
		if _, err := state.Get(ctx, key); !errors.Is(err, jetstream.ErrKeyNotFound) {
			return trace, fmt.Errorf("seed %d repaired tombstone retained: %v", seed, err)
		}
	}
	extraValue, err := json.Marshal(retention.Tombstone{Tombstone: true, InvSeq: 1, PurgedAt: now.Add(-2 * time.Hour), ExpiresAt: now.Add(-time.Hour)})
	if err != nil {
		return trace, err
	}
	if _, err := state.Create(ctx, identity.Key(typ, "extra"), extraValue); err != nil {
		return trace, err
	}
	if _, err := state.Create(ctx, identity.Key(typ, "result"), []byte(`{"inv_seq":9,"result":true}`)); err != nil {
		return trace, err
	}
	if _, err := state.Create(ctx, "scan.cursor.v1", []byte(`1`)); err != nil {
		return trace, err
	}
	full, err := retention.SweepTombstonesWithPort(ctx, port, now)
	wantFullDeleted := 1
	if mode == "dry_run" {
		wantFullDeleted++
	}
	if err != nil || full.Deleted != wantFullDeleted {
		return trace, fmt.Errorf("seed %d mode %s full sweep=%+v err=%v", seed, mode, full, err)
	}
	if _, err := state.Get(ctx, identity.Key(typ, "extra")); !errors.Is(err, jetstream.ErrKeyNotFound) {
		return trace, fmt.Errorf("seed %d extra tombstone survived: %v", seed, err)
	}
	if _, err := state.Get(ctx, identity.Key(typ, "result")); err != nil {
		return trace, fmt.Errorf("seed %d terminal result removed: %v", seed, err)
	}
	second, err := retention.SweepTombstonesWithPort(ctx, port, now)
	if err != nil || second.Deleted != 0 {
		return trace, fmt.Errorf("seed %d mode %s repeat sweep=%+v err=%v", seed, mode, second, err)
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_tombstone_sweep", Subject: key, Outcome: mode, AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededTombstoneSweepReplay(t *testing.T) {
	if os.Getenv("SIM_TOMBSTONE_SWEEP_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededTombstoneSweep(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_TOMBSTONE_SWEEP_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	observed := map[string]int{}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		generated, err := runSeededTombstoneSweep(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "tombstone-sweep-failure.json")
			}
			_ = generated.Save(path)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		observed[generated.Decisions[0].Chosen]++
		if seed <= 10 {
			replayed, err := runSeededTombstoneSweep(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d replay: %v", seed, err)
			}
		}
	}
	for _, mode := range []string{"absent", "held", "reused", "not_expired", "dry_run", "revision_changed", "delete_drop", "delete_ack_lost"} {
		if observed[mode] == 0 {
			t.Fatalf("mode %s was not scheduled", mode)
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("tombstone-sweep-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededTombstoneSweepReplay$")
		cmd.Env = append(os.Environ(), "SIM_TOMBSTONE_SWEEP_HELPER=1", "SIM_TOMBSTONE_SWEEP_OUT="+files[i], "FAULT_SEED=42")
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
		t.Fatal("tombstone sweep trace changed across processes")
	}
}
