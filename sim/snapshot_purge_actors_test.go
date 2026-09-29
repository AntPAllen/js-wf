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
	"js-wf/journal"
	"js-wf/retention"
	"js-wf/wf"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func runSnapshotPurgeActors(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("snapshot_purge_lease"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"during_snapshot", "after_release"})
	if err != nil {
		return trace, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const typ, id = "test", "snapshot-purge"
	key := identity.Key(typ, id)
	port := NewPurgeTransport(schedule)
	old := time.Unix(0, 0).UTC().Add(-time.Hour)
	port.Blobs.PutObject("input-snapshot-purge", []byte("input"), old)
	port.Blobs.PutObject("terminal-result-snapshot-purge", []byte("result"), old)
	port.Blobs.PutObject("input-orphan-snapshot-purge", []byte("orphan"), old)
	header := nats.Header{}
	header.Set("Wf-Input-Ref", "input-snapshot-purge")
	invSeq, err := port.Blobs.PublishSubject("WF_INV", identity.InvocationSubject(typ, id), header, []byte(`null`))
	if err != nil {
		return trace, err
	}
	terminal, err := json.Marshal(wf.Outcome{InvSeq: invSeq, ResultRef: "terminal-result-snapshot-purge"})
	if err != nil {
		return trace, err
	}
	for index, entry := range []journal.Entry{{Kind: journal.Started}, {Kind: journal.Completed, Payload: terminal}} {
		entry.Index, entry.Epoch = uint64(index), 1
		data, err := json.Marshal(entry)
		if err != nil {
			return trace, err
		}
		if _, err := port.Blobs.PublishSubject("WF_JRN", identity.JournalSubject(typ, id), nil, data); err != nil {
			return trace, err
		}
	}
	if _, err := port.Blobs.State().Create(ctx, key, terminal); err != nil {
		return trace, err
	}
	workerLease, err := port.leasing.Acquire(ctx, typ, id, "snapshot-worker")
	if err != nil {
		return trace, err
	}
	workerHeld := true
	var purgeWhileHeld bool
	actors := []CooperativeActor{
		{Name: "snapshot", Run: func(ctx context.Context, yield YieldFunc) error {
			store := journal.NewWithSnapshotPort(nil,
				yieldingSnapshotReadPort{yield: yield, transport: port},
				yieldingSnapshotPort{yield: yield, transport: port})
			_, snapshotErr := store.SnapshotPrefix(ctx, typ, id, 1)
			var releaseErr error
			if err := yield(ctx, "release_worker_lease", func() {
				releaseErr = workerLease.Release(ctx)
				if releaseErr == nil {
					workerHeld = false
				}
			}); err != nil {
				return err
			}
			if snapshotErr != nil {
				return snapshotErr
			}
			return releaseErr
		}},
		{Name: "purge", Run: func(ctx context.Context, yield YieldFunc) error {
			if mode == "after_release" {
				for attempt := 0; attempt < 100; attempt++ {
					var released bool
					if err := yield(ctx, "await_worker_release", func() { released = !workerHeld }); err != nil {
						return err
					}
					if released {
						break
					}
					if attempt == 99 {
						return fmt.Errorf("worker lease release was not scheduled")
					}
				}
			}
			var purgeErr error
			if err := yield(ctx, "attempt_purge", func() {
				purgeWhileHeld = workerHeld
				purgeErr = retention.PurgeWithPort(ctx, port, typ, id, time.Hour)
			}); err != nil {
				return err
			}
			return purgeErr
		}},
	}
	results, err := RunCooperative(ctx, schedule, actors)
	if err != nil {
		return trace, err
	}
	if results["snapshot"] != nil {
		return trace, fmt.Errorf("seed %d snapshot: %w", seed, results["snapshot"])
	}
	if purgeWhileHeld {
		if !errors.Is(results["purge"], retention.ErrActive) {
			return trace, fmt.Errorf("seed %d purge while worker held lease: %v", seed, results["purge"])
		}
		records, _, err := journal.NewWithSnapshotReadPort(nil, port, port).Read(ctx, typ, id)
		if err != nil || len(records) != 2 || records[1].Kind != journal.Completed {
			return trace, fmt.Errorf("seed %d snapshot after fenced purge: records=%v err=%v", seed, records, err)
		}
	} else if results["purge"] != nil {
		return trace, fmt.Errorf("seed %d purge after lease release: %w", seed, results["purge"])
	}
	if err := retention.PurgeWithPort(ctx, port, typ, id, time.Hour); err != nil {
		return trace, fmt.Errorf("seed %d retry purge: %w", seed, err)
	}
	value, err := port.Blobs.State().Get(ctx, key)
	if err != nil {
		return trace, err
	}
	marker, tomb, err := retention.Decode(value.Value)
	if err != nil || !tomb || marker.InvSeq != invSeq || port.PurgeEventCount() != 1 {
		return trace, fmt.Errorf("seed %d tombstone=%+v tomb=%v purge events=%d err=%v", seed, marker, tomb, port.PurgeEventCount(), err)
	}
	if _, err := port.Blobs.State().Get(ctx, "snap."+key); !errors.Is(err, jetstream.ErrKeyNotFound) {
		return trace, fmt.Errorf("seed %d snapshot manifest retained: %v", seed, err)
	}
	after, err := retention.SweepBlobsQuiescentWithPort(ctx, port.Blobs, 0, time.Unix(0, 0).UTC().Add(time.Hour))
	if err != nil || after.Referenced != 0 || after.Deleted != 4 {
		return trace, fmt.Errorf("seed %d blob sweep=%+v err=%v", seed, after, err)
	}
	outcome := "purge_fenced"
	if !purgeWhileHeld {
		outcome = "purge_after_release"
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_snapshot_purge_lease", Subject: key, Sequence: invSeq, Outcome: outcome, AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestCooperativeSnapshotPurgeLeaseReplay(t *testing.T) {
	if os.Getenv("SIM_SNAPSHOT_PURGE_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSnapshotPurgeActors(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_SNAPSHOT_PURGE_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	seen := map[string]int{}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		generated, err := runSnapshotPurgeActors(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "snapshot-purge-failure.json")
			}
			_ = generated.Save(path)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		seen[generated.Transport[len(generated.Transport)-1].Outcome]++
		if seed <= 10 {
			replayed, err := runSnapshotPurgeActors(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d replay: %v", seed, err)
			}
		}
	}
	if seen["purge_fenced"] == 0 || seen["purge_after_release"] == 0 {
		t.Fatalf("missing lease ordering: %v", seen)
	}
	for _, seed := range []int{1, 2} {
		var files [2]string
		for i := range files {
			files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("snapshot-purge-%d-%d.json", seed, i))
			cmd := exec.Command(os.Args[0], "-test.run=^TestCooperativeSnapshotPurgeLeaseReplay$")
			cmd.Env = append(os.Environ(), "SIM_SNAPSHOT_PURGE_HELPER=1", "SIM_SNAPSHOT_PURGE_OUT="+files[i], fmt.Sprintf("FAULT_SEED=%d", seed))
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("seed %d child %d: %v: %s", seed, i, err, output)
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
			t.Fatalf("seed %d snapshot/purge trace changed across processes", seed)
		}
	}
}
