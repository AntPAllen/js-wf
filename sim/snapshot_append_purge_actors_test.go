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

func runSnapshotAppendPurgeActors(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("snapshot_append_purge"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"clean", "append_drop", "append_ack_lost"})
	if err != nil {
		return trace, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const typ, id = "test", "snapshot-append-purge"
	key := identity.Key(typ, id)
	subject := identity.JournalSubject(typ, id)
	port := NewPurgeTransport(schedule)
	old := time.Unix(0, 0).UTC().Add(-time.Hour)
	for _, name := range []string{"input-snapshot-append", "step-result-snapshot-append", "terminal-result-snapshot-append", "input-orphan-snapshot-append"} {
		port.Blobs.PutObject(name, []byte(name), old)
	}
	header := nats.Header{}
	header.Set("Wf-Input-Ref", "input-snapshot-append")
	invSeq, err := port.Blobs.PublishSubject("WF_INV", identity.InvocationSubject(typ, id), header, []byte(`null`))
	if err != nil {
		return trace, err
	}
	terminal, err := json.Marshal(wf.Outcome{InvSeq: invSeq, ResultRef: "terminal-result-snapshot-append"})
	if err != nil {
		return trace, err
	}
	base := journal.NewWithSnapshotPort(port, port, port)
	var tail uint64
	for index, entry := range []journal.Entry{
		{Kind: journal.Started},
		{Kind: journal.StepRequested, Payload: json.RawMessage(`{"kind":"run","name":"work"}`)},
		{Kind: journal.StepCompleted, Payload: json.RawMessage(`{"result_ref":"step-result-snapshot-append"}`)},
	} {
		entry.Index, entry.Epoch = uint64(index), 1
		tail, err = base.Append(ctx, typ, id, entry, tail)
		if err != nil {
			return trace, err
		}
	}
	workerLease, err := port.leasing.Acquire(ctx, typ, id, "snapshot-writer")
	if err != nil {
		return trace, err
	}
	if mode != "clean" {
		fault := DropBeforeCommit
		if mode == "append_ack_lost" {
			fault = LoseAckAfterCommit
		}
		if err := port.QueueFault(PurgeFault{Operation: "append_WF_JRN", Kind: fault}); err != nil {
			return trace, err
		}
	}
	var snap journal.Snapshot
	actors := []CooperativeActor{
		{Name: "snapshot", Run: func(ctx context.Context, yield YieldFunc) error {
			store := journal.NewWithSnapshotPort(
				yieldingAppendPort{yield: yield, transport: port},
				yieldingSnapshotReadPort{yield: yield, transport: port},
				yieldingSnapshotPort{yield: yield, transport: port})
			var err error
			snap, err = store.SnapshotPrefix(ctx, typ, id, 1)
			return err
		}},
		{Name: "writer", Run: func(ctx context.Context, yield YieldFunc) error {
			store := journal.NewWithAppendPort(yieldingAppendPort{yield: yield, transport: port})
			entry := journal.Entry{Index: 3, Epoch: 1, Kind: journal.Completed, Payload: terminal}
			if _, err := store.Append(ctx, typ, id, entry, tail); err != nil {
				if !errors.Is(err, journal.ErrUnknown) || mode == "clean" {
					return err
				}
				reader := journal.NewWithSnapshotReadPort(nil,
					yieldingSnapshotReadPort{yield: yield, transport: port},
					yieldingSnapshotPort{yield: yield, transport: port})
				records, _, readErr := reader.Read(ctx, typ, id)
				if readErr != nil {
					return readErr
				}
				switch len(records) {
				case 3:
					if _, err := store.Append(ctx, typ, id, entry, tail); err != nil {
						return fmt.Errorf("retry dropped terminal append: %w", err)
					}
				case 4:
					if records[3].Kind != journal.Completed || !bytes.Equal(records[3].Payload, terminal) {
						return fmt.Errorf("unknown append resolved to foreign terminal entry")
					}
				default:
					return fmt.Errorf("unknown append read %d entries", len(records))
				}
			}
			var stateErr error
			if err := yield(ctx, "write_terminal_state", func() {
				_, stateErr = port.Blobs.State().Create(ctx, key, terminal)
			}); err != nil {
				return err
			}
			return stateErr
		}},
		{Name: "purge", Run: func(ctx context.Context, yield YieldFunc) error {
			var purgeErr error
			if err := yield(ctx, "attempt_purge", func() {
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
	if results["snapshot"] != nil || results["writer"] != nil || !errors.Is(results["purge"], retention.ErrActive) {
		return trace, fmt.Errorf("seed %d actors=%v", seed, results)
	}
	var dropped, lostAck, committed int
	for _, event := range schedule.Trace().Transport {
		if event.Operation != "purge_journal_publish" || event.Subject != subject || event.Expected != tail {
			continue
		}
		switch event.Outcome {
		case string(DropBeforeCommit):
			dropped++
		case string(LoseAckAfterCommit):
			lostAck++
		case "ok":
			committed++
		}
	}
	if mode == "clean" && (dropped != 0 || lostAck != 0 || committed != 1) ||
		mode == "append_drop" && (dropped != 1 || lostAck != 0 || committed != 1) ||
		mode == "append_ack_lost" && (dropped != 0 || lostAck != 1 || committed != 0) {
		return trace, fmt.Errorf("seed %d mode %s append outcomes: dropped=%d lost_ack=%d committed=%d", seed, mode, dropped, lostAck, committed)
	}
	if snap.LastIndex != 1 && snap.LastIndex != 2 {
		return trace, fmt.Errorf("seed %d snapshot cutoff index %d", seed, snap.LastIndex)
	}
	read, readTail, err := base.Read(ctx, typ, id)
	if err != nil || len(read) != 4 || readTail <= snap.LastSeq || read[3].Kind != journal.Completed || !bytes.Equal(read[3].Payload, terminal) {
		return trace, fmt.Errorf("seed %d compacted read=%v tail=%d snapshot=%+v err=%v", seed, read, readTail, snap, err)
	}
	for index, record := range read {
		if record.Index != uint64(index) {
			return trace, fmt.Errorf("seed %d journal index %d at %d", seed, record.Index, index)
		}
	}
	first, last, err := port.Blobs.StreamRange(ctx, "WF_JRN")
	if err != nil {
		return trace, err
	}
	var live int
	for sequence := first; sequence != 0 && sequence <= last; sequence++ {
		message, err := port.Blobs.StreamMessage(ctx, "WF_JRN", sequence)
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			continue
		}
		if err != nil {
			return trace, err
		}
		if message.Subject == subject {
			live++
		}
	}
	if live != 3-int(snap.LastIndex) {
		return trace, fmt.Errorf("seed %d retained live journal=%d cutoff=%d", seed, live, snap.LastIndex)
	}
	if err := workerLease.Release(ctx); err != nil {
		return trace, err
	}
	if err := retention.PurgeWithPort(ctx, port, typ, id, time.Hour); err != nil {
		return trace, fmt.Errorf("seed %d purge after release: %w", seed, err)
	}
	after, err := retention.SweepBlobsQuiescentWithPort(ctx, port.Blobs, 0, time.Unix(0, 0).UTC().Add(time.Hour))
	if err != nil || after.Referenced != 0 || after.Deleted != 5 || port.PurgeEventCount() != 1 {
		return trace, fmt.Errorf("seed %d sweep=%+v events=%d err=%v", seed, after, port.PurgeEventCount(), err)
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_snapshot_append_purge", Subject: key, Sequence: snap.LastSeq, Outcome: fmt.Sprintf("%s_cut_%d", mode, snap.LastIndex), AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestCooperativeSnapshotAppendPurgeReplay(t *testing.T) {
	if os.Getenv("SIM_SNAPSHOT_APPEND_PURGE_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSnapshotAppendPurgeActors(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_SNAPSHOT_APPEND_PURGE_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	seen := map[string]int{}
	cutoffs := map[string]int{}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		generated, err := runSnapshotAppendPurgeActors(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "snapshot-append-purge-failure.json")
			}
			_ = generated.Save(path)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		seen[generated.Decisions[0].Chosen]++
		cutoffs[generated.Transport[len(generated.Transport)-1].Outcome]++
		if seed <= 10 {
			replayed, err := runSnapshotAppendPurgeActors(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d replay: %v", seed, err)
			}
		}
	}
	for _, mode := range []string{"clean", "append_drop", "append_ack_lost"} {
		if seen[mode] == 0 {
			t.Fatalf("missing append fault mode %s: %v", mode, seen)
		}
	}
	var cutOne, cutTwo int
	for outcome, count := range cutoffs {
		if len(outcome) >= 5 && outcome[len(outcome)-5:] == "cut_1" {
			cutOne += count
		}
		if len(outcome) >= 5 && outcome[len(outcome)-5:] == "cut_2" {
			cutTwo += count
		}
	}
	if cutOne == 0 || cutTwo == 0 {
		t.Fatalf("missing append/snapshot ordering: %v", cutoffs)
	}
	for _, seed := range []int{1, 2, 5} {
		var files [2]string
		for i := range files {
			files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("snapshot-append-purge-%d-%d.json", seed, i))
			cmd := exec.Command(os.Args[0], "-test.run=^TestCooperativeSnapshotAppendPurgeReplay$")
			cmd.Env = append(os.Environ(), "SIM_SNAPSHOT_APPEND_PURGE_HELPER=1", "SIM_SNAPSHOT_APPEND_PURGE_OUT="+files[i], fmt.Sprintf("FAULT_SEED=%d", seed))
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("seed %d child %d: %v: %s", seed, i, err, output)
			}
		}
		firstTrace, err := os.ReadFile(files[0])
		if err != nil {
			t.Fatal(err)
		}
		secondTrace, err := os.ReadFile(files[1])
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(firstTrace, secondTrace) {
			t.Fatalf("seed %d snapshot/append/purge trace changed across processes", seed)
		}
	}
}
