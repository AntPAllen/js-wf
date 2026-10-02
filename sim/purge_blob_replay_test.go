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

func runSeededPurgeBlob(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("purge_blob"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"clean", "marker_drop", "marker_ack_lost", "signals_drop", "signals_ack_lost", "journal_drop", "timer_drop", "snapshot_delete_drop", "tombstone_ack_lost", "purge_event_ack_lost", "invocation_ack_lost", "missing_inv_state_read_lost", "snapshot_delayed_read", "snapshot_corrupt"})
	if err != nil {
		return trace, err
	}
	ctx := context.Background()
	const typ, id = "test", "retired"
	key := identity.Key(typ, id)
	invSubject := identity.InvocationSubject(typ, id)
	journalSubject := identity.JournalSubject(typ, id)
	port := NewPurgeTransport(schedule)
	port.EnableFallbackTimers()
	old := time.Unix(0, 0).UTC().Add(-time.Hour)
	for _, name := range []string{"input-one", "signal-one", "step-result-one", "terminal-result-one", "input-orphan", "user-unmanaged"} {
		port.Blobs.PutObject(name, []byte(name), old)
	}
	inputHeader := nats.Header{}
	inputHeader.Set("Wf-Input-Ref", "input-one")
	invSeq, err := port.Blobs.PublishSubject("WF_INV", invSubject, inputHeader, []byte(`null`))
	if err != nil {
		return trace, err
	}
	signalHeader := nats.Header{}
	signalHeader.Set("Wf-Signal-Ref", "signal-one")
	if _, err := port.Blobs.PublishSubject("WF_SIG", "wf.sig.test.retired.go", signalHeader, []byte(`signal`)); err != nil {
		return trace, err
	}
	terminal, err := json.Marshal(wf.Outcome{InvSeq: invSeq, ResultRef: "terminal-result-one"})
	if err != nil {
		return trace, err
	}
	for index, entry := range []journal.Entry{
		{Kind: journal.Started},
		{Kind: journal.StepRequested, Payload: json.RawMessage(`{"kind":"run","name":"work"}`)},
		{Kind: journal.StepCompleted, Payload: json.RawMessage(`{"result_ref":"step-result-one"}`)},
		{Kind: journal.Completed, Payload: terminal},
	} {
		entry.Index = uint64(index)
		entry.Epoch = 1
		data, err := json.Marshal(entry)
		if err != nil {
			return trace, err
		}
		if _, err := port.Blobs.PublishSubject("WF_JRN", journalSubject, nil, data); err != nil {
			return trace, err
		}
	}
	snapshot, err := journal.NewWithSnapshotPort(nil, port, port).SnapshotPrefix(ctx, typ, id, 1)
	if err != nil {
		return trace, fmt.Errorf("seed %d snapshot prefix: %w", seed, err)
	}
	snapshotName := snapshot.Object
	snapshotBytes, err := port.GetObject(ctx, snapshotName)
	if err != nil {
		return trace, err
	}
	if _, err := port.Blobs.State().Create(ctx, key, terminal); err != nil {
		return trace, err
	}
	currentTimer := identity.TimerSubject(typ, id, invSeq, 0)
	otherTimer := identity.TimerSubject(typ, id, invSeq+100, 0)
	for _, subject := range []string{currentTimer, otherTimer} {
		if _, err := port.Blobs.PublishSubject("WF_TIMER", subject, nil, []byte(`timer`)); err != nil {
			return trace, err
		}
	}
	sweepNow := time.Unix(0, 0).UTC().Add(time.Hour)
	before, err := retention.SweepBlobsQuiescentWithPort(ctx, port.Blobs, 0, sweepNow)
	if err != nil || before.Objects != 7 || before.Referenced != 5 || before.Deleted != 1 {
		return trace, fmt.Errorf("seed %d initial blob sweep=%+v err=%v", seed, before, err)
	}
	if mode == "snapshot_delayed_read" {
		if err := port.Blobs.QueueReadFault(snapshotName); err != nil {
			return trace, err
		}
	}
	if mode == "snapshot_corrupt" {
		port.Blobs.PutObject(snapshotName, []byte(`corrupt`), old)
	}
	switch mode {
	case "marker_drop", "marker_ack_lost":
		fault := KVDropBeforeCommit
		if mode == "marker_ack_lost" {
			fault = KVLoseAckAfterCommit
		}
		if err := port.Blobs.State().QueueFault(KVFault{Operation: "put", Kind: fault}); err != nil {
			return trace, err
		}
	case "signals_drop", "signals_ack_lost", "journal_drop", "timer_drop", "purge_event_ack_lost", "invocation_ack_lost":
		operation := map[string]string{"signals_drop": "purge_WF_SIG", "signals_ack_lost": "purge_WF_SIG", "journal_drop": "purge_WF_JRN", "timer_drop": "purge_WF_TIMER", "purge_event_ack_lost": "publish_purge", "invocation_ack_lost": "purge_WF_INV"}[mode]
		fault := DropBeforeCommit
		if mode == "signals_ack_lost" || mode == "purge_event_ack_lost" || mode == "invocation_ack_lost" {
			fault = LoseAckAfterCommit
		}
		if err := port.QueueFault(PurgeFault{Operation: operation, Kind: fault}); err != nil {
			return trace, err
		}
	case "snapshot_delete_drop":
		if err := port.Blobs.State().QueueFault(KVFault{Operation: "delete", Kind: KVDropBeforeCommit}); err != nil {
			return trace, err
		}
	case "tombstone_ack_lost":
		if err := port.Blobs.State().QueueFault(KVFault{Operation: "update", Kind: KVLoseAckAfterCommit}); err != nil {
			return trace, err
		}
	}
	firstErr := retention.PurgeWithPort(ctx, port, typ, id, time.Hour)
	if mode == "clean" || mode == "tombstone_ack_lost" || mode == "missing_inv_state_read_lost" || mode == "snapshot_delayed_read" {
		if firstErr != nil {
			return trace, fmt.Errorf("seed %d mode %s first purge: %w", seed, mode, firstErr)
		}
	} else if firstErr == nil {
		return trace, fmt.Errorf("seed %d mode %s expected uncertain first purge", seed, mode)
	}
	if mode == "snapshot_delayed_read" && schedule.NowMillis() < 25 {
		return trace, fmt.Errorf("seed %d snapshot read did not retry after visibility delay", seed)
	}
	if mode == "snapshot_corrupt" {
		if !errors.Is(firstErr, journal.ErrGap) {
			return trace, fmt.Errorf("seed %d corrupt snapshot error=%v", seed, firstErr)
		}
		if _, err := port.Blobs.State().Get(ctx, "purging."+key); !errors.Is(err, jetstream.ErrKeyNotFound) {
			return trace, fmt.Errorf("seed %d corrupt snapshot allowed purge marker: %v", seed, err)
		}
		port.Blobs.PutObject(snapshotName, snapshotBytes, old)
	}
	if mode == "missing_inv_state_read_lost" {
		if err := port.Blobs.State().QueueFault(KVFault{Operation: "get", Kind: KVGetTransportLost}); err != nil {
			return trace, err
		}
	}
	if err := retention.PurgeWithPort(ctx, port, typ, id, time.Hour); err != nil {
		if mode != "missing_inv_state_read_lost" || !errors.Is(err, ErrTransportLost) || errors.Is(err, retention.ErrNotFound) {
			return trace, fmt.Errorf("seed %d mode %s retry purge: %w", seed, mode, err)
		}
	} else if mode == "missing_inv_state_read_lost" {
		return trace, fmt.Errorf("seed %d transient state read was hidden", seed)
	}
	if err := retention.PurgeWithPort(ctx, port, typ, id, time.Hour); err != nil {
		return trace, fmt.Errorf("seed %d mode %s idempotent purge: %w", seed, mode, err)
	}
	state, err := port.Blobs.State().Get(ctx, key)
	if err != nil {
		return trace, err
	}
	tombstone, tomb, err := retention.Decode(state.Value)
	if err != nil || !tomb || tombstone.InvSeq != invSeq || port.PurgeEventCount() != 1 {
		return trace, fmt.Errorf("seed %d mode %s terminal=%+v tomb=%v events=%d err=%v", seed, mode, tombstone, tomb, port.PurgeEventCount(), err)
	}
	if _, err := port.Blobs.State().Get(ctx, "purging."+key); !errors.Is(err, jetstream.ErrKeyNotFound) {
		return trace, fmt.Errorf("seed %d marker retained: %v", seed, err)
	}
	if _, err := port.Blobs.State().Get(ctx, "snap."+key); !errors.Is(err, jetstream.ErrKeyNotFound) {
		return trace, fmt.Errorf("seed %d snapshot manifest retained: %v", seed, err)
	}
	if _, err := port.Invocation(ctx, invSubject); !errors.Is(err, jetstream.ErrMsgNotFound) {
		return trace, fmt.Errorf("seed %d invocation retained: %v", seed, err)
	}
	for _, item := range []struct{ stream, subject string }{{"WF_SIG", "wf.sig.test.retired.go"}, {"WF_JRN", journalSubject}, {"WF_TIMER", currentTimer}} {
		first, last, err := port.Blobs.StreamRange(ctx, item.stream)
		if err != nil {
			return trace, err
		}
		for sequence := first; sequence != 0 && sequence <= last; sequence++ {
			message, err := port.Blobs.StreamMessage(ctx, item.stream, sequence)
			if errors.Is(err, jetstream.ErrMsgNotFound) {
				continue
			}
			if err != nil || message.Subject == item.subject {
				return trace, fmt.Errorf("seed %d retained %s/%s: %v", seed, item.stream, item.subject, err)
			}
		}
	}
	if first, _, err := port.Blobs.StreamRange(ctx, "WF_TIMER"); err != nil || first == 0 {
		return trace, fmt.Errorf("seed %d other generation timer removed: first=%d err=%v", seed, first, err)
	}
	after, err := retention.SweepBlobsQuiescentWithPort(ctx, port.Blobs, 0, sweepNow)
	if err != nil || after.Referenced != 0 || after.Deleted != 5 || port.Blobs.HasObject("user-unmanaged") == false {
		return trace, fmt.Errorf("seed %d mode %s final blob sweep=%+v err=%v", seed, mode, after, err)
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_purge_blob", Subject: key, Outcome: mode, Sequence: uint64(after.Deleted), AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededPurgeBlobReplay(t *testing.T) {
	if os.Getenv("SIM_PURGE_BLOB_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededPurgeBlob(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_PURGE_BLOB_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	observed := map[string]int{}
	for seed := range seededSchedules(t) {
		generated, err := runSeededPurgeBlob(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "purge-blob-failure.json")
			}
			_ = generated.Save(path)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		observed[generated.Decisions[0].Chosen]++
		if seed <= 10 {
			replayed, err := runSeededPurgeBlob(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d replay: %v", seed, err)
			}
		}
	}
	for _, mode := range []string{"clean", "marker_drop", "marker_ack_lost", "signals_drop", "signals_ack_lost", "journal_drop", "timer_drop", "snapshot_delete_drop", "tombstone_ack_lost", "purge_event_ack_lost", "invocation_ack_lost", "missing_inv_state_read_lost", "snapshot_delayed_read", "snapshot_corrupt"} {
		if observed[mode] == 0 {
			t.Fatalf("mode %s was not scheduled", mode)
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("purge-blob-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededPurgeBlobReplay$")
		cmd.Env = append(os.Environ(), "SIM_PURGE_BLOB_HELPER=1", "SIM_PURGE_BLOB_OUT="+files[i], "FAULT_SEED=42")
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
		t.Fatal("purge/blob trace changed across processes")
	}
}
