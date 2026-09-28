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

	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/reconcile"
	"js-wf/worker"

	"github.com/nats-io/nats.go"
)

func runSeededSignalPipeline(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("signal_pipeline_20"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	ctx := context.Background()
	model := NewSignalTransport(schedule)
	journalModel := NewJournalTransport(schedule)
	journalStore := journal.NewWithAppendPort(journalModel)
	c := client.NewWithSignalPorts(model, model)
	scan := reconcile.NewSignalScanWithPort(model)
	const typ, id = "test", "pipeline"
	generation, err := model.PublishInvocation(ctx, &nats.Msg{Subject: identity.InvocationSubject(typ, id), Data: []byte("input")})
	if err != nil {
		return trace, err
	}
	sequences := make([]uint64, 0, 20)
	payloads := make([][]byte, 0, 20)
	for i := 0; i < 20; i++ {
		mode, err := schedule.Choose([]string{"normal", "duplicate", "lost_publish_ack", "lost_enqueue_ack", "dropped_enqueue"})
		if err != nil {
			return trace, err
		}
		payload := []byte(fmt.Sprintf("signal-%02d", i))
		key := fmt.Sprintf("key-%02d", i)
		call := func() (uint64, error) { return c.Signal(ctx, typ, id, "go", payload, key) }
		if mode == "lost_publish_ack" {
			if err := model.QueueSignalFault(SignalLoseAckAfterCommit); err != nil {
				return trace, err
			}
		} else if mode == "lost_enqueue_ack" || mode == "dropped_enqueue" {
			kind := "lose_ack_after_commit"
			if mode == "dropped_enqueue" {
				kind = "drop_before_commit"
			}
			if err := model.QueueFault(StartFault{Operation: "enqueue_run", Kind: kind}); err != nil {
				return trace, err
			}
		}
		sequence, err := call()
		switch mode {
		case "lost_publish_ack":
			if !errors.Is(err, client.ErrSignalUnknown) {
				return trace, fmt.Errorf("seed %d signal %d lost publish: %v", seed, i, err)
			}
			stored := model.SignalFor(typ, id, "go")
			if len(stored) != i+1 {
				return trace, fmt.Errorf("seed %d signal %d committed count=%d", seed, i, len(stored))
			}
			sequence = stored[i].Sequence
		case "lost_enqueue_ack", "dropped_enqueue":
			if !errors.Is(err, client.ErrEnqueueUnknown) || sequence == 0 {
				return trace, fmt.Errorf("seed %d signal %d uncertain enqueue seq=%d err=%v", seed, i, sequence, err)
			}
		default:
			if err != nil || sequence == 0 {
				return trace, fmt.Errorf("seed %d signal %d mode=%s seq=%d err=%v", seed, i, mode, sequence, err)
			}
		}
		if mode == "lost_publish_ack" || mode == "lost_enqueue_ack" || mode == "dropped_enqueue" {
			if _, err := scan.Scan(ctx, sequence, 1, false); err != nil {
				return trace, err
			}
		}
		if mode == "duplicate" || mode == "lost_publish_ack" || mode == "lost_enqueue_ack" || mode == "dropped_enqueue" {
			if again, err := call(); err != nil || again != sequence {
				return trace, fmt.Errorf("seed %d signal %d retry seq=%d first=%d err=%v", seed, i, again, sequence, err)
			}
		}
		sequences = append(sequences, sequence)
		payloads = append(payloads, payload)
		if got := len(model.Runs()); got != i+1 {
			return trace, fmt.Errorf("seed %d signal %d mode=%s retained %d runs", seed, i, mode, got)
		}
	}
	journalSubject := identity.JournalSubject(typ, id)
	startSeq, err := journalStore.Append(ctx, typ, id, journal.Entry{Kind: journal.Started, Index: 0, Epoch: 1}, 0)
	if err != nil {
		return trace, fmt.Errorf("seed %d journal start: %w", seed, err)
	}
	records := []journal.Record{{Entry: journal.Entry{Kind: journal.Started, Index: 0, Epoch: 1}, Sequence: startSeq}}
	journalSeq := startSeq
	faultChoices := make([]string, 20)
	for i := range faultChoices {
		faultChoices[i] = strconv.Itoa(i)
	}
	faultChoice, err := schedule.Choose(faultChoices)
	if err != nil {
		return trace, err
	}
	faultAt, _ := strconv.Atoi(faultChoice)
	faulted := false
	appendEntry := func(kind journal.Kind, payload json.RawMessage) error {
		if !faulted && len(records)-1 == faultAt {
			faulted = true
			if err := journalModel.QueueFault(Fault{Kind: LoseAckAfterCommit}); err != nil {
				return err
			}
		}
		entry := journal.Entry{Kind: kind, Index: uint64(len(records)), Epoch: 1, Payload: append([]byte(nil), payload...)}
		seq, err := journalStore.Append(ctx, typ, id, entry, journalSeq)
		if err != nil {
			return err
		}
		journalSeq = seq
		records = append(records, journal.Record{Entry: entry, Sequence: seq})
		return nil
	}
	signals, err := worker.DrainSignalsWithPort(ctx, model, typ, id, generation, records, appendEntry)
	if !errors.Is(err, journal.ErrUnknown) || !faulted {
		return trace, fmt.Errorf("seed %d lost journal ack at %d: %v", seed, faultAt, err)
	}
	// Redelivery reads the committed journal tail before draining again.
	records = nil
	for _, message := range journalModel.Messages(journalSubject) {
		var entry journal.Entry
		if err := json.Unmarshal(message.Data, &entry); err != nil {
			return trace, err
		}
		records = append(records, journal.Record{Entry: entry, Sequence: message.Sequence})
		journalSeq = message.Sequence
	}
	signals, err = worker.DrainSignalsWithPort(ctx, model, typ, id, generation, records, appendEntry)
	if err != nil || len(signals) != 20 || len(records) != 21 {
		return trace, fmt.Errorf("seed %d drain signals=%d records=%d err=%v", seed, len(signals), len(records), err)
	}
	retained := journalModel.Messages(journalSubject)
	if len(retained) != len(records) {
		return trace, fmt.Errorf("seed %d retained journal=%d records=%d", seed, len(retained), len(records))
	}
	for i, message := range retained {
		encoded, err := json.Marshal(records[i].Entry)
		if err != nil || message.Sequence != records[i].Sequence || !bytes.Equal(message.Data, encoded) {
			return trace, fmt.Errorf("seed %d journal entry %d retained=%+v record=%+v err=%v", seed, i, message, records[i], err)
		}
	}
	for i, signal := range signals {
		if signal.Sequence != sequences[i] || signal.Name != "go" || !bytes.Equal(signal.Payload, payloads[i]) {
			return trace, fmt.Errorf("seed %d signal %d drained=%+v want seq=%d payload=%q", seed, i, signal, sequences[i], payloads[i])
		}
	}
	model.SetJournal(typ, id, records)
	for i := range sequences {
		purge, err := schedule.Choose([]string{"keep", "purge"})
		if err != nil {
			return trace, err
		}
		if purge != "purge" {
			continue
		}
		model.PurgeSignal(sequences[i])
		key := fmt.Sprintf("key-%02d", i)
		if again, err := c.Signal(ctx, typ, id, "go", payloads[i], key); err != nil || again != sequences[i] {
			return trace, fmt.Errorf("seed %d purged signal %d retry seq=%d want=%d err=%v", seed, i, again, sequences[i], err)
		}
		if _, err := c.Signal(ctx, typ, id, "go", []byte("changed"), key); !errors.Is(err, client.ErrSignalMismatch) {
			return trace, fmt.Errorf("seed %d purged signal %d changed retry: %v", seed, i, err)
		}
	}
	replayed, err := worker.DrainSignalsWithPort(ctx, model, typ, id, generation, records, appendEntry)
	if err != nil || len(replayed) != 20 || len(records) != 21 || len(journalModel.Messages(journalSubject)) != 21 {
		return trace, fmt.Errorf("seed %d replay signals=%d records=%d err=%v", seed, len(replayed), len(records), err)
	}
	before := len(model.Runs())
	result, err := scan.Scan(ctx, 1, 20, false)
	if err != nil || result.Reenqueued != 0 || len(model.Runs()) != before {
		return trace, fmt.Errorf("seed %d post-consumption scan=%+v runs=%d/%d err=%v", seed, result, len(model.Runs()), before, err)
	}
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededSignalPipelineReplay(t *testing.T) {
	if os.Getenv("SIM_SIGNAL_PIPELINE_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededSignalPipeline(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_SIGNAL_PIPELINE_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed := int64(1); seed <= 1000; seed++ {
		generated, err := runSeededSignalPipeline(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-signal-pipeline-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-signal-pipeline.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runSeededSignalPipeline(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d signal pipeline replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("signal-pipeline-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededSignalPipelineReplay$")
		cmd.Env = append(os.Environ(), "SIM_SIGNAL_PIPELINE_HELPER=1", "SIM_SIGNAL_PIPELINE_OUT="+files[i], "FAULT_SEED=42")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("signal pipeline child %d: %v: %s", i, err, output)
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
		t.Fatal("seeded signal pipeline trace changed across processes")
	}
}
