package sim

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/worker"

	"github.com/nats-io/nats.go"
)

func runSeededWorkerSignalDrain(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("worker_signal_drain_20"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	ctx := context.Background()
	model := NewSignalTransport(schedule)
	c := client.NewWithSignalPorts(model, model)
	for i := 0; i < 20; i++ {
		mode, err := schedule.Choose([]string{"normal", "stale_generation", "purged_hole", "unrelated_gap", "blob_ref", "append_failure", "lost_publish_ack", "hash_corrupt"})
		if err != nil {
			return trace, err
		}
		id := fmt.Sprintf("case-%02d", i)
		generation, err := model.PublishInvocation(ctx, &nats.Msg{Subject: identity.InvocationSubject("test", id), Data: []byte("input")})
		if err != nil {
			return trace, err
		}
		payload := []byte(fmt.Sprintf("payload-%02d", i))
		fixture := func(name string, generation uint64, data []byte) *nats.Msg {
			message := &nats.Msg{Subject: "wf.sig.test." + id + "." + name, Data: data, Header: nats.Header{}}
			message.Header.Set("Wf-Inv-Seq", strconv.FormatUint(generation, 10))
			return message
		}
		switch mode {
		case "stale_generation":
			model.CommitSignal(fixture("old", generation+100, []byte("old")))
		case "purged_hole":
			model.PurgeSignal(model.CommitSignal(fixture("deleted", generation, []byte("deleted"))))
		case "unrelated_gap":
			model.CommitSignal(&nats.Msg{Subject: "wf.sig.test.other.go", Data: []byte("other")})
		}
		var sequence uint64
		if mode == "blob_ref" {
			key := "signal-blob-" + id
			if err := model.PutSignalBlob(ctx, key, payload); err != nil {
				return trace, err
			}
			message := fixture("go", generation, nil)
			hash := sha256.Sum256(payload)
			message.Header.Set("Wf-Signal-Ref", key)
			message.Header.Set("Wf-Input-SHA256", hex.EncodeToString(hash[:]))
			sequence = model.CommitSignal(message)
		} else if mode == "hash_corrupt" {
			message := fixture("go", generation, payload)
			message.Header.Set("Wf-Input-SHA256", strings.Repeat("0", 64))
			sequence = model.CommitSignal(message)
		} else {
			if mode == "lost_publish_ack" {
				if err := model.QueueSignalFault(SignalLoseAckAfterCommit); err != nil {
					return trace, err
				}
			}
			sequence, err = c.Signal(ctx, "test", id, "go", payload, "same")
			if mode == "lost_publish_ack" {
				if !errors.Is(err, client.ErrSignalUnknown) {
					return trace, fmt.Errorf("seed %d case %d lost signal ack: %v", seed, i, err)
				}
				stored := model.SignalFor("test", id, "go")
				if len(stored) != 1 {
					return trace, fmt.Errorf("seed %d case %d lost ack retained %d signals", seed, i, len(stored))
				}
				sequence = stored[0].Sequence
			} else if err != nil {
				return trace, err
			}
		}
		var records []journal.Record
		appendEntry := func(kind journal.Kind, data json.RawMessage) error {
			records = append(records, journal.Record{Entry: journal.Entry{Kind: kind, Index: uint64(len(records)), Payload: append([]byte(nil), data...)}})
			return nil
		}
		if mode == "append_failure" {
			_, err := worker.DrainSignalsWithPort(ctx, model, "test", id, generation, nil, func(journal.Kind, json.RawMessage) error { return ErrTransportLost })
			if !errors.Is(err, ErrTransportLost) {
				return trace, fmt.Errorf("seed %d case %d lost journal append: %v", seed, i, err)
			}
		}
		signals, err := worker.DrainSignalsWithPort(ctx, model, "test", id, generation, nil, appendEntry)
		if mode == "hash_corrupt" {
			if err == nil || !strings.Contains(err.Error(), "hash mismatch") || len(records) != 0 {
				return trace, fmt.Errorf("seed %d case %d corrupt payload: signals=%v records=%d err=%v", seed, i, signals, len(records), err)
			}
			continue
		}
		if err != nil || len(signals) != 1 || len(records) != 1 || signals[0].Sequence != sequence || signals[0].Name != "go" || !bytes.Equal(signals[0].Payload, payload) {
			return trace, fmt.Errorf("seed %d case %d mode=%s: signals=%v records=%d err=%v", seed, i, mode, signals, len(records), err)
		}
		replayed, err := worker.DrainSignalsWithPort(ctx, model, "test", id, generation, records, appendEntry)
		if err != nil || len(replayed) != 1 || len(records) != 1 || replayed[0].Sequence != sequence || !bytes.Equal(replayed[0].Payload, payload) {
			return trace, fmt.Errorf("seed %d case %d replay: signals=%v records=%d err=%v", seed, i, replayed, len(records), err)
		}
	}
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededWorkerSignalDrainReplay(t *testing.T) {
	if os.Getenv("SIM_WORKER_SIGNAL_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededWorkerSignalDrain(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_WORKER_SIGNAL_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed := int64(1); seed <= 1000; seed++ {
		generated, err := runSeededWorkerSignalDrain(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-worker-signal-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-worker-signal.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runSeededWorkerSignalDrain(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d worker signal replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("worker-signal-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededWorkerSignalDrainReplay$")
		cmd.Env = append(os.Environ(), "SIM_WORKER_SIGNAL_HELPER=1", "SIM_WORKER_SIGNAL_OUT="+files[i], "FAULT_SEED=42")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("worker signal child %d: %v: %s", i, err, output)
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
		t.Fatal("seeded worker signal trace changed across processes")
	}
}
