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
	"strings"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/retention"
	"js-wf/worker"
)

type loseOutcomeReadAfterUpdate struct{ *KVTransport }

func (p loseOutcomeReadAfterUpdate) Update(ctx context.Context, key string, payload []byte, revision uint64) (uint64, error) {
	sequence, err := p.KVTransport.Update(ctx, key, payload, revision)
	if errors.Is(err, ErrTransportLost) {
		if faultErr := p.QueueFault(KVFault{Operation: "get", Kind: KVGetTransportLost}); faultErr != nil {
			return sequence, faultErr
		}
	}
	return sequence, err
}

func runSeededOutcomePersistence(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("outcome_persistence_20"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	ctx := context.Background()
	state := NewKVTransport(schedule, 0)
	for i := 0; i < 20; i++ {
		mode, err := schedule.Choose([]string{"normal", "drop_create", "lose_create_ack", "old_tombstone", "new_tombstone", "drop_update", "lose_update_ack", "lose_update_ack_read_lost"})
		if err != nil {
			return trace, err
		}
		id := fmt.Sprintf("outcome-%02d", i)
		key := identity.Key("test", id)
		invSeq := uint64(i + 2)
		payload := []byte(fmt.Sprintf(`{"inv_seq":%d,"result":"done"}`, invSeq))
		var marker []byte
		if mode == "old_tombstone" || mode == "new_tombstone" || mode == "drop_update" || mode == "lose_update_ack" || mode == "lose_update_ack_read_lost" {
			tombSeq := invSeq - 1
			if mode == "old_tombstone" {
				tombSeq = invSeq
			}
			marker, err = json.Marshal(retention.Tombstone{Tombstone: true, InvSeq: tombSeq, PurgedAt: time.Unix(1_700_000_000, 0).UTC(), ExpiresAt: time.Unix(1_700_000_100, 0).UTC()})
			if err != nil {
				return trace, err
			}
			if _, err := state.Create(ctx, key, marker); err != nil {
				return trace, err
			}
		}
		switch mode {
		case "drop_create":
			err = state.QueueFault(KVFault{Operation: "create", Kind: KVDropBeforeCommit})
		case "lose_create_ack":
			err = state.QueueFault(KVFault{Operation: "create", Kind: KVLoseAckAfterCommit})
		case "drop_update":
			err = state.QueueFault(KVFault{Operation: "update", Kind: KVDropBeforeCommit})
		case "lose_update_ack", "lose_update_ack_read_lost":
			err = state.QueueFault(KVFault{Operation: "update", Kind: KVLoseAckAfterCommit})
		}
		if err != nil {
			return trace, err
		}
		var port worker.OutcomePort = state
		if mode == "lose_update_ack_read_lost" {
			port = loseOutcomeReadAfterUpdate{state}
		}
		firstErr := worker.PersistOutcomeWithPort(ctx, port, "test", id, invSeq, payload)
		switch mode {
		case "old_tombstone":
			if !errors.Is(firstErr, client.ErrPurged) {
				return trace, fmt.Errorf("seed %d case %d old tombstone: %v", seed, i, firstErr)
			}
		case "drop_create", "lose_create_ack", "drop_update", "lose_update_ack_read_lost":
			if !errors.Is(firstErr, ErrTransportLost) {
				return trace, fmt.Errorf("seed %d case %d %s first attempt: %v", seed, i, mode, firstErr)
			}
		default:
			if firstErr != nil {
				return trace, fmt.Errorf("seed %d case %d %s first attempt: %w", seed, i, mode, firstErr)
			}
		}
		if mode != "old_tombstone" {
			if err := worker.PersistOutcomeWithPort(ctx, state, "test", id, invSeq, payload); err != nil {
				return trace, fmt.Errorf("seed %d case %d %s retry: %w", seed, i, mode, err)
			}
			changed := []byte(fmt.Sprintf(`{"inv_seq":%d,"result":"changed"}`, invSeq))
			if err := worker.PersistOutcomeWithPort(ctx, state, "test", id, invSeq, changed); err == nil || !strings.Contains(err.Error(), "terminal result changed") {
				return trace, fmt.Errorf("seed %d case %d %s changed terminal: %v", seed, i, mode, err)
			}
		}
		entry, err := state.Get(ctx, key)
		if err != nil {
			return trace, err
		}
		want := payload
		if mode == "old_tombstone" {
			want = marker
		}
		if !bytes.Equal(entry.Value, want) {
			return trace, fmt.Errorf("seed %d case %d %s retained value=%s want=%s", seed, i, mode, entry.Value, want)
		}
		schedule.RecordTransport(TransportEvent{Operation: "check_outcome", Subject: key, DataSHA256: digest(entry.Value), Outcome: mode, AtMillis: schedule.NowMillis()})
	}
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededOutcomePersistenceReplay(t *testing.T) {
	if os.Getenv("SIM_OUTCOME_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededOutcomePersistence(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_OUTCOME_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed := range seededSchedules(t) {
		generated, err := runSeededOutcomePersistence(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-outcome-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-outcome.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runSeededOutcomePersistence(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d outcome replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("outcome-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededOutcomePersistenceReplay$")
		cmd.Env = append(os.Environ(), "SIM_OUTCOME_HELPER=1", "SIM_OUTCOME_OUT="+files[i], "FAULT_SEED=42")
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
		t.Fatal("outcome trace changed across processes")
	}
}

var _ worker.OutcomePort = (*KVTransport)(nil)
