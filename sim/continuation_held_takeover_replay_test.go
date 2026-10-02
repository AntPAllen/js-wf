package sim

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	"js-wf/journal"
)

// The dead actor cannot perform deferred cleanup. This is an actor-lifetime
// boundary, not an injected successful KV delete reply. The underlying retained
// key is deliberately left untouched, so production Acquire must wait for TTL.
type continuationKilledActorKV struct {
	*KVTransport
	dead bool
}

func (p *continuationKilledActorKV) Delete(ctx context.Context, key string, rev uint64) error {
	if p.dead {
		p.schedule.RecordTransport(TransportEvent{Operation: "dead_actor_cleanup_suppressed", Subject: key, Sequence: rev, AtMillis: p.schedule.NowMillis()})
		return nil
	}
	return p.KVTransport.Delete(ctx, key, rev)
}

type continuationTakeoverCut struct {
	*continuationLimitJournal
	kind  journal.Kind
	stop  func()
	actor *continuationKilledActorKV
	fired bool
}

func (p *continuationTakeoverCut) Publish(ctx context.Context, subject string, data []byte, expected uint64) (uint64, error) {
	seq, err := p.continuationLimitJournal.Publish(ctx, subject, data, expected)
	if err != nil {
		return seq, err
	}
	var entry journal.Entry
	if err := json.Unmarshal(data, &entry); err != nil {
		return seq, err
	}
	if !p.fired && entry.Kind == p.kind {
		p.fired = true
		p.actor.dead = true
		p.actor.schedule.RecordTransport(TransportEvent{Operation: "continuation_actor_stopped", Subject: subject, Sequence: seq, Outcome: string(entry.Kind), AtMillis: p.actor.schedule.NowMillis()})
		p.stop()
		return 0, context.Canceled
	}
	return seq, nil
}

func runSeededContinuationHeldTakeover(seed int64, replay *Trace) (Trace, error) {
	return runContinuationLimitScenario(seed, replay, true)
}
func TestSeededContinuationHeldTakeoverReplay(t *testing.T) {
	if os.Getenv("SIM_CONTINUATION_HELD_HELPER") == "1" {
		trace, err := runSeededContinuationHeldTakeover(42, nil)
		if saveErr := trace.Save(os.Getenv("SIM_CONTINUATION_HELD_OUT")); saveErr != nil {
			t.Fatal(saveErr)
		}
		if err != nil {
			t.Fatal(err)
		}
		return
	}
	covered := map[string]bool{}
	for seed := range seededSchedules(t) {
		trace, err := runSeededContinuationHeldTakeover(seed, nil)
		if err != nil {
			dir, saveErr := os.MkdirTemp("", "js-wf-continuation-held-failure-")
			if saveErr != nil {
				t.Fatalf("seed=%d err=%v save=%v", seed, err, saveErr)
			}
			path := filepath.Join(dir, "trace.json")
			if saveErr := trace.Save(path); saveErr != nil {
				t.Fatalf("seed=%d err=%v save=%v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		key := trace.Decisions[0].Chosen + ":" + trace.Decisions[1].Chosen + ":" + trace.Decisions[2].Chosen
		covered[key] = true
		if seed <= 10 {
			replay, err := runSeededContinuationHeldTakeover(seed, &trace)
			if err != nil || !reflect.DeepEqual(trace, replay) {
				t.Fatalf("seed=%d replay: %v", seed, err)
			}
		}
		if seed == 42 && os.Getenv("SIM_CONTINUATION_HELD_OUT") != "" {
			if err := trace.Save(os.Getenv("SIM_CONTINUATION_HELD_OUT")); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(covered) != 27 {
		t.Fatalf("covered %d/27 cut/budget/heal combinations", len(covered))
	}
	var paths [2]string
	for i := range paths {
		paths[i] = filepath.Join(t.TempDir(), "trace.json")
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededContinuationHeldTakeoverReplay$")
		cmd.Env = append(os.Environ(), "SIM_CONTINUATION_HELD_HELPER=1", "SIM_CONTINUATION_HELD_OUT="+paths[i])
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child=%d err=%v output=%s", i, err, output)
		}
	}
	first, err := os.ReadFile(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(paths[1])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("held takeover trace differs across processes")
	}

}
