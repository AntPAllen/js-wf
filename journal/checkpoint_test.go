package journal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"

	"js-wf/internal/checkpoint"
)

type checkpointProbe struct {
	SnapshotWritePort
	data  []byte
	reads int
}

func (p *checkpointProbe) GetObject(context.Context, string) ([]byte, error) {
	p.reads++
	return p.data, nil
}

func TestRuntimeCheckpointBindsActualAnchorAndSDKCounter(t *testing.T) {
	// SignalConsumed advances journal index without advancing SDK position.
	frame := checkpoint.Frame{Version: 1, Identity: checkpoint.Identity{Type: "test", ID: "probe", InvSeq: 17}, Stage: "next_v1", Data: json.RawMessage(`23`), Anchor: checkpoint.Anchor{Index: 5, Epoch: 51}, StepPosition: 4}
	raw, hash, err := checkpoint.Encode(frame)
	if err != nil {
		t.Fatal(err)
	}
	input := sha256.Sum256(frame.Data)
	request, _ := json.Marshal(map[string]string{"kind": "checkpoint", "name": "next_v1", "input_hash": hex.EncodeToString(input[:])})
	done, _ := json.Marshal(map[string]string{"result_ref": "step-result-" + hash, "result_hash": hash})
	kinds := []Kind{Started, StepRequested, StepCompleted, SignalConsumed, StepRequested, StepCompleted}
	records := make([]Record, len(kinds))
	for i, kind := range kinds {
		records[i] = Record{Entry: Entry{Index: uint64(i), Epoch: 51, Kind: kind}, Sequence: uint64(10 + i)}
	}
	records[4].Payload = request
	records[5].Payload = done
	runtime := RuntimeCheckpoint{InvSeq: 17, Stage: "next_v1", Sequence: 15, Index: 5, Epoch: 51, StepPosition: 4, Object: "step-result-" + hash, SHA256: hash}
	port := &checkpointProbe{data: raw}
	store := &Store{snapshotWritePort: port}
	if err := store.verifyRuntimeCheckpoint(context.Background(), "test", "probe", records, runtime); err != nil || port.reads != 1 {
		t.Fatalf("valid checkpoint err=%v reads=%d", err, port.reads)
	}
	for _, test := range []struct {
		name   string
		change func(*RuntimeCheckpoint)
	}{
		{"sequence", func(r *RuntimeCheckpoint) { r.Sequence++ }},
		{"epoch", func(r *RuntimeCheckpoint) { r.Epoch++ }},
		{"index", func(r *RuntimeCheckpoint) { r.Index = 4 }},
		{"position", func(r *RuntimeCheckpoint) { r.StepPosition = 2 }},
		{"stage", func(r *RuntimeCheckpoint) { r.Stage = "other_v1" }},
		{"object", func(r *RuntimeCheckpoint) { r.Object = "unmanaged" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := runtime
			test.change(&changed)
			port.reads = 0
			if err := store.verifyRuntimeCheckpoint(context.Background(), "test", "probe", records, changed); !errors.Is(err, ErrGap) || port.reads != 0 {
				t.Fatalf("err=%v reads=%d", err, port.reads)
			}
		})
	}
	foreign := runtime
	foreign.InvSeq++
	if err := store.verifyRuntimeCheckpoint(context.Background(), "test", "probe", records, foreign); !errors.Is(err, ErrGap) {
		t.Fatal(err)
	}
	// An intact frame and matching completion hash cannot change the declared
	// checkpoint input. This failure occurs after the frame read, before writes.
	records[4].Payload = json.RawMessage(`{"kind":"checkpoint","name":"next_v1","input_hash":"wrong"}`)
	if err := store.verifyRuntimeCheckpoint(context.Background(), "test", "probe", records, runtime); !errors.Is(err, ErrGap) {
		t.Fatal(err)
	}
	records[4].Payload = request
	records[2].Kind = StepRequested
	port.reads = 0
	if err := store.verifyRuntimeCheckpoint(context.Background(), "test", "probe", records, runtime); !errors.Is(err, ErrGap) || port.reads != 0 {
		t.Fatalf("err=%v reads=%d", err, port.reads)
	}
}

func TestRuntimeSnapshotPurgeBoundAndLimit(t *testing.T) {
	if err := ValidateRuntimeSnapshot(Snapshot{Version: 2}); !errors.Is(err, ErrGap) {
		t.Fatal(err)
	}
	hash := hex.EncodeToString(make([]byte, 32))
	runtime := RuntimeCheckpoint{InvSeq: 17, Stage: "next_v1", Sequence: 15, Index: 5, Epoch: 51, StepPosition: 4, Object: "step-result-" + hash, SHA256: hash}
	snap := Snapshot{Version: 2, LastSeq: 14, LastIndex: 4, Runtime: &runtime}
	if err := ValidateRuntimeSnapshot(snap); err != nil {
		t.Fatal(err)
	}
	snap.Version = 1
	if err := ValidateRuntimeSnapshot(snap); !errors.Is(err, ErrGap) {
		t.Fatal(err)
	}
	snap.Version = 2
	snap.LastSeq = 15
	if err := ValidateRuntimeSnapshot(snap); !errors.Is(err, ErrGap) {
		t.Fatal(err)
	}
	snap.LastSeq = 14
	snap.LastIndex = 5
	if err := ValidateRuntimeSnapshot(snap); !errors.Is(err, ErrGap) {
		t.Fatal(err)
	}
	snap.LastIndex = 4
	runtime.Index = MaxEntries
	if err := ValidateRuntimeSnapshot(snap); !errors.Is(err, ErrGap) {
		t.Fatal(err)
	}
}
