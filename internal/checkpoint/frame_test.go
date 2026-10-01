package checkpoint

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

func fixture() Frame {
	return Frame{
		Version: Version, Identity: Identity{Type: "parent", ID: "same-id", InvSeq: 17}, Stage: "collect_v1",
		Data:   json.RawMessage(`{"child":{"ChildType":"child","ChildID":"c-17","SignalName":"child_4"},"total":23}`),
		Anchor: Anchor{Index: 16, Epoch: 51}, StepPosition: 12,
		State:           map[string]json.RawMessage{"total": json.RawMessage(`23`)},
		ConsumedSignals: []uint64{3, 7}, CancelledTimers: []uint64{0, 8}, PanicAttempts: 1,
		PromiseOutcomes: map[string]json.RawMessage{"child_4": json.RawMessage(`{"inv_seq":22,"result_ref":"result-object","result_hash":"` + string(bytes.Repeat([]byte("a"), 64)) + `"}`)},
	}
}

func digest(raw []byte) string { s := sha256.Sum256(raw); return hex.EncodeToString(s[:]) }

func TestFrameRoundTripAndDetachedState(t *testing.T) {
	original := fixture()
	raw, hash, err := Encode(original)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decode(raw, hash, original.Identity, original.Anchor)
	if err != nil || !reflect.DeepEqual(got, original) {
		t.Fatalf("roundtrip=%+v err=%v", got, err)
	}
	// Decoder-owned state and user locals survive modifications to transport
	// buffers and the source frame; no context or derived promise cache is stored.
	for i := range raw {
		raw[i] = 0
	}
	original.State["total"][0] = '9'
	original.Data[0] = '['
	if string(got.State["total"]) != "23" || got.Data[0] != '{' {
		t.Fatal("decoded data aliases input")
	}
}

func TestFrameRejectsCorruptionBeforeReturningState(t *testing.T) {
	frame := fixture()
	raw, hash, err := Encode(frame)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name     string
		raw      []byte
		hash     string
		identity Identity
		anchor   Anchor
	}{
		{"lost-byte", raw[:len(raw)-1], hash, frame.Identity, frame.Anchor},
		{"changed-content", append([]byte(" "), raw...), hash, frame.Identity, frame.Anchor},
		{"missing-hash", raw, "", frame.Identity, frame.Anchor},
		{"reused-id", raw, hash, Identity{Type: frame.Identity.Type, ID: frame.Identity.ID, InvSeq: 18}, frame.Anchor},
		{"foreign-invocation", raw, hash, Identity{Type: "other", ID: frame.Identity.ID, InvSeq: 17}, frame.Anchor},
		{"stale-anchor", raw, hash, frame.Identity, Anchor{Index: 14, Epoch: 51}},
		{"stale-epoch", raw, hash, frame.Identity, Anchor{Index: 16, Epoch: 52}},
		{"oversized", make([]byte, MaxBytes+1), hash, frame.Identity, frame.Anchor},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Decode(tt.raw, tt.hash, tt.identity, tt.anchor)
			if !errors.Is(err, ErrInvalid) || !reflect.DeepEqual(got, Frame{}) {
				t.Fatalf("partial frame=%+v err=%v", got, err)
			}
		})
	}
}

func TestFrameRejectsInvalidSemanticsWithMatchingHash(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Frame)
	}{
		{"version", func(f *Frame) { f.Version++ }},
		{"stage", func(f *Frame) { f.Stage = "stage.*" }},
		{"generation", func(f *Frame) { f.Identity.InvSeq = 0 }},
		{"epoch", func(f *Frame) { f.Anchor.Epoch = 0 }},
		{"pending-step", func(f *Frame) { f.StepPosition = 11 }},
		{"sdk-ahead-of-journal", func(f *Frame) { f.StepPosition = 18 }},
		{"buffered-zero", func(f *Frame) { f.SignalCursor = 9; f.PendingSignals = []Signal{{Sequence: 0, Name: "input"}} }},
		{"buffered-past-cursor", func(f *Frame) { f.SignalCursor = 8; f.PendingSignals = []Signal{{Sequence: 9, Name: "input"}} }},
		{"buffered-consumed", func(f *Frame) { f.SignalCursor = 9; f.PendingSignals = []Signal{{Sequence: 7, Name: "input"}} }},
		{"buffered-order", func(f *Frame) {
			f.SignalCursor = 11
			f.PendingSignals = []Signal{{Sequence: 9, Name: "input"}, {Sequence: 8, Name: "input"}}
		}},
		{"signal-duplicate", func(f *Frame) { f.ConsumedSignals = []uint64{3, 3} }},
		{"signal-zero", func(f *Frame) { f.ConsumedSignals = []uint64{0} }},
		{"signal-order", func(f *Frame) { f.ConsumedSignals = []uint64{7, 3} }},
		{"odd-timer", func(f *Frame) { f.CancelledTimers = []uint64{3} }},
		{"impossible-attempt-count", func(f *Frame) { f.PanicAttempts = 17 }},
		{"future-timer", func(f *Frame) { f.CancelledTimers = []uint64{12} }},
		{"state-key", func(f *Frame) { f.State["invalid.key"] = json.RawMessage(`null`) }},
		{"promise-null", func(f *Frame) { f.PromiseOutcomes["child_4"] = json.RawMessage(`null`) }},
		{"promise-inline-and-ref", func(f *Frame) {
			f.PromiseOutcomes["child_4"] = json.RawMessage(`{"result":"eA==","result_ref":"blob","result_hash":"` + string(bytes.Repeat([]byte("a"), 64)) + `"}`)
		}},
		{"promise-missing-hash", func(f *Frame) { f.PromiseOutcomes["child_4"] = json.RawMessage(`{"result_ref":"blob"}`) }},
		{"promise-unknown-field", func(f *Frame) { f.PromiseOutcomes["child_4"] = json.RawMessage(`{"new_semantics":true}`) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := fixture()
			tt.change(&f)
			if _, _, err := Encode(f); !errors.Is(err, ErrInvalid) {
				t.Fatalf("encode err=%v", err)
			}
			raw, err := json.Marshal(f)
			if err != nil {
				t.Fatal(err)
			}
			got, err := Decode(raw, digest(raw), f.Identity, f.Anchor)
			if !errors.Is(err, ErrInvalid) || !reflect.DeepEqual(got, Frame{}) {
				t.Fatalf("decode=%+v err=%v", got, err)
			}
		})
	}
}

func TestFrameRejectsUnknownFieldsTrailingJSONAndInvalidState(t *testing.T) {
	f := fixture()
	raw, _, err := Encode(f)
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]byte{
		append(append([]byte(nil), raw...), []byte(` {}`)...),
		bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"unknown":true`), 1),
	} {
		if got, err := Decode(bad, digest(bad), f.Identity, f.Anchor); !errors.Is(err, ErrInvalid) || !reflect.DeepEqual(got, Frame{}) {
			t.Fatalf("decode=%+v err=%v", got, err)
		}
	}
	f.PromiseOutcomes["child_4"] = json.RawMessage(`   `)
	if _, _, err := Encode(f); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	f = fixture()
	f.State["total"] = json.RawMessage(`{`)
	if _, _, err := Encode(f); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	f = fixture()
	f.Data = json.RawMessage(`{`)
	if _, _, err := Encode(f); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	f = fixture()
	f.Data = json.RawMessage(`"` + string(bytes.Repeat([]byte("x"), MaxBytes)) + `"`)
	if _, _, err := Encode(f); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}
