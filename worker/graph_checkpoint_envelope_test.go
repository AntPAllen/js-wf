package worker

import (
	"encoding/json"
	"strings"
	"testing"

	"js-wf/internal/checkpoint"
)

func TestGraphCheckpointMetadataEnvelopeAdmission(t *testing.T) {
	meta := graphCheckpointMetadata{Version: 1, Signals: map[uint64]signalRecord{1: {Sequence: 1, Name: "child"}}}
	raw, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range []string{
		strings.Replace(string(raw), `"version":1`, `"version":0,"version":1`, 1),
		strings.Replace(string(raw), `"version":1`, `"Version":1`, 1),
		strings.Replace(string(raw), `"signals":{"1":`, `"signals":{"01":{"sig_seq":1,"name":"child"},"1":`, 1),
		strings.Replace(string(raw), `"sig_seq":1`, `"Sig_Seq":1`, 1),
	} {
		if candidate == string(raw) || !json.Valid([]byte(candidate)) {
			t.Fatal("invalid metadata mutation")
		}
		var decoded graphCheckpointMetadata
		if err := checkpoint.DecodeUnambiguous([]byte(candidate), &decoded); err == nil {
			t.Fatal("ambiguous metadata admitted", candidate)
		}
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	reordered, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	var decoded graphCheckpointMetadata
	if err := checkpoint.DecodeUnambiguous(append([]byte(" \n"), reordered...), &decoded); err != nil || decoded.Version != 1 || decoded.Signals[1].Sequence != 1 {
		t.Fatal("unambiguous reordered metadata rejected", decoded, err)
	}
}
