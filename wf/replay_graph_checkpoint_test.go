package wf

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"js-wf/internal/checkpoint"
	"js-wf/journal"
)

func TestReplayGraphCheckpointMetadata(t *testing.T) {
	hash := func(raw []byte) string { sum := sha256.Sum256(raw); return hex.EncodeToString(sum[:]) }
	type fixture struct {
		records []journal.Record
		objects map[string][]byte
	}
	makeFixture := func(buffered bool) fixture {
		marshal := func(value any) json.RawMessage {
			raw, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			return raw
		}
		r := []journal.Record{{Entry: journal.Entry{Kind: journal.Started}}, {Entry: journal.Entry{Kind: journal.StepRequested, Payload: marshal(map[string]any{"kind": "call_async", "name": "child_0", "child_type": "child", "child_id": "child-id"})}}, {Entry: journal.Entry{Kind: journal.StepCompleted, Payload: marshal(map[string]any{"result": []byte(`null`)})}}}
		child := replayCheckpointChild{Type: "child", ID: "child-id"}
		frame := checkpoint.Frame{Version: 1, Identity: checkpoint.Identity{Type: "parent", ID: "id", InvSeq: 1}, Stage: "finish_v1", Data: json.RawMessage(`7`), StepPosition: 4}
		meta := replayCheckpointMetadata{Version: 1, Identity: frame.Identity, Children: map[string]replayCheckpointChild{"child_0": child}, Signals: map[uint64]replayCheckpointSignal{}}
		if buffered {
			outcome := marshal(Outcome{InvSeq: 2, Result: []byte(`42`)})
			resolved := child
			resolved.Invocation = 2
			event := replayCheckpointSignal{Sequence: 1, Name: "child_0", Payload: outcome, Child: &resolved}
			event.Canonical = &struct {
				Index uint64 `json:"index"`
				Token string `json:"token"`
			}{Token: "owned"}
			r = append(r, journal.Record{Entry: journal.Entry{Kind: journal.SignalConsumed, Payload: marshal(event)}})
			frame.SignalCursor = 1
			frame.PendingSignals = []checkpoint.Signal{{Sequence: 1, Name: "child_0", Payload: outcome}}
			meta.SignalNext = 1
			meta.SignalLast = 1
			meta.Signals[1] = event
		}
		r = append(r, journal.Record{Entry: journal.Entry{Kind: journal.StepRequested, Payload: marshal(map[string]any{"kind": "checkpoint", "name": "finish_v1", "input_hash": hash(frame.Data)})}})
		frame.Anchor = checkpoint.Anchor{Index: uint64(len(r)), Epoch: 1}
		raw, digest, err := checkpoint.Encode(frame)
		if err != nil {
			t.Fatal(err)
		}
		meta.Anchor = frame.Anchor
		meta.FrameHash = digest
		metadata := marshal(meta)
		r = append(r, journal.Record{Entry: journal.Entry{Kind: journal.StepCompleted, Payload: marshal(map[string]any{"result_ref": "frame", "result_hash": digest, "checkpoint_metadata_ref": "metadata", "checkpoint_metadata_hash": hash(metadata)})}})
		for i := range r {
			r[i].Index = uint64(i)
			r[i].Epoch = 1
			r[i].Sequence = uint64(i + 1)
		}
		return fixture{r, map[string][]byte{"frame": raw, "metadata": metadata}}
	}
	mutateMeta := func(f *fixture, change func(*replayCheckpointMetadata)) {
		var meta replayCheckpointMetadata
		if err := json.Unmarshal(f.objects["metadata"], &meta); err != nil {
			t.Fatal(err)
		}
		change(&meta)
		raw, _ := json.Marshal(meta)
		f.objects["metadata"] = raw
		var fields map[string]any
		_ = json.Unmarshal(f.records[len(f.records)-1].Payload, &fields)
		fields["checkpoint_metadata_hash"] = hash(raw)
		f.records[len(f.records)-1].Payload, _ = json.Marshal(fields)
	}
	mutateCompletion := func(f *fixture, change func(map[string]any)) {
		var fields map[string]any
		_ = json.Unmarshal(f.records[len(f.records)-1].Payload, &fields)
		change(fields)
		f.records[len(f.records)-1].Payload, _ = json.Marshal(fields)
	}
	mutateRaw := func(f *fixture, change func([]byte) []byte) {
		f.objects["metadata"] = change(f.objects["metadata"])
		mutateCompletion(f, func(e map[string]any) { e["checkpoint_metadata_hash"] = hash(f.objects["metadata"]) })
	}
	tests := []struct {
		name   string
		change func(*fixture)
		want   error
	}{
		{"valid", func(*fixture) {}, nil},
		{"duplicate_completion_metadata_reference", func(f *fixture) {
			at := len(f.records) - 1
			f.records[at].Payload = append([]byte(`{"checkpoint_metadata_ref":"foreign",`), f.records[at].Payload[1:]...)
		}, ErrCorruptJournal},
		{"duplicate_completion_metadata_hash", func(f *fixture) {
			at := len(f.records) - 1
			f.records[at].Payload = append([]byte(`{"checkpoint_metadata_hash":"foreign",`), f.records[at].Payload[1:]...)
		}, ErrCorruptJournal},

		{"legacy", func(f *fixture) {
			mutateCompletion(f, func(e map[string]any) { delete(e, "checkpoint_metadata_ref"); delete(e, "checkpoint_metadata_hash") })
		}, nil},
		{"missing_metadata", func(f *fixture) { delete(f.objects, "metadata") }, ErrReplayObjectMissing},
		{"changed_bytes", func(f *fixture) { f.objects["metadata"] = append(f.objects["metadata"], ' ') }, ErrCorruptJournal},
		{"missing_hash", func(f *fixture) {
			mutateCompletion(f, func(e map[string]any) { delete(e, "checkpoint_metadata_hash") })
		}, ErrCorruptJournal},
		{"missing_reference", func(f *fixture) { mutateCompletion(f, func(e map[string]any) { delete(e, "checkpoint_metadata_ref") }) }, ErrCorruptJournal},
		{"wrong_hash", func(f *fixture) {
			mutateCompletion(f, func(e map[string]any) { e["checkpoint_metadata_hash"] = hash([]byte(`wrong`)) })
		}, ErrCorruptJournal},
		{"null_reference", func(f *fixture) { mutateCompletion(f, func(e map[string]any) { e["checkpoint_metadata_ref"] = nil }) }, ErrCorruptJournal},
		{"wrong_version", func(f *fixture) { mutateMeta(f, func(m *replayCheckpointMetadata) { m.Version = 99 }) }, ErrCorruptJournal},
		{"wrong_type", func(f *fixture) { mutateMeta(f, func(m *replayCheckpointMetadata) { m.Identity.Type = "foreign" }) }, ErrCorruptJournal},
		{"wrong_id", func(f *fixture) { mutateMeta(f, func(m *replayCheckpointMetadata) { m.Identity.ID = "foreign" }) }, ErrCorruptJournal},
		{"wrong_invocation", func(f *fixture) { mutateMeta(f, func(m *replayCheckpointMetadata) { m.Identity.InvSeq++ }) }, ErrCorruptJournal},
		{"wrong_anchor", func(f *fixture) { mutateMeta(f, func(m *replayCheckpointMetadata) { m.Anchor.Index++ }) }, ErrCorruptJournal},
		{"wrong_epoch", func(f *fixture) { mutateMeta(f, func(m *replayCheckpointMetadata) { m.Anchor.Epoch++ }) }, ErrCorruptJournal},
		{"wrong_frame_hash", func(f *fixture) {
			mutateMeta(f, func(m *replayCheckpointMetadata) { m.FrameHash = hash([]byte(`wrong`)) })
		}, ErrCorruptJournal},
		{"wrong_signal_next", func(f *fixture) { mutateMeta(f, func(m *replayCheckpointMetadata) { m.SignalNext++ }) }, ErrCorruptJournal},
		{"wrong_signal_last", func(f *fixture) { mutateMeta(f, func(m *replayCheckpointMetadata) { m.SignalLast++ }) }, ErrCorruptJournal},
		{"missing_child", func(f *fixture) { mutateMeta(f, func(m *replayCheckpointMetadata) { delete(m.Children, "child_0") }) }, ErrCorruptJournal},
		{"foreign_child", func(f *fixture) {
			mutateMeta(f, func(m *replayCheckpointMetadata) {
				m.Children["child_0"] = replayCheckpointChild{Type: "foreign", ID: "child-id"}
			})
		}, ErrCorruptJournal},
		{"resolved_child_declaration", func(f *fixture) {
			mutateMeta(f, func(m *replayCheckpointMetadata) {
				m.Children["child_0"] = replayCheckpointChild{Type: "child", ID: "child-id", Invocation: 2}
			})
		}, ErrCorruptJournal},
		{"extra_child", func(f *fixture) {
			mutateMeta(f, func(m *replayCheckpointMetadata) {
				m.Children["foreign"] = replayCheckpointChild{Type: "child", ID: "child-id"}
			})
		}, ErrCorruptJournal},
		{"extra_signal", func(f *fixture) {
			mutateMeta(f, func(m *replayCheckpointMetadata) {
				m.Signals[99] = replayCheckpointSignal{Sequence: 99, Name: "foreign"}
			})
		}, ErrCorruptJournal},
		{"duplicate_version", func(f *fixture) {
			mutateRaw(f, func(raw []byte) []byte { return append([]byte(`{"version":1,`), raw[1:]...) })
		}, ErrCorruptJournal},
		{"alias_version", func(f *fixture) {
			mutateRaw(f, func(raw []byte) []byte { return append([]byte(`{"Version":1,`), raw[1:]...) })
		}, ErrCorruptJournal},
		{"unknown_field", func(f *fixture) {
			mutateRaw(f, func(raw []byte) []byte { return append([]byte(`{"future":1,`), raw[1:]...) })
		}, ErrCorruptJournal},
		{"non_checkpoint", func(f *fixture) {
			var e map[string]any
			_ = json.Unmarshal(f.records[len(f.records)-2].Payload, &e)
			e["kind"] = "run"
			f.records[len(f.records)-2].Payload, _ = json.Marshal(e)
		}, ErrCorruptJournal},
		{"missing_frame", func(f *fixture) { delete(f.objects, "frame") }, ErrReplayObjectMissing},
		{"changed_frame", func(f *fixture) { f.objects["frame"] = append(f.objects["frame"], ' ') }, ErrCorruptJournal},
	}
	for _, buffered := range []bool{false, true} {
		for _, test := range tests {
			t.Run(fmt.Sprintf("buffered=%t/%s", buffered, test.name), func(t *testing.T) {
				f := makeFixture(buffered)
				test.change(&f)
				err := ValidateReplayGraphCheckpoints(f.records, f.objects, "parent", "id", 1)
				if !errors.Is(err, test.want) {
					t.Fatalf("metadata validation=%v want=%v", err, test.want)
				}
				if test.want != nil {
					raw, _ := json.Marshal(f.records)
					calls := 0
					_, err = Replay(raw, func(*Context) (int, error) { calls++; return 42, nil }, ReplayOptions{Type: "parent", ID: "id", InvSeq: 1, Objects: f.objects})
					if !errors.Is(err, test.want) || calls != 0 {
						t.Fatalf("SDK err=%v calls=%d", err, calls)
					}
				}
			})
		}
		if buffered {
			for _, mode := range []string{"missing_buffered", "changed_payload", "changed_token", "changed_binding", "wrong_sequence", "wrong_child"} {
				t.Run("buffered=true/"+mode, func(t *testing.T) {
					f := makeFixture(true)
					mutateMeta(&f, func(m *replayCheckpointMetadata) {
						e := m.Signals[1]
						switch mode {
						case "missing_buffered":
							delete(m.Signals, 1)
							return
						case "changed_payload":
							e.Payload = []byte(`wrong`)
						case "changed_token":
							e.Canonical.Token = "foreign"
						case "changed_binding":
							e.Canonical.Index++
						case "wrong_sequence":
							e.Sequence++
						case "wrong_child":
							e.Child.ID = "foreign"
						}
						m.Signals[1] = e
					})
					if err := ValidateReplayGraphCheckpoints(f.records, f.objects, "parent", "id", 1); !errors.Is(err, ErrCorruptJournal) {
						t.Fatal(err)
					}
				})
			}
		}
	}
}
