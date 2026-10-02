package protocol_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"js-wf/journal"
	"js-wf/protocol"
	protocolv1 "js-wf/protocol/v1"
	"js-wf/wf"
)

func TestRecordRoundTrip(t *testing.T) {
	for _, kind := range []journal.Kind{journal.Started, journal.StepRequested, journal.StepCompleted, journal.Suspended, journal.SignalConsumed, journal.Attempt, journal.Completed, journal.Failed} {
		for _, payload := range []json.RawMessage{nil, []byte("null"), []byte(" {\"integer\":18446744073709551615,\"text\":\"λ\"} \n")} {
			want := journal.Record{Entry: journal.Entry{Epoch: math.MaxUint64, Index: 1<<53 + 1, Kind: kind, Payload: payload, WorkerID: "worker-λ"}, Sequence: math.MaxUint64 - 1}
			wire, err := protocol.MarshalRecord(want)
			if err != nil {
				t.Fatal(err)
			}
			got, err := protocol.UnmarshalRecord(wire)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("got %#v want %#v", got, want)
			}
			again, err := protocol.MarshalRecord(want)
			if err != nil || !bytes.Equal(wire, again) {
				t.Fatal("unstable local encoding", err)
			}
		}
	}
}

func TestRecordRejectsUnsupportedOrMalformed(t *testing.T) {
	valid := func() *protocolv1.JournalRecord {
		return &protocolv1.JournalRecord{Version: 1, Entry: &protocolv1.JournalEntry{Kind: protocolv1.EntryKind_ENTRY_KIND_STARTED, PayloadJson: []byte("null")}}
	}
	for name, mutate := range map[string]func(*protocolv1.JournalRecord){
		"version":      func(m *protocolv1.JournalRecord) { m.Version = 2 },
		"no entry":     func(m *protocolv1.JournalRecord) { m.Entry = nil },
		"kind":         func(m *protocolv1.JournalRecord) { m.Entry.Kind = 99 },
		"unspecified":  func(m *protocolv1.JournalRecord) { m.Entry.Kind = 0 },
		"json":         func(m *protocolv1.JournalRecord) { m.Entry.PayloadJson = []byte("{") },
		"utf8 payload": func(m *protocolv1.JournalRecord) { m.Entry.PayloadJson = []byte{'"', 255, '"'} },
		"utf8 worker":  func(m *protocolv1.JournalRecord) { m.Entry.WorkerId = string([]byte{255}) },
		"envelope extension": func(m *protocolv1.JournalRecord) {
			m.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 90, protowire.VarintType), 1))
		},
		"entry extension": func(m *protocolv1.JournalRecord) {
			m.Entry.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 90, protowire.VarintType), 1))
		},
	} {
		t.Run(name, func(t *testing.T) {
			m := valid()
			mutate(m)
			if _, err := protocol.FromMessage(m); !errors.Is(err, protocol.ErrInvalidRecord) {
				t.Fatal(err)
			}
		})
	}
	if _, err := protocol.FromMessage(nil); !errors.Is(err, protocol.ErrInvalidRecord) {
		t.Fatal(err)
	}
	for _, wire := range [][]byte{nil, {0xff}, {0x1a, 0x80}} {
		if _, err := protocol.UnmarshalRecord(wire); !errors.Is(err, protocol.ErrInvalidRecord) {
			t.Fatal(err)
		}
	}
}

func TestPayloadOwnership(t *testing.T) {
	record := journal.Record{Entry: journal.Entry{Kind: journal.Started, Payload: []byte("null")}}
	message, err := protocol.ToMessage(record)
	if err != nil {
		t.Fatal(err)
	}
	record.Payload[0] = 'x'
	if string(message.Entry.PayloadJson) != "null" {
		t.Fatal("ToMessage aliases input")
	}
	result, err := protocol.FromMessage(message)
	if err != nil {
		t.Fatal(err)
	}
	message.Entry.PayloadJson[0] = 'x'
	if string(result.Payload) != "null" {
		t.Fatal("FromMessage aliases input")
	}
}

// Exercise the actual SDK producer and recovery decisions after interchange.
func TestSDKRecoveryStates(t *testing.T) {
	var history []wf.Entry
	producer := wf.NewContext(context.Background(), nil, func(_ context.Context, kind wf.Kind, payload json.RawMessage) error {
		history = append(history, wf.Entry{Index: uint64(len(history)), Kind: kind, Payload: bytes.Clone(payload)})
		return nil
	})
	result, err := wf.Run(producer, "charge", 42, func(context.Context) (int, error) { return 77, nil })
	if err != nil || result != 77 || len(history) != 2 {
		t.Fatal(result, err, history)
	}
	for _, tc := range []struct {
		name             string
		entries          []wf.Entry
		effects, appends int
		wantErr          error
	}{
		{"neither", nil, 1, 2, nil}, {"request", history[:1], 1, 1, nil}, {"both", history, 0, 0, nil}, {"completion", history[1:], 0, 0, wf.ErrCorruptJournal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var decoded []wf.Entry
			for _, e := range tc.entries {
				wire, err := protocol.MarshalRecord(journal.Record{Entry: journal.Entry{Epoch: 7, Index: e.Index, Kind: journal.Kind(e.Kind), Payload: e.Payload}})
				if err != nil {
					t.Fatal(err)
				}
				r, err := protocol.UnmarshalRecord(wire)
				if err != nil {
					t.Fatal(err)
				}
				decoded = append(decoded, wf.Entry{Index: r.Index, Kind: wf.Kind(r.Kind), Payload: r.Payload})
			}
			effects, appends := 0, 0
			c := wf.NewContext(context.Background(), decoded, func(context.Context, wf.Kind, json.RawMessage) error { appends++; return nil })
			got, err := wf.Run(c, "charge", 42, func(context.Context) (int, error) { effects++; return 77, nil })
			if !errors.Is(err, tc.wantErr) || effects != tc.effects || appends != tc.appends || (err == nil && got != 77) {
				t.Fatal(got, err, effects, appends)
			}
		})
	}
	for _, tc := range []struct {
		name  string
		input int
	}{{"renamed", 42}, {"charge", 43}} {
		c := wf.NewContext(context.Background(), history, func(context.Context, wf.Kind, json.RawMessage) error { t.Fatal("unexpected append"); return nil })
		_, err := wf.Run(c, tc.name, tc.input, func(context.Context) (int, error) { t.Fatal("unexpected effect"); return 0, nil })
		if !errors.Is(err, wf.ErrNonDeterministic) {
			t.Fatal(err)
		}
	}
}

type vector struct{ Name, Sequence, Epoch, Index, Kind, PayloadBase64, WorkerID, WireBase64 string }

func TestPortableVectors(t *testing.T) {
	path := "testdata/vectors.json"
	if dir := os.Getenv("PROTOCOL_INTEROP_DIR"); dir != "" {
		path = filepath.Join(dir, "vectors.json")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var vectors []vector
	if err := json.Unmarshal(data, &vectors); err != nil {
		t.Fatal(err)
	}
	if len(vectors) != 8 {
		t.Fatal("expected all eight kinds", len(vectors))
	}
	for _, v := range vectors {
		t.Run(v.Name, func(t *testing.T) {
			parse := func(s string) uint64 {
				n, err := strconv.ParseUint(s, 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				return n
			}
			decode := func(s string) []byte {
				b, err := base64.StdEncoding.DecodeString(s)
				if err != nil {
					t.Fatal(err)
				}
				return b
			}
			want := journal.Record{Entry: journal.Entry{Epoch: parse(v.Epoch), Index: parse(v.Index), Kind: journal.Kind(v.Kind), Payload: decode(v.PayloadBase64), WorkerID: v.WorkerID}, Sequence: parse(v.Sequence)}
			got, err := protocol.UnmarshalRecord(decode(v.WireBase64))
			if err != nil {
				t.Fatal(err)
			}
			if got.Epoch != want.Epoch || got.Index != want.Index || got.Sequence != want.Sequence || got.Kind != want.Kind || got.WorkerID != want.WorkerID || !bytes.Equal(got.Payload, want.Payload) {
				t.Fatal(got, want)
			}
		})
	}
}

func TestSDKRecordedFailureAndSpilledResult(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(strconv.FormatBool(fail), func(t *testing.T) {
			var entries []wf.Entry
			var stored []byte
			value := string(bytes.Repeat([]byte("x"), wf.MaxInlineResult+1))
			producer := wf.NewContext(context.Background(), nil, func(_ context.Context, k wf.Kind, p json.RawMessage) error {
				entries = append(entries, wf.Entry{Index: uint64(len(entries)), Kind: k, Payload: bytes.Clone(p)})
				return nil
			})
			producer.SetResultStore(func(_ context.Context, b []byte) (string, error) { stored = bytes.Clone(b); return "blob", nil }, nil)
			_, err := wf.Run(producer, "effect", nil, func(context.Context) (string, error) {
				if fail {
					return "", errors.New("recorded failure")
				}
				return value, nil
			})
			if fail && (err == nil || err.Error() != "recorded failure") {
				t.Fatal(err)
			}
			if !fail && err != nil {
				t.Fatal(err)
			}
			var decoded []wf.Entry
			for _, e := range entries {
				wire, err := protocol.MarshalRecord(journal.Record{Entry: journal.Entry{Kind: journal.Kind(e.Kind), Index: e.Index, Payload: e.Payload}})
				if err != nil {
					t.Fatal(err)
				}
				r, err := protocol.UnmarshalRecord(wire)
				if err != nil {
					t.Fatal(err)
				}
				decoded = append(decoded, wf.Entry{Index: r.Index, Kind: wf.Kind(r.Kind), Payload: r.Payload})
			}
			replay := func(corrupt bool) (string, error) {
				c := wf.NewContext(context.Background(), decoded, func(context.Context, wf.Kind, json.RawMessage) error { t.Fatal("unexpected replay append"); return nil })
				c.SetResultStore(nil, func(_ context.Context, key string) ([]byte, error) {
					if key != "blob" {
						t.Fatal(key)
					}
					b := bytes.Clone(stored)
					if corrupt {
						b[1] = 'y'
					}
					return b, nil
				})
				return wf.Run(c, "effect", nil, func(context.Context) (string, error) { t.Fatal("unexpected replay effect"); return "", nil })
			}
			got, err := replay(false)
			if fail {
				if err == nil || err.Error() != "recorded failure" {
					t.Fatal(err)
				}
			} else {
				if err != nil || got != value {
					t.Fatal("spilled replay mismatch", err)
				}
				if _, err := replay(true); !errors.Is(err, wf.ErrCorruptJournal) {
					t.Fatal(err)
				}
			}
		})
	}
}
