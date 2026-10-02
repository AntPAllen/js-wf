package journal_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/journal"
)

type encodingPort struct{ messages []journal.AppendTail }

func (p *encodingPort) Last(context.Context, string) (journal.AppendTail, error) {
	if len(p.messages) == 0 {
		return journal.AppendTail{}, jetstream.ErrMsgNotFound
	}
	return p.messages[len(p.messages)-1], nil
}
func (p *encodingPort) Publish(_ context.Context, _ string, data []byte, expected uint64) (uint64, error) {
	if expected != uint64(len(p.messages)) {
		return 0, journal.ErrStale
	}
	sequence := expected + 1
	p.messages = append(p.messages, journal.AppendTail{Sequence: sequence, Data: bytes.Clone(data)})
	return sequence, nil
}
func (p *encodingPort) Next(_ context.Context, _ string, start uint64) (journal.AppendTail, error) {
	for _, m := range p.messages {
		if m.Sequence >= start {
			return m, nil
		}
	}
	return journal.AppendTail{}, jetstream.ErrMsgNotFound
}
func (p *encodingPort) Wait(context.Context, time.Duration) error { return nil }

func TestStoredEntryEncoding(t *testing.T) {
	entry := journal.Entry{Epoch: ^uint64(0), Index: 1<<53 + 1, Kind: journal.StepRequested, Payload: []byte(" {\"large\":18446744073709551615} \n"), WorkerID: "λ"}
	wire, err := journal.MarshalEntry(entry, journal.ProtobufV1)
	if err != nil {
		t.Fatal(err)
	}
	var got journal.Entry
	if err := journal.UnmarshalEntry(wire, &got); err != nil || !reflect.DeepEqual(entry, got) {
		t.Fatal(got, err)
	}
	for _, encoding := range []journal.Encoding{"", journal.JSON} {
		old, err := json.Marshal(entry)
		if err != nil {
			t.Fatal(err)
		}
		data, err := journal.MarshalEntry(entry, encoding)
		if err != nil || !bytes.Equal(old, data) {
			t.Fatal("legacy bytes changed", err)
		}
	}
	unknown, _ := journal.MarshalProtoRecord(journal.Record{Entry: entry, Sequence: 9})
	for _, data := range [][]byte{nil, {'W', 'F', 'J', 0}, {'W', 'F', 'J', 0, 255}, append([]byte{'W', 'F', 'J', 0}, unknown...)} {
		destination := journal.Entry{Kind: journal.Failed}
		if err := journal.UnmarshalEntry(data, &destination); err == nil || destination.Kind != journal.Failed {
			t.Fatal("bad entry accepted or destination mutated", err)
		}
	}
	if _, err := journal.NewWithEncoding(nil, "bad"); err == nil {
		t.Fatal("unknown encoding")
	}
	if _, err := journal.MarshalEntry(entry, "bad"); err == nil {
		t.Fatal("unknown encoding")
	}
}

func TestMixedEncodingAppendReadAndFencing(t *testing.T) {
	port := &encodingPort{}
	jsonStore, err := journal.NewWithEncodedPorts(port, port, journal.JSON)
	if err != nil {
		t.Fatal(err)
	}
	protoStore, err := journal.NewWithEncodedPorts(port, port, journal.ProtobufV1)
	if err != nil {
		t.Fatal(err)
	}
	expected := uint64(0)
	for i, kind := range []journal.Kind{journal.Started, journal.StepRequested, journal.StepCompleted, journal.Completed} {
		store := jsonStore
		if i%2 == 1 {
			store = protoStore
		}
		expected, err = store.Append(context.Background(), "test", "id", journal.Entry{Epoch: uint64(i + 1), Index: uint64(i), Kind: kind, Payload: []byte(`null`)}, expected)
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, store := range []*journal.Store{jsonStore, protoStore} {
		records, tail, err := store.Read(context.Background(), "test", "id")
		if err != nil || len(records) != 4 || tail != 4 {
			t.Fatal(records, tail, err)
		}
	}
	if _, err := protoStore.Append(context.Background(), "test", "id", journal.Entry{Epoch: 1, Index: 4, Kind: journal.StepCompleted}, 4); !errors.Is(err, journal.ErrStale) {
		t.Fatal("terminal fencing", err)
	}
	port.messages[3].Data = []byte{'W', 'F', 'J', 0, 255}
	if _, _, err := jsonStore.Read(context.Background(), "test", "id"); !errors.Is(err, journal.ErrGap) {
		t.Fatal("corrupt protobuf read", err)
	}
	if _, err := protoStore.Append(context.Background(), "test", "id", journal.Entry{Epoch: 5, Index: 4, Kind: journal.StepCompleted}, 4); !errors.Is(err, journal.ErrGap) {
		t.Fatal("corrupt protobuf tail", err)
	}
}
