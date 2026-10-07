package integrity

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/journal"
)

func decodeMessages(t *testing.T, count int) []*jetstream.RawStreamMsg {
	t.Helper()
	rng := rand.New(rand.NewSource(8520))
	messages := make([]*jetstream.RawStreamMsg, count)
	for i := range messages {
		encoding := journal.JSON
		if rng.Intn(2) == 0 {
			encoding = journal.ProtobufV1
		}
		e := journal.Entry{Epoch: 1, Index: uint64(i + 1), Kind: journal.StepCompleted, WorkerID: "worker", Payload: json.RawMessage(`{"value":` + strconv.Itoa(rng.Intn(10000)) + `}`)}
		data, err := journal.MarshalEntry(e, encoding)
		if err != nil {
			t.Fatal(err)
		}
		messages[i] = &jetstream.RawStreamMsg{Sequence: uint64(i + 1), Subject: "wf.jrn.a.b", Data: data}
	}
	return messages
}

func messageScanner(messages []*jetstream.RawStreamMsg, terminal error) func(context.Context, func(*jetstream.RawStreamMsg) error) error {
	return func(ctx context.Context, visit func(*jetstream.RawStreamMsg) error) error {
		for _, m := range messages {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := visit(m); err != nil {
				return err
			}
		}
		return terminal
	}
}

func acceptJournal(*jetstream.RawStreamMsg) bool { return true }

func realEntryDecode(_ context.Context, data []byte, entry *journal.Entry) error {
	return journal.UnmarshalEntry(data, entry)
}

func TestParallelJournalDecodePreservesMixedEncodingOrder(t *testing.T) {
	messages := decodeMessages(t, 513)
	var expected []journal.Record
	for _, m := range messages {
		var e journal.Entry
		if err := journal.UnmarshalEntry(m.Data, &e); err != nil {
			t.Fatal(err)
		}
		expected = append(expected, journal.Record{Sequence: m.Sequence, Entry: e})
	}
	secondComplete := make(chan struct{})
	decode := func(ctx context.Context, data []byte, e *journal.Entry) error {
		if err := journal.UnmarshalEntry(data, e); err != nil {
			return err
		}
		if e.Index == 1 {
			select {
			case <-secondComplete:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if e.Index == 128 {
			close(secondComplete)
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var actual []journal.Record
	err := decodeJournalEntries(ctx, messageScanner(messages, nil), acceptJournal, func(m *jetstream.RawStreamMsg, e journal.Entry) error {
		actual = append(actual, journal.Record{Sequence: m.Sequence, Entry: e})
		return nil
	}, decode, 2)
	if err != nil || !reflect.DeepEqual(actual, expected) {
		t.Fatalf("ordered mixed JSON/protobuf mismatch: %v, records=%d", err, len(actual))
	}
}

func TestParallelJournalDecodeFiltersBeforeDecodeAndPreservesFirstError(t *testing.T) {
	for _, malformed := range [][]byte{[]byte("{"), {'W', 'F', 'J', 0, 255}} {
		messages := decodeMessages(t, 97)
		messages[1].Subject = "wf.jrn.later.cohort"
		messages[1].Data = malformed // Must be excluded without attempting decode.
		messages[3].Data = malformed
		messages[69].Data = []byte("later-invalid")
		var destination journal.Entry
		want := journal.UnmarshalEntry(malformed, &destination)
		var visited []uint64
		err := decodeJournalEntries(context.Background(), messageScanner(messages, errors.New("later transport failure")), func(m *jetstream.RawStreamMsg) bool {
			return m.Subject == "wf.jrn.a.b"
		}, func(m *jetstream.RawStreamMsg, _ journal.Entry) error {
			visited = append(visited, m.Sequence)
			return nil
		}, realEntryDecode, 2)
		if err == nil || err.Error() != want.Error() || !reflect.DeepEqual(visited, []uint64{1, 3}) {
			t.Fatalf("first source error lost: %v, want %v, visited %v", err, want, visited)
		}
	}
}

func TestParallelJournalDecodeVisitorErrorCancelsAndJoinsWithoutRevisit(t *testing.T) {
	var active atomic.Int32
	decode := func(ctx context.Context, data []byte, e *journal.Entry) error {
		active.Add(1)
		defer active.Add(-1)
		if err := journal.UnmarshalEntry(data, e); err != nil {
			return err
		}
		if e.Index >= 65 {
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}
	want := errors.New("first invariant failure")
	var visited []uint64
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := decodeJournalEntries(ctx, messageScanner(decodeMessages(t, 380), nil), acceptJournal, func(m *jetstream.RawStreamMsg, _ journal.Entry) error {
		visited = append(visited, m.Sequence)
		if m.Sequence == 3 {
			return want
		}
		return nil
	}, decode, 2)
	if !errors.Is(err, want) || active.Load() != 0 || !reflect.DeepEqual(visited, []uint64{1, 2, 3}) {
		t.Fatalf("err=%v active=%d visited=%v", err, active.Load(), visited)
	}
}

func TestParallelJournalDecodeBoundsBackpressureAndJoinsOnCancellation(t *testing.T) {
	for _, test := range []struct {
		name            string
		bytes, accepted int
	}{
		{"record-limit", 1, 3 * journalDecodeBatchRecords},
		{"byte-limit", 600 << 10, 4},
		{"one-oversized-record", 2 << 20, 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			data := []byte(strings.Repeat("x", test.bytes))
			var active, accepted atomic.Int32
			blocked := make(chan struct{})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			scan := func(ctx context.Context, visit func(*jetstream.RawStreamMsg) error) error {
				for i := 1; i <= 1000; i++ {
					accepted.Add(1)
					if i == test.accepted {
						close(blocked)
					}
					if err := visit(&jetstream.RawStreamMsg{Sequence: uint64(i), Data: data}); err != nil {
						return err
					}
				}
				return nil
			}
			finished := make(chan error, 1)
			go func() {
				finished <- decodeJournalEntries(ctx, scan, acceptJournal, func(*jetstream.RawStreamMsg, journal.Entry) error {
					return errors.New("unexpected visitor after cancellation")
				}, func(ctx context.Context, _ []byte, _ *journal.Entry) error {
					active.Add(1)
					defer active.Add(-1)
					<-ctx.Done()
					return ctx.Err()
				}, 2)
			}()
			select {
			case <-blocked:
				cancel()
			case <-ctx.Done():
				t.Fatal("bounded pipeline did not fill")
			}
			select {
			case err := <-finished:
				if !errors.Is(err, context.Canceled) || active.Load() != 0 || int(accepted.Load()) != test.accepted {
					t.Fatalf("err=%v active=%d accepted=%d want=%d", err, active.Load(), accepted.Load(), test.accepted)
				}
			case <-time.After(time.Second):
				t.Fatal("cancellation did not join all workers")
			}
		})
	}
}

func TestParallelJournalDecodePartialBatchPrecedesScannerError(t *testing.T) {
	want := errors.New("transport failure after captured prefix")
	visited := 0
	err := decodeJournalEntries(context.Background(), messageScanner(decodeMessages(t, 3), want), acceptJournal, func(*jetstream.RawStreamMsg, journal.Entry) error {
		visited++
		return nil
	}, realEntryDecode, 1)
	if !errors.Is(err, want) || visited != 3 {
		t.Fatalf("err=%v visited=%d", err, visited)
	}
}
