// Package protocol converts versioned protobuf interchange records to the
// current persisted journal model. It does not publish protobuf bytes to NATS.
package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"
	"js-wf/journal"
	protocolv1 "js-wf/protocol/v1"
)

const Version = 1

var ErrInvalidRecord = errors.New("invalid journal interchange record")

var kinds = map[journal.Kind]protocolv1.EntryKind{
	journal.Started:        protocolv1.EntryKind_ENTRY_KIND_STARTED,
	journal.StepRequested:  protocolv1.EntryKind_ENTRY_KIND_STEP_REQUESTED,
	journal.StepCompleted:  protocolv1.EntryKind_ENTRY_KIND_STEP_COMPLETED,
	journal.Suspended:      protocolv1.EntryKind_ENTRY_KIND_SUSPENDED,
	journal.SignalConsumed: protocolv1.EntryKind_ENTRY_KIND_SIGNAL_CONSUMED,
	journal.Attempt:        protocolv1.EntryKind_ENTRY_KIND_ATTEMPT,
	journal.Completed:      protocolv1.EntryKind_ENTRY_KIND_COMPLETED,
	journal.Failed:         protocolv1.EntryKind_ENTRY_KIND_FAILED,
}

// ToMessage detaches payload bytes. Empty and absent payloads both map to the
// persisted JSON omitted field; explicit JSON null remains four payload bytes.
func ToMessage(record journal.Record) (*protocolv1.JournalRecord, error) {
	kind, ok := kinds[record.Kind]
	if !ok || !utf8.ValidString(record.WorkerID) || len(record.Payload) > 0 && (!utf8.Valid(record.Payload) || !json.Valid(record.Payload)) {
		return nil, fmt.Errorf("%w: kind, worker or payload", ErrInvalidRecord)
	}
	return &protocolv1.JournalRecord{Version: Version, Sequence: record.Sequence, Entry: &protocolv1.JournalEntry{Epoch: record.Epoch, Index: record.Index, Kind: kind, PayloadJson: bytes.Clone(record.Payload), WorkerId: record.WorkerID}}, nil
}

// FromMessage rejects unsupported fields rather than silently dropping them
// when projecting a future message into the current journal model. It validates
// the envelope, not journal history invariants or kind-specific payload schemas.
func FromMessage(message *protocolv1.JournalRecord) (journal.Record, error) {
	if message == nil || message.Version != Version || message.Entry == nil || len(message.ProtoReflect().GetUnknown()) != 0 || len(message.Entry.ProtoReflect().GetUnknown()) != 0 {
		return journal.Record{}, fmt.Errorf("%w: version, entry or unknown fields", ErrInvalidRecord)
	}
	entry := message.Entry
	var kind journal.Kind
	for candidate, value := range kinds {
		if entry.Kind == value {
			kind = candidate
			break
		}
	}
	record := journal.Record{Entry: journal.Entry{Epoch: entry.Epoch, Index: entry.Index, Kind: kind, Payload: bytes.Clone(entry.PayloadJson), WorkerID: entry.WorkerId}, Sequence: message.Sequence}
	if _, err := ToMessage(record); err != nil {
		return journal.Record{}, err
	}
	return record, nil
}

func MarshalRecord(record journal.Record) ([]byte, error) {
	message, err := ToMessage(record)
	if err != nil {
		return nil, err
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(message)
}

func UnmarshalRecord(data []byte) (journal.Record, error) {
	var message protocolv1.JournalRecord
	if err := proto.Unmarshal(data, &message); err != nil {
		return journal.Record{}, fmt.Errorf("%w: %v", ErrInvalidRecord, err)
	}
	return FromMessage(&message)
}
