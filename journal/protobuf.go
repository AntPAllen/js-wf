// Protobuf record projection shared by storage and interchange.
package journal

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"
	protocolv1 "js-wf/protocol/v1"
)

const ProtocolVersion = 1

var ErrInvalidRecord = errors.New("invalid journal interchange record")

var kinds = map[Kind]protocolv1.EntryKind{
	Started:        protocolv1.EntryKind_ENTRY_KIND_STARTED,
	StepRequested:  protocolv1.EntryKind_ENTRY_KIND_STEP_REQUESTED,
	StepCompleted:  protocolv1.EntryKind_ENTRY_KIND_STEP_COMPLETED,
	Suspended:      protocolv1.EntryKind_ENTRY_KIND_SUSPENDED,
	SignalConsumed: protocolv1.EntryKind_ENTRY_KIND_SIGNAL_CONSUMED,
	Attempt:        protocolv1.EntryKind_ENTRY_KIND_ATTEMPT,
	Completed:      protocolv1.EntryKind_ENTRY_KIND_COMPLETED,
	Failed:         protocolv1.EntryKind_ENTRY_KIND_FAILED,
}

// RecordToProto detaches payload bytes. Empty and absent payloads both map to the
// persisted JSON omitted field; explicit JSON null remains four payload bytes.
func RecordToProto(record Record) (*protocolv1.JournalRecord, error) {
	kind, ok := kinds[record.Kind]
	if !ok || !utf8.ValidString(record.WorkerID) || len(record.Payload) > 0 && (!utf8.Valid(record.Payload) || !json.Valid(record.Payload)) {
		return nil, fmt.Errorf("%w: kind, worker or payload", ErrInvalidRecord)
	}
	return &protocolv1.JournalRecord{Version: ProtocolVersion, Sequence: record.Sequence, Entry: &protocolv1.JournalEntry{Epoch: record.Epoch, Index: record.Index, Kind: kind, PayloadJson: bytes.Clone(record.Payload), WorkerId: record.WorkerID}}, nil
}

// RecordFromProto rejects unsupported fields rather than silently dropping them
// when projecting a future message into the current journal model. It validates
// the envelope, not journal history invariants or kind-specific payload schemas.
func RecordFromProto(message *protocolv1.JournalRecord) (Record, error) {
	if message == nil || message.Version != ProtocolVersion || message.Entry == nil || len(message.ProtoReflect().GetUnknown()) != 0 || len(message.Entry.ProtoReflect().GetUnknown()) != 0 {
		return Record{}, fmt.Errorf("%w: version, entry or unknown fields", ErrInvalidRecord)
	}
	entry := message.Entry
	var kind Kind
	for candidate, value := range kinds {
		if entry.Kind == value {
			kind = candidate
			break
		}
	}
	record := Record{Entry: Entry{Epoch: entry.Epoch, Index: entry.Index, Kind: kind, Payload: bytes.Clone(entry.PayloadJson), WorkerID: entry.WorkerId}, Sequence: message.Sequence}
	if _, err := RecordToProto(record); err != nil {
		return Record{}, err
	}
	return record, nil
}

func MarshalProtoRecord(record Record) ([]byte, error) {
	message, err := RecordToProto(record)
	if err != nil {
		return nil, err
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(message)
}

func UnmarshalProtoRecord(data []byte) (Record, error) {
	var message protocolv1.JournalRecord
	if err := proto.Unmarshal(data, &message); err != nil {
		return Record{}, fmt.Errorf("%w: %v", ErrInvalidRecord, err)
	}
	return RecordFromProto(&message)
}
