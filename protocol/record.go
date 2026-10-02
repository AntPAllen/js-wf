// Package protocol provides the versioned journal interchange API.
package protocol

import (
	"js-wf/journal"
	protocolv1 "js-wf/protocol/v1"
)

const Version = journal.ProtocolVersion

var ErrInvalidRecord = journal.ErrInvalidRecord

func ToMessage(record journal.Record) (*protocolv1.JournalRecord, error) {
	return journal.RecordToProto(record)
}
func FromMessage(message *protocolv1.JournalRecord) (journal.Record, error) {
	return journal.RecordFromProto(message)
}
func MarshalRecord(record journal.Record) ([]byte, error) { return journal.MarshalProtoRecord(record) }
func UnmarshalRecord(data []byte) (journal.Record, error) { return journal.UnmarshalProtoRecord(data) }
