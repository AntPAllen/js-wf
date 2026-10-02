package journal

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

// Encoding selects new entry writes. Every updated reader accepts either format.
type Encoding string

const (
	JSON       Encoding = "json"
	ProtobufV1 Encoding = "protobuf-v1"
)

// A non-JSON prefix distinguishes storage from the standalone interchange
// envelope. Version lives in the envelope; the physical sequence is supplied by
// JetStream and must be zero in the stored bytes.
var protobufPrefix = []byte{'W', 'F', 'J', 0}

func validateEncoding(encoding Encoding) error {
	if encoding != "" && encoding != JSON && encoding != ProtobufV1 {
		return fmt.Errorf("unsupported journal encoding %q", encoding)
	}
	return nil
}

func NewWithEncoding(js jetstream.JetStream, encoding Encoding) (*Store, error) {
	if err := validateEncoding(encoding); err != nil {
		return nil, err
	}
	store := New(js)
	store.encoding = encoding
	return store, nil
}

// NewWithEncodedPorts exercises the same writer and decoder through modeled I/O.
func NewWithEncodedPorts(appendPort AppendPort, readPort ReadPort, encoding Encoding) (*Store, error) {
	if err := validateEncoding(encoding); err != nil {
		return nil, err
	}
	store := NewWithPorts(appendPort, readPort)
	store.encoding = encoding
	return store, nil
}

func MarshalEntry(entry Entry, encoding Encoding) ([]byte, error) {
	if err := validateEncoding(encoding); err != nil {
		return nil, err
	}
	if encoding != ProtobufV1 {
		return json.Marshal(entry)
	}
	data, err := MarshalProtoRecord(Record{Entry: entry})
	if err != nil {
		return nil, err
	}
	return append(bytes.Clone(protobufPrefix), data...), nil
}

// UnmarshalEntry auto-detects versioned protobuf storage and legacy JSON. It
// never falls back to JSON after recognizing a protobuf prefix. Decoding failure
// leaves the caller's destination unchanged.
func UnmarshalEntry(data []byte, entry *Entry) error {
	if entry == nil {
		return fmt.Errorf("%w: nil entry destination", ErrInvalidRecord)
	}
	var decoded Entry
	if bytes.HasPrefix(data, protobufPrefix) {
		record, err := UnmarshalProtoRecord(data[len(protobufPrefix):])
		if err != nil {
			return err
		}
		if record.Sequence != 0 {
			return fmt.Errorf("%w: stored physical sequence", ErrInvalidRecord)
		}
		decoded = record.Entry
	} else if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*entry = decoded
	return nil
}
