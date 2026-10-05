//go:build linux

package integration_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/journal"
)

const matrixProtobufToJSON = "protobuf-to-json"

func matrixRolloutEncoding(generation int) journal.Encoding {
	if os.Getenv("WF_MATRIX_JOURNAL_ROLLOUT") == matrixProtobufToJSON && generation == 0 {
		return journal.ProtobufV1
	}
	return journal.JSON
}

func matrixRolloutWire(data []byte) (journal.Encoding, error) {
	var entry journal.Entry
	if err := journal.UnmarshalEntry(data, &entry); err != nil {
		return "", err
	}
	actual := journal.JSON
	if bytes.HasPrefix(data, []byte{'W', 'F', 'J', 0}) {
		actual = journal.ProtobufV1
	}
	expected := journal.JSON
	if entry.WorkerID != "" {
		prefix, generation, ok := strings.Cut(entry.WorkerID, "-generation-")
		if !ok || !strings.HasPrefix(prefix, "matrix-process-") {
			return "", fmt.Errorf("unknown rollout writer %q", entry.WorkerID)
		}
		value, err := strconv.Atoi(generation)
		if err != nil || value < 0 {
			return "", fmt.Errorf("invalid rollout writer %q", entry.WorkerID)
		}
		if value == 0 {
			expected = journal.ProtobufV1
		}
	} else if entry.Kind != journal.Started {
		return "", fmt.Errorf("unattributed rollout entry %s", entry.Kind)
	}
	if actual != expected {
		return "", fmt.Errorf("writer %s stored %s, expected %s", entry.WorkerID, actual, expected)
	}
	return actual, nil
}

type matrixRolloutProof struct {
	Type     string           `json:"type"`
	ID       string           `json:"id"`
	Protobuf int              `json:"protobuf_worker_entries"`
	JSON     int              `json:"json_worker_entries"`
	Mixed    bool             `json:"mixed_worker_entries"`
	Records  []map[string]any `json:"records"`
}

func auditMatrixRolloutInvocation(ctx context.Context, js jetstream.JetStream, root, typ, id string) (bool, error) {
	records, _, err := journal.New(js).Read(ctx, typ, id)
	if err != nil {
		return false, err
	}
	stream, err := js.Stream(ctx, "WF_JRN")
	if err != nil {
		return false, err
	}
	proof := matrixRolloutProof{Type: typ, ID: id}
	for _, record := range records {
		raw, err := stream.GetMsg(ctx, record.Sequence)
		if err != nil {
			return false, err
		}
		encoding, err := matrixRolloutWire(raw.Data)
		if err != nil {
			return false, err
		}
		proof.Records = append(proof.Records, map[string]any{"sequence": raw.Sequence, "wire_base64": base64.StdEncoding.EncodeToString(raw.Data)})
		if record.WorkerID == "" {
			continue
		}
		if encoding == journal.ProtobufV1 {
			proof.Protobuf++
		} else {
			proof.JSON++
		}
	}
	proof.Mixed = proof.Protobuf > 0 && proof.JSON > 0
	data, err := json.MarshalIndent(proof, "", "  ")
	if err != nil {
		return false, err
	}
	err = os.WriteFile(filepath.Join(root, "rollout-"+typ+"-"+id+".json"), data, 0600)
	return proof.Mixed, err
}

func TestMatrixRolloutRejectsIncorrectWriterFormat(t *testing.T) {
	for _, test := range []struct {
		worker   string
		encoding journal.Encoding
		valid    bool
	}{
		{"matrix-process-0-generation-0", journal.ProtobufV1, true},
		{"matrix-process-0-generation-1", journal.JSON, true},
		{"matrix-process-0-generation-0", journal.JSON, false},
		{"matrix-process-0-generation-1", journal.ProtobufV1, false},
		{"unknown", journal.JSON, false},
	} {
		wire, err := journal.MarshalEntry(journal.Entry{Epoch: 1, Index: 1, Kind: journal.StepCompleted, WorkerID: test.worker, Payload: json.RawMessage(`{"result":42}`)}, test.encoding)
		if err != nil {
			t.Fatal(err)
		}
		_, err = matrixRolloutWire(wire)
		if (err == nil) != test.valid {
			t.Fatalf("worker=%s encoding=%s valid=%v err=%v", test.worker, test.encoding, test.valid, err)
		}
	}
}
