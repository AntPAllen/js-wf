// Regenerate portable interoperability fixtures from the production codecs.
// Run from the repository root and redirect stdout to protocol/testdata/vectors.json.
package main

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"strconv"

	"js-wf/journal"
	"js-wf/protocol"
)

func main() {
	var vectors []map[string]string
	for i, kind := range []journal.Kind{journal.Started, journal.StepRequested, journal.StepCompleted, journal.Suspended, journal.SignalConsumed, journal.Attempt, journal.Completed, journal.Failed} {
		payload := []byte(" {\"integer\":18446744073709551615,\"text\":\"λ\"} \n")
		if i == 0 {
			payload = nil
		}
		if i == 1 {
			payload = []byte("null")
		}
		record := journal.Record{Entry: journal.Entry{Epoch: ^uint64(0), Index: 1<<53 + uint64(i), Kind: kind, Payload: payload, WorkerID: "worker-λ"}, Sequence: ^uint64(0) - uint64(i)}
		wire, err := protocol.MarshalRecord(record)
		if err != nil {
			panic(err)
		}
		stored, err := journal.MarshalEntry(record.Entry, journal.ProtobufV1)
		if err != nil {
			panic(err)
		}
		vectors = append(vectors, map[string]string{"Name": string(kind), "Kind": string(kind), "Epoch": strconv.FormatUint(record.Epoch, 10), "Index": strconv.FormatUint(record.Index, 10), "Sequence": strconv.FormatUint(record.Sequence, 10), "WorkerID": record.WorkerID, "PayloadBase64": base64.StdEncoding.EncodeToString(payload), "WireBase64": base64.StdEncoding.EncodeToString(wire), "StoredWireBase64": base64.StdEncoding.EncodeToString(stored)})
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(vectors); err != nil {
		panic(err)
	}
}
