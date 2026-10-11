package worker_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/sim"
	"js-wf/wf"
	"js-wf/worker"
)

func TestGraphReplaySnapshotTerminalMetadataAdmission(t *testing.T) {
	for _, encoding := range []journal.Encoding{journal.JSON, journal.ProtobufV1} {
		for name, raw := range terminalAdmissionCases() {
			t.Run(string(encoding)+"/"+name, func(t *testing.T) {
				ctx := context.Background()
				schedule := sim.NewScheduler(1)
				model := sim.NewGraphPublicationTransport(schedule)
				store, err := journal.NewGraphStore(journal.GraphConfig{Protocol: model.Protocol(), Encoding: encoding, CanonicalStarts: true, CanonicalSignals: true, Now: func() time.Time { return time.Unix(1000, 0) }})
				if err != nil {
					t.Fatal(err)
				}
				transport := sim.NewWorkerTransport(schedule, 3*time.Second)
				c, err := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport).WithGraphJournal(store)
				if err != nil {
					t.Fatal(err)
				}
				input := []byte(`7`)
				h, err := c.Start(ctx, "test", "terminal_export", input)
				if err != nil {
					t.Fatal(err)
				}
				if h.InvSeq != 1 {
					t.Fatal("fixture generation", h.InvSeq)
				}
				hash := sha256.Sum256(input)
				start, _ := json.Marshal(map[string]string{"input_sha256": hex.EncodeToString(hash[:])})
				tail, err := store.Begin(ctx, h.Type, h.ID, h.InvSeq)
				if err != nil {
					t.Fatal(err)
				}
				tail, err = store.Append(ctx, h.Type, h.ID, h.InvSeq, journal.Entry{Kind: journal.Started, Payload: start}, tail, [][]byte{input}, nil)
				if err != nil {
					t.Fatal(err)
				}
				_, err = store.Append(ctx, h.Type, h.ID, h.InvSeq, journal.Entry{Index: 1, Kind: journal.Completed, Payload: []byte(raw)}, tail, nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				records, _, err := store.Read(ctx, h.Type, h.ID, h.InvSeq)
				if err != nil || len(records) != 2 || !bytes.Equal(records[1].Payload, []byte(raw)) {
					t.Fatal("fixture changed", err)
				}
				if err = model.CheckReferences(); err != nil {
					t.Fatal(err)
				}
				source, err := transport.LastInvocation(ctx, identity.InvocationSubject(h.Type, h.ID))
				if err != nil {
					t.Fatal(err)
				}
				snapshot, err := worker.ReadGraphReplaySnapshot(ctx, store, h.Type, h.ID, source)
				if validTerminalAdmissionCase(name) {
					if err != nil || snapshot.Format != wf.ReplayFormatGraphV1 || !bytes.Equal(snapshot.Input, input) || len(snapshot.Records) != 2 || !bytes.Equal(snapshot.Records[1].Payload, []byte(raw)) {
						t.Fatal("valid terminal export failed", snapshot, err)
					}
				} else if !errors.Is(err, wf.ErrCorruptJournal) || !reflect.DeepEqual(snapshot, worker.GraphReplaySnapshot{}) {
					t.Fatal("malformed terminal exported or partial snapshot leaked", snapshot, err)
				}
				if err = model.CheckReferences(); err != nil {
					t.Fatal(err)
				}
				after, _, err := store.Read(ctx, h.Type, h.ID, h.InvSeq)
				if err != nil || !reflect.DeepEqual(after, records) {
					t.Fatal("export changed journal", err)
				}
			})
		}
	}
}
