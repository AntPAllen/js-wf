package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/provision"
	"js-wf/retention"
	"js-wf/wf"
	"js-wf/worker"
)

func TestProtobufWorkerResumesJSONPrefixAndCompacts(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c := client.New(all[0])
	if _, err := c.Start(ctx, "encoding", "mixed", []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	leases, err := lease.New(ctx, all[0])
	if err != nil {
		t.Fatal(err)
	}
	held, err := leases.Acquire(ctx, "encoding", "mixed", "legacy")
	if err != nil {
		t.Fatal(err)
	}
	legacy := journal.New(all[0])
	if _, err := legacy.Append(ctx, "encoding", "mixed", journal.Entry{Epoch: held.Epoch(), Index: 0, Kind: journal.Started, WorkerID: "legacy"}, 0); err != nil {
		t.Fatal(err)
	}
	if err := held.Release(ctx); err != nil {
		t.Fatal(err)
	}
	var effects atomic.Int64
	w, err := worker.New(ctx, all[1], "protobuf-worker", map[string]worker.Handler{"encoding": func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		result, err := wf.Run(c, "large", nil, func(context.Context) (string, error) {
			effects.Add(1)
			return strings.Repeat("x", wf.MaxInlineResult+1), nil
		})
		if err != nil {
			return nil, err
		}
		if len(result) != wf.MaxInlineResult+1 {
			return nil, fmt.Errorf("result length %d", len(result))
		}
		return json.RawMessage(`true`), nil
	}}, worker.WithJournalEncoding(journal.ProtobufV1))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() {
		done <- w.RunPartition(workerCtx, identity.Partition("encoding", "mixed", provision.Partitions))
	}()
	result, err := c.Await(ctx, "encoding", "mixed")
	if err != nil || string(result) != "true" || effects.Load() != 1 {
		t.Fatal(string(result), err, effects.Load())
	}
	stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not stop")
	}
	stream, err := all[2].Stream(ctx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := stream.GetLastMsgForSubject(ctx, identity.JournalSubject("encoding", "mixed"))
	if err != nil || !bytes.HasPrefix(raw.Data, []byte{'W', 'F', 'J', 0}) {
		t.Fatal("not persisted protobuf", err)
	}
	var old journal.Entry
	if json.Unmarshal(raw.Data, &old) == nil {
		t.Fatal("legacy decoder unexpectedly understands protobuf")
	}
	for _, js := range all {
		records, _, err := journal.New(js).Read(ctx, "encoding", "mixed")
		if err != nil || len(records) != 4 || records[0].WorkerID != "legacy" || records[3].Kind != journal.Completed {
			t.Fatal(records, err)
		}
		report, err := integrity.Check(ctx, js)
		if err != nil || report.Invocations != 1 || report.Terminal != 1 || report.Entries != 4 {
			t.Fatal(report, err)
		}
	}
	sweep, err := retention.SweepBlobsQuiescent(ctx, all[2], 0, time.Now().Add(time.Minute))
	if err != nil || sweep.Deleted != 0 {
		t.Fatal("live protobuf blob reference lost", sweep, err)
	}
	if _, err := journal.New(all[1]).SnapshotPrefix(ctx, "encoding", "mixed", 1); err != nil {
		t.Fatal(err)
	}
	records, _, err := journal.New(all[2]).Read(ctx, "encoding", "mixed")
	if err != nil || len(records) != 4 {
		t.Fatal("mixed snapshot replay", records, err)
	}
	report, err := integrity.Check(ctx, all[0])
	if err != nil || report.Entries != 4 || report.Terminal != 1 {
		t.Fatal("snapshot audit", report, err)
	}
	sweep, err = retention.SweepBlobsQuiescent(ctx, all[0], 0, time.Now().Add(time.Minute))
	if err != nil || sweep.Deleted != 0 {
		t.Fatal("snapshot blob reference lost", sweep, err)
	}
	t.Logf("R3 mixed JSON/protobuf: four entries, one effect/terminal, all-peer audit, live/snapshot blob retention and compaction pass")
}

// Disabling protobuf writes after reader upgrade must preserve already stored
// requests and replay outcomes. This is a writer rollout reversal, not a binary
// downgrade to the old JSON-only reader.
func TestJSONWorkerResumesProtobufStep(t *testing.T) {
	for _, completed := range []bool{false, true} {
		t.Run(fmt.Sprintf("completed=%v", completed), func(t *testing.T) {
			all, _ := setup(t)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			c := client.New(all[0])
			if _, err := c.Start(ctx, "encoding", "reverse", []byte(`null`)); err != nil {
				t.Fatal(err)
			}
			leases, err := lease.New(ctx, all[0])
			if err != nil {
				t.Fatal(err)
			}
			held, err := leases.Acquire(ctx, "encoding", "reverse", "protobuf-owner")
			if err != nil {
				t.Fatal(err)
			}
			store, err := journal.NewWithEncoding(all[0], journal.ProtobufV1)
			if err != nil {
				t.Fatal(err)
			}
			tail, err := store.Append(ctx, "encoding", "reverse", journal.Entry{Epoch: held.Epoch(), Index: 0, Kind: journal.Started, WorkerID: "protobuf-owner"}, 0)
			if err != nil {
				t.Fatal(err)
			}
			index := uint64(1)
			var effects atomic.Int64
			interrupted := fmt.Errorf("fixture interrupts before completion publication")
			sdk := wf.NewContext(ctx, nil, func(_ context.Context, kind wf.Kind, payload json.RawMessage) error {
				if kind == wf.StepCompleted && !completed {
					return interrupted
				}
				var err error
				tail, err = store.Append(ctx, "encoding", "reverse", journal.Entry{Epoch: held.Epoch(), Index: index, Kind: journal.Kind(kind), Payload: payload, WorkerID: "protobuf-owner"}, tail)
				if err == nil {
					index++
				}
				return err
			})
			_, err = wf.Run(sdk, "effect", nil, func(context.Context) (int, error) { effects.Add(1); return 77, nil })
			if completed && err != nil || !completed && err != interrupted {
				t.Fatal(err)
			}
			prefix, _, err := store.Read(ctx, "encoding", "reverse")
			if err != nil {
				t.Fatal(err)
			}
			if err := held.Release(ctx); err != nil {
				t.Fatal(err)
			}
			w, err := worker.New(ctx, all[1], "json-owner", map[string]worker.Handler{"encoding": func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
				value, err := wf.Run(c, "effect", nil, func(context.Context) (int, error) { effects.Add(1); return 77, nil })
				if err != nil {
					return nil, err
				}
				return json.Marshal(value)
			}}, worker.WithJournalEncoding(journal.JSON))
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			workerCtx, stop := context.WithCancel(ctx)
			defer stop()
			done := make(chan error, 1)
			go func() {
				done <- w.RunPartition(workerCtx, identity.Partition("encoding", "reverse", provision.Partitions))
			}()
			result, err := c.Await(ctx, "encoding", "reverse")
			expectedEffects := int64(2)
			if completed {
				expectedEffects = 1
			}
			if err != nil || string(result) != "77" || effects.Load() != expectedEffects {
				t.Fatal(string(result), err, effects.Load())
			}
			stop()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("worker did not stop")
			}
			stream, err := all[2].Stream(ctx, "WF_JRN")
			if err != nil {
				t.Fatal(err)
			}
			raw, err := stream.GetLastMsgForSubject(ctx, identity.JournalSubject("encoding", "reverse"))
			if err != nil || !json.Valid(raw.Data) {
				t.Fatal("replacement did not persist JSON", err)
			}
			for _, js := range all {
				records, _, err := journal.New(js).Read(ctx, "encoding", "reverse")
				if err != nil || len(records) != 4 {
					t.Fatal(records, err)
				}
				for i, old := range prefix {
					if records[i].Sequence != old.Sequence || records[i].Epoch != old.Epoch || records[i].Kind != old.Kind || !bytes.Equal(records[i].Payload, old.Payload) {
						t.Fatal("retained protobuf prefix changed")
					}
				}
				if records[3].Epoch <= held.Epoch() || records[3].WorkerID != "json-owner" {
					t.Fatal("replacement fencing epoch", records[3])
				}
				report, err := integrity.Check(ctx, js)
				if err != nil || report.Entries != 4 || report.Terminal != 1 {
					t.Fatal(report, err)
				}
			}
			t.Logf("protobuf-to-JSON recovery completed=%v effects=%d: retained prefix, higher epoch and all-peer audit pass", completed, effects.Load())
		})
	}
}
