package journal_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/internal/checkpoint"
	"js-wf/internal/retainedgraph"
	"js-wf/journal"
	"js-wf/sim"
)

// This charges a deterministic budget to decoded entry reads, not wall time.
// It measures production journal work over a model, not native latency.
type checkpointCostPort struct {
	*sim.GraphPublicationTransport
	entries          map[string]bool
	entryReads, gets int
	maxEntryReads    int
}

func (p *checkpointCostPort) Get(ctx context.Context, link retainedgraph.Link, limit int) ([]byte, error) {
	p.gets++
	if p.entries[link.Hash] {
		p.entryReads++
		if p.maxEntryReads > 0 && p.entryReads > p.maxEntryReads {
			return nil, context.DeadlineExceeded
		}
	}
	return p.GraphPublicationTransport.Get(ctx, link, limit)
}

func TestGraphCheckpointPublicationPrefixCostAndReadBudget(t *testing.T) {
	for _, padding := range []int{16, 128, 512} {
		t.Run(fmt.Sprintf("padding=%d", padding), func(t *testing.T) {
			ctx := context.Background()
			schedule := sim.NewScheduler(23)
			model := sim.NewGraphPublicationTransport(schedule)
			port := &checkpointCostPort{GraphPublicationTransport: model, entries: map[string]bool{}}
			protocol := model.Protocol()
			protocol.Port = port
			store, err := journal.NewGraphStore(journal.GraphConfig{Protocol: protocol, Now: func() time.Time { return time.Unix(1000, 0) }, CanonicalStarts: true, CanonicalSignals: true, CheckpointIndex: true, ArchiveCheckpoints: true})
			if err != nil {
				t.Fatal(err)
			}
			transport := sim.NewSignalTransport(schedule)
			c, err := client.NewWithSignalPorts(transport, transport).WithGraphJournal(store)
			if err != nil {
				t.Fatal(err)
			}
			h, err := c.Start(ctx, "flow", "cost", []byte(`7`))
			if err != nil {
				t.Fatal(err)
			}
			status, err := store.InspectStart(ctx, h.Type, h.ID)
			if err != nil {
				t.Fatal(err)
			}
			tail, err := store.Begin(ctx, h.Type, h.ID, h.InvSeq)
			if err != nil {
				t.Fatal(err)
			}
			index := uint64(0)
			appendRecord := func(kind journal.Kind, payload []byte, objects ...[]byte) {
				t.Helper()
				entry := journal.Entry{Index: index, Epoch: 3, Kind: kind, Payload: payload}
				encoded, err := journal.MarshalEntry(entry, journal.JSON)
				if err != nil {
					t.Fatal(err)
				}
				hash := sha256.Sum256(encoded)
				port.entries[hex.EncodeToString(hash[:])] = true
				tail, err = store.Append(ctx, h.Type, h.ID, h.InvSeq, entry, tail, objects, nil)
				if err != nil {
					t.Fatal(err)
				}
				index++
			}
			started, _ := json.Marshal(map[string]string{"input_sha256": status.State.Start.InputSHA256})
			appendRecord(journal.Started, started, []byte(`7`))
			for i := 0; i < padding; i++ {
				request, _ := json.Marshal(map[string]string{"kind": "run", "name": fmt.Sprint("padding", i)})
				appendRecord(journal.StepRequested, request)
				appendRecord(journal.StepCompleted, []byte(`{"result":"Nw=="}`))
			}
			locals := json.RawMessage(`{"value":42}`)
			digest := sha256.Sum256(locals)
			frame := checkpoint.Frame{Version: checkpoint.Version, Identity: checkpoint.Identity{Type: h.Type, ID: h.ID, InvSeq: h.InvSeq}, Stage: "next", Data: locals, Anchor: checkpoint.Anchor{Index: index + 1, Epoch: 3}, StepPosition: uint64(2*padding + 2)}
			data, hash, err := checkpoint.Encode(frame)
			if err != nil {
				t.Fatal(err)
			}
			request, _ := json.Marshal(map[string]string{"kind": "checkpoint", "name": "next", "input_hash": hex.EncodeToString(digest[:])})
			completion, _ := json.Marshal(map[string]string{"result_ref": "step-result-" + hash, "result_hash": hash})
			appendRecord(journal.StepRequested, request)
			appendRecord(journal.StepCompleted, completion, data)
			runtime := journal.RuntimeCheckpoint{InvSeq: h.InvSeq, Stage: "next", Sequence: tail, Index: index - 1, Epoch: 3, StepPosition: frame.StepPosition, Object: "step-result-" + hash, SHA256: hash}
			appendRecord(journal.Suspended, []byte(`{"waiting_on":"continuation:next"}`))
			// Exhaustion must not publish a pointer, mutate history or leak a pin.
			port.entryReads, port.gets, port.maxEntryReads = 0, 0, 16
			if err := store.PublishCheckpoint(ctx, h.Type, h.ID, runtime, tail); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("injected read budget did not stop confirmation", err)
			}
			status, err = store.InspectStart(ctx, h.Type, h.ID)
			if err != nil || status.Checkpoint != nil || status.JournalCount != index || status.JournalTail != tail {
				t.Fatal("failed confirmation changed logical state", status, err)
			}
			keys, err := model.RootKeys(ctx)
			if err != nil || len(keys) != 1 {
				t.Fatal(keys, err)
			}
			root, err := model.ReadRoot(ctx, keys[0])
			if err != nil || len(root.Readers) != 0 {
				t.Fatal("failed confirmation leaked its reader", root, err)
			}
			// A retry with enough work budget must confirm the same owned frame.
			port.entryReads, port.gets, port.maxEntryReads = 0, 0, 0
			if err := store.PublishCheckpoint(ctx, h.Type, h.ID, runtime, tail); err != nil {
				t.Fatal(err)
			}
			firstReads, firstGets := port.entryReads, port.gets
			port.entryReads, port.gets = 0, 0
			if err := store.PublishCheckpoint(ctx, h.Type, h.ID, runtime, tail); err != nil {
				t.Fatal(err)
			}
			indexedReads, indexedGets := port.entryReads, port.gets
			if indexedReads > 4 {
				t.Fatal("indexed retry scanned its prefix", indexedReads)
			}
			port.entryReads, port.gets = 0, 0
			if err := store.CompactCheckpoint(ctx, h.Type, h.ID, runtime, tail); err != nil {
				t.Fatal(err)
			}
			t.Logf("CHECKPOINT_PREFIX_COST padding=%d entries=%d first_entry_reads=%d first_gets=%d indexed_entry_reads=%d indexed_gets=%d compaction_entry_reads=%d compaction_gets=%d injected_budget_entry_reads=16", padding, index, firstReads, firstGets, indexedReads, indexedGets, port.entryReads, port.gets)
		})
	}
}
