package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/internal/checkpoint"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/provision"
	"js-wf/testcluster"
	"js-wf/wf"
)

// Admission remains closed; exercise the production handoff boundary directly
// while compaction and full stage execution are still being migrated.
func TestNativeGraphContinuationHandoff(t *testing.T) {
	cluster, err := testcluster.Start(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := provision.Ensure(ctx, js, 1); err != nil {
		t.Fatal(err)
	}
	cfg := journal.NativeGraphConfig{AuthorityStream: "CONTINUE_AUTH", AuthorityPrefix: "wf.graph.continue", ObjectBucket: "CONTINUE_OBJECTS", ExpectedReplicas: 1, CanonicalStarts: true, CanonicalSignals: true}
	configs, err := journal.NativeGraphStreamConfigs(cfg, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, config := range configs {
		if _, err := js.CreateStream(ctx, config); err != nil {
			t.Fatal(err)
		}
	}
	graph, err := journal.OpenNativeGraphStore(ctx, js, cfg)
	if err != nil {
		t.Fatal(err)
	}
	c, err := client.New(js).WithGraphJournal(graph)
	if err != nil {
		t.Fatal(err)
	}
	h, err := c.Start(ctx, "test", "continue", []byte(`7`))
	if err != nil {
		t.Fatal(err)
	}
	leases, err := lease.New(ctx, js)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := leases.Acquire(ctx, h.Type, h.ID, "handoff")
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Release(context.Background())
	status, err := graph.InspectStart(ctx, h.Type, h.ID)
	if err != nil {
		t.Fatal(err)
	}
	locals := json.RawMessage(`{"count":1}`)
	digest := sha256.Sum256(locals)
	frame := checkpoint.Frame{Version: checkpoint.Version, Identity: checkpoint.Identity{Type: h.Type, ID: h.ID, InvSeq: h.InvSeq}, Stage: "next", Data: locals, Anchor: checkpoint.Anchor{Index: 2, Epoch: owner.Epoch()}, StepPosition: 2}
	encoded, hash, err := checkpoint.Encode(frame)
	if err != nil {
		t.Fatal(err)
	}
	started, _ := json.Marshal(map[string]string{"input_sha256": status.State.Start.InputSHA256})
	request, _ := json.Marshal(map[string]string{"kind": "checkpoint", "name": "next", "input_hash": hex.EncodeToString(digest[:])})
	completion, _ := json.Marshal(map[string]string{"result_ref": "step-result-" + hash, "result_hash": hash})
	tail, err := graph.Begin(ctx, h.Type, h.ID, h.InvSeq)
	if err != nil {
		t.Fatal(err)
	}
	for i, entry := range []journal.Entry{{Kind: journal.Started, Payload: started}, {Kind: journal.StepRequested, Payload: request}, {Kind: journal.StepCompleted, Payload: completion}} {
		entry.Index = uint64(i)
		entry.Epoch = owner.Epoch()
		var objects [][]byte
		if i == 0 {
			objects = [][]byte{[]byte(`7`)}
		} else if i == 2 {
			objects = [][]byte{encoded}
		}
		tail, err = graph.Append(ctx, h.Type, h.ID, h.InvSeq, entry, tail, objects, nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	records, _, err := graph.ReadExisting(ctx, h.Type, h.ID, h.InvSeq)
	if err != nil {
		t.Fatal(err)
	}
	worker := &Worker{graphJournal: graph, client: c}
	point := wf.ContinuationCheckpoint{ContinuationAnchor: wf.ContinuationAnchor{Index: 2, Epoch: owner.Epoch()}, Stage: "next", Object: "step-result-" + hash, SHA256: hash, StepPosition: 2}
	appendEntry := func(kind journal.Kind, payload json.RawMessage) error {
		if err := owner.Renew(ctx); err != nil {
			return err
		}
		var err error
		tail, err = graph.Append(ctx, h.Type, h.ID, h.InvSeq, journal.Entry{Index: uint64(len(records)), Epoch: owner.Epoch(), Kind: kind, Payload: payload}, tail, nil, nil)
		if err == nil {
			records, _, err = graph.ReadExisting(ctx, h.Type, h.ID, h.InvSeq)
		}
		return err
	}
	runs, err := js.Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	if err := runs.Purge(ctx); err != nil {
		t.Fatal(err)
	}
	wrong := point
	wrong.SHA256 = hex.EncodeToString(digest[:])
	wrong.Object = "step-result-" + wrong.SHA256
	if err := worker.publishContinuation(ctx, h.Type, h.ID, h.InvSeq, owner, records, wrong, appendEntry); !errors.Is(err, journal.ErrGap) {
		t.Fatal("unconfirmed checkpoint dispatched", err)
	}
	info, err := runs.Info(ctx)
	if err != nil || info.State.Msgs != 0 {
		t.Fatal("invalid handoff published", info, err)
	}
	if err := worker.publishContinuation(ctx, h.Type, h.ID, h.InvSeq, owner, records, point, appendEntry); err != nil {
		t.Fatal(err)
	}
	if len(records) != 4 || records[3].Kind != journal.Suspended {
		t.Fatal("missing suspension", records)
	}
	first, err := runs.Info(ctx)
	if err != nil || first.State.Msgs != 1 {
		t.Fatal(first, err)
	}
	if err := worker.publishContinuation(ctx, h.Type, h.ID, h.InvSeq, owner, records, point, appendEntry); err != nil {
		t.Fatal(err)
	}
	second, err := runs.Info(ctx)
	if err != nil || second.State.Msgs != 2 || len(records) != 4 {
		t.Fatal("retry suppressed or appended duplicate suspension", second, err)
	}
	if err := owner.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := worker.publishContinuation(ctx, h.Type, h.ID, h.InvSeq, owner, records, point, appendEntry); !errors.Is(err, lease.ErrLost) {
		t.Fatal("lost lease authorized handoff", err)
	}
	afterLoss, err := runs.Info(ctx)
	if err != nil || afterLoss.State.Msgs != 2 || len(records) != 4 {
		t.Fatal("lost lease mutated handoff", afterLoss, err)
	}
	legacy, err := js.Stream(ctx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	legacyInfo, err := legacy.Info(ctx)
	if err != nil || legacyInfo.State.Msgs != 0 {
		t.Fatal("handoff used legacy journal", legacyInfo, err)
	}
}
