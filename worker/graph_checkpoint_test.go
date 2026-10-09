package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/internal/checkpoint"
	"js-wf/internal/graphpublication"
	"js-wf/internal/retainedgraph"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/testcluster"
	"js-wf/wf"
)

type checkpointPrefixPort struct {
	*graphpublication.NativePort
	blocked      map[string]bool
	blockedReads int
}

func (p *checkpointPrefixPort) Get(ctx context.Context, link retainedgraph.Link, limit int) ([]byte, error) {
	if p.blocked[link.Hash] {
		p.blockedReads++
		return nil, fmt.Errorf("prefix entry unavailable")
	}
	return p.NativePort.Get(ctx, link, limit)
}

func TestNativeGraphCheckpointMaterializedReferences(t *testing.T) {
	for _, domain := range []string{"", "WFMATERIALIZED"} {
		name := "R1"
		replicas := 1
		if domain != "" {
			name = "R3Domain"
			replicas = 3
		}
		t.Run(name, func(t *testing.T) {
			var cluster *testcluster.Cluster
			var err error
			if domain == "" {
				cluster, err = testcluster.Start(t.TempDir(), replicas)
			} else {
				cluster, err = testcluster.StartWithDomain(t.TempDir(), replicas, domain)
			}
			if err != nil {
				t.Fatal(err)
			}
			defer cluster.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if replicas > 1 {
				for ctx.Err() == nil {
					ready := false
					for _, s := range cluster.Servers {
						ready = ready || s.JetStreamIsLeader() && len(s.JetStreamClusterPeers()) == replicas
					}
					if ready {
						break
					}
					time.Sleep(20 * time.Millisecond)
				}
			}
			var js jetstream.JetStream
			if domain == "" {
				js, err = jetstream.New(cluster.Clients[0])
			} else {
				js, err = jetstream.NewWithDomain(cluster.Clients[0], domain)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := provision.Ensure(ctx, js, replicas); err != nil {
				t.Fatal(err)
			}
			cfg := journal.NativeGraphConfig{AuthorityStream: "MATERIALIZED_AUTH", AuthorityPrefix: "wf.graph.materialized", ObjectBucket: "MATERIALIZED_OBJECTS", ExpectedReplicas: replicas, CanonicalStarts: true, CanonicalSignals: true, CheckpointIndex: true}
			configs, err := journal.NativeGraphStreamConfigs(cfg, replicas)
			if err != nil {
				t.Fatal(err)
			}
			for _, config := range configs {
				if _, err := js.CreateStream(ctx, config); err != nil {
					t.Fatal(err)
				}
			}
			authority, err := graphpublication.OpenNativeAuthority(ctx, js, cfg.AuthorityStream, cfg.AuthorityPrefix)
			if err != nil {
				t.Fatal(err)
			}
			native, err := graphpublication.OpenNativePort(ctx, authority, cfg.ObjectBucket)
			if err != nil {
				t.Fatal(err)
			}
			port := &checkpointPrefixPort{NativePort: native, blocked: map[string]bool{}}
			store, err := journal.NewGraphStore(journal.GraphConfig{Protocol: graphpublication.Protocol{Port: port}, CanonicalStarts: true, CanonicalSignals: true, CheckpointIndex: true})
			if err != nil {
				t.Fatal(err)
			}
			c, err := client.New(js).WithGraphJournal(store)
			if err != nil {
				t.Fatal(err)
			}
			h, err := c.Start(ctx, "test", "materialized", []byte(`7`))
			if err != nil {
				t.Fatal(err)
			}
			g, err := openGraphDelivery(ctx, store, h.Type, h.ID, h.InvSeq)
			if err != nil {
				t.Fatal(err)
			}
			defer g.close(context.Background())
			tail := g.view.Tail()
			appendEntry := func(kind journal.Kind, payload []byte) error {
				var err error
				tail, err = g.append(ctx, journal.Entry{Index: uint64(len(g.records)), Epoch: 3, Kind: kind, Payload: payload}, tail)
				return err
			}
			g.input = []byte(`7`)
			started, _ := json.Marshal(map[string]string{"input_sha256": graphHash(g.input)})
			if err := appendEntry(journal.Started, started); err != nil {
				t.Fatal(err)
			}
			if err := appendEntry(journal.StepRequested, []byte(`{"kind":"run","name":"source"}`)); err != nil {
				t.Fatal(err)
			}
			payload := []byte(strings.Repeat("owned", 16384))
			sourceHash := graphHash(payload)
			sourceRef := "step-result-" + sourceHash
			if err := g.PutBytes(ctx, sourceRef, payload); err != nil {
				t.Fatal(err)
			}
			result, _ := json.Marshal(map[string]string{"result_ref": sourceRef, "result_hash": sourceHash})
			if err := appendEntry(journal.StepCompleted, result); err != nil {
				t.Fatal(err)
			}
			oldSource := g.refs[sourceRef]
			locals := json.RawMessage(`{"count":1}`)
			request, _ := json.Marshal(map[string]string{"kind": "checkpoint", "name": "next", "input_hash": graphHash(locals)})
			if err := appendEntry(journal.StepRequested, request); err != nil {
				t.Fatal(err)
			}
			checkpointTail := tail
			makeFrame := func(mode string) ([]byte, string) {
				outcome := wf.Outcome{InvSeq: 99, ResultRef: sourceRef, ResultHash: sourceHash}
				frame := checkpoint.Frame{Version: checkpoint.Version, Identity: checkpoint.Identity{Type: h.Type, ID: h.ID, InvSeq: h.InvSeq}, Stage: "next", Data: locals, Anchor: checkpoint.Anchor{Index: 4, Epoch: 3}, StepPosition: 4, PromiseOutcomes: map[string]json.RawMessage{}}
				if mode == "missing" || mode == "unowned-staged" {
					outcome.ResultRef = "step-result-" + graphHash([]byte("unowned"))
					outcome.ResultHash = graphHash([]byte("unowned"))
				}
				if mode == "forged-hash" {
					outcome.ResultHash = graphHash([]byte("forged"))
				}
				if mode == "wrong-epoch" {
					frame.Anchor.Epoch++
				}
				raw, _ := json.Marshal(outcome)
				frame.PromiseOutcomes["child"] = raw
				frame.PromiseOutcomes["again"] = raw
				encoded, hash, err := checkpoint.Encode(frame)
				if err != nil {
					t.Fatal(err)
				}
				return encoded, hash
			}
			for _, mode := range []string{"missing", "unowned-staged", "forged-hash", "wrong-epoch"} {
				if mode == "unowned-staged" {
					unowned := []byte("unowned")
					if err := g.PutBytes(ctx, "step-result-"+graphHash(unowned), unowned); err != nil {
						t.Fatal(err)
					}
				}
				encoded, hash := makeFrame(mode)
				ref := "step-result-" + hash
				if err := g.PutBytes(ctx, ref, encoded); err != nil {
					t.Fatal(err)
				}
				completed, _ := json.Marshal(map[string]string{"result_ref": ref, "result_hash": hash})
				_, err := g.append(ctx, journal.Entry{Index: 4, Epoch: 3, Kind: journal.StepCompleted, Payload: completed}, checkpointTail)
				if !errors.Is(err, journal.ErrGap) || g.view.Count() != 4 {
					t.Fatal("invalid materialization appended", mode, err, g.view.Count())
				}
			}
			encoded, hash := makeFrame("valid")
			if err := g.PutBytes(ctx, "step-result-"+hash, encoded); err != nil {
				t.Fatal(err)
			}
			completion, _ := json.Marshal(map[string]string{"result_ref": "step-result-" + hash, "result_hash": hash})
			if err := appendEntry(journal.StepCompleted, completion); err != nil {
				t.Fatal(err)
			}
			anchor, err := g.view.Read(ctx, 4)
			if err != nil {
				t.Fatal(err)
			}
			if len(anchor.Blobs) != 2 || g.refs[sourceRef].index != 4 {
				t.Fatal("payload not atomically materialized", anchor.Blobs, g.refs[sourceRef])
			}
			found, err := g.view.ReadCheckpoint(ctx, h.Type, h.ID)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.PublishCheckpoint(ctx, h.Type, h.ID, found.Runtime, tail); err != nil {
				t.Fatal(err)
			}
			for i := uint64(0); i < 3; i++ {
				record, err := g.view.Read(ctx, i)
				if err != nil {
					t.Fatal(err)
				}
				port.blocked[record.EntryBlob.Hash] = true
			}
			if _, err := g.view.Payload(ctx, oldSource.index, oldSource.link, store.PayloadReadLimit()); err == nil {
				t.Fatal("negative prefix control did not fail")
			}
			port.blockedReads = 0
			got, err := g.GetBytes(ctx, sourceRef)
			if err != nil || !bytes.Equal(got, payload) || port.blockedReads != 0 {
				t.Fatal("materialized read touched prefix", err, port.blockedReads)
			}
			fresh, err := store.OpenExisting(ctx, h.Type, h.ID, h.InvSeq)
			if err != nil {
				t.Fatal(err)
			}
			defer fresh.Close(context.Background())
			indexed, err := fresh.ReadCheckpoint(ctx, h.Type, h.ID)
			if err != nil || indexed.Runtime != found.Runtime || port.blockedReads != 0 {
				t.Fatal("indexed frame touched prefix", indexed, err, port.blockedReads)
			}
			got, err = fresh.Payload(ctx, 4, g.refs[sourceRef].link, store.PayloadReadLimit())
			if err != nil || !bytes.Equal(got, payload) || port.blockedReads != 0 {
				t.Fatal("fresh completion ownership lost", err, port.blockedReads)
			}
			t.Logf("promise aliases=2 completion_payload_edges=%d materialized_index=%d blocked_prefix_reads=%d", len(anchor.Blobs), g.refs[sourceRef].index, port.blockedReads)
		})
	}
}
