package graphpublication

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"js-wf/internal/blobpublication"
	"js-wf/internal/retainedgraph"
)

func stageFixture(t *testing.T, name string, population int) (*memoryPort, Protocol, Root) {
	t.Helper()
	m, p := newModel("stage-" + name)
	root := EmptyRoot()
	for i := 0; i < population; i++ {
		root = appendOne(t, m, p, "owner", []byte(fmt.Sprint("record-", i)), [][]byte{[]byte("shared")})
	}
	return m, p, root
}

func stageCheckpoint(t *testing.T, stage *CompactionStage) []byte {
	t.Helper()
	data, err := stage.Checkpoint()
	if err != nil || len(data) > MaxCompactionCheckpointBytes {
		t.Fatal(len(data), err)
	}
	return data
}

func TestGraphPrefixCompactionStageBatchesAndResumption(t *testing.T) {
	for _, mode := range []string{"normal", "resume-every-record", "inherited-archive", "cancel", "put-drop", "put-lost", "ready-lost", "append", "retire", "reader", "collector", "stale-target"} {
		t.Run(mode, func(t *testing.T) {
			m, p, root := stageFixture(t, mode, 12)
			if mode == "inherited-archive" {
				prepared, err := p.PreparePrefixCompaction(ctx, "owner", root.Head, 2, 1024, epoch.Add(time.Minute), nil)
				if err != nil {
					t.Fatal(err)
				}
				root, err = p.CommitPrefixCompaction(ctx, prepared)
				if err != nil {
					t.Fatal(err)
				}
			}
			original := cloneRoot(root)
			stage, err := p.BeginPrefixCompaction(ctx, "owner", root.Head, 5, 1024, epoch.Add(time.Minute), []byte("cursor"))
			if err != nil {
				t.Fatal(err)
			}
			plan, done, err := stage.Advance(ctx, 1)
			if err != nil || done || plan.destination != "" || stage.NextIndex() != 1 || !reflect.DeepEqual(original, m.roots["owner"]) {
				t.Fatal("partial publication or unbounded work", done, err, stage.NextIndex())
			}
			checkpoint := stageCheckpoint(t, stage)
			if mode == "cancel" {
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				if _, done, err = stage.Advance(cancelled, 3); done || !errors.Is(err, context.Canceled) || stage.NextIndex() != 1 {
					t.Fatal(done, err)
				}
			}
			if mode == "put-drop" || mode == "put-lost" {
				m.putHook = func(name string, data []byte) error {
					if mode == "put-lost" {
						m.objects[name] = bytes.Clone(data)
					}
					return lostReply
				}
			}
			if mode == "ready-lost" {
				m.blobAfter = func(_ string, f Fence) error {
					if f.Phase == "ready" {
						m.blobAfter = nil
						return lostReply
					}
					return nil
				}
			}
			if mode == "put-drop" || mode == "put-lost" || mode == "ready-lost" {
				if _, done, err = stage.Advance(ctx, 3); done || err == nil || stage.NextIndex() != 1 {
					t.Fatal("uncertain record counted", done, err, stage.NextIndex())
				}
				if _, done, err = stage.Advance(ctx, 3); done || err == nil {
					t.Fatal("uncertain stage reused", done, err)
				}
				checkpoint = stageCheckpoint(t, stage)
			}
			if mode == "append" {
				appendOne(t, m, p, "owner", []byte("raced"), nil)
			}
			if mode == "retire" {
				if err = p.RetireLive(ctx, "owner", root.Head); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "reader" {
				if _, _, err = p.AcquireReader(ctx, "owner", root.Head, epoch.Add(time.Hour)); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "collector" {
				if _, err = p.SweepWithReaders(ctx, epoch.Add(time.Hour)); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "append" || mode == "retire" || mode == "reader" || mode == "collector" {
				before := cloneRoot(m.roots["owner"])
				if resumed, err := p.ResumePrefixCompaction(ctx, checkpoint); resumed != nil || !errors.Is(err, ErrConflict) {
					t.Fatal("stale source resumed", resumed, err)
				}
				if _, done, err = stage.Advance(ctx, 2); done || !errors.Is(err, ErrConflict) {
					t.Fatal("live stage ignored changed head", done, err)
				}
				if !reflect.DeepEqual(before, m.roots["owner"]) {
					t.Fatal("failed resumption changed root")
				}
				return
			}
			if mode == "stale-target" {
				var v compactionCheckpoint
				if err = json.Unmarshal(checkpoint, &v); err != nil {
					t.Fatal(err)
				}
				leaf := selectGraph(v.Publication.Graph, v.Publication.Streams, PrefixArchiveStream).Frontier[0].Link
				scope, _, err := objectAuthority(blobpublication.Object{Key: leaf.Hash, Reference: leaf.Reference})
				if err != nil {
					t.Fatal(err)
				}
				grant := m.blobs[scope]
				grant.Fence.Phase = "closed"
				grant.Fence.Object = ""
				grant.Fence.Intents = nil
				m.blobs[scope] = grant
			}
			stage, err = p.ResumePrefixCompaction(ctx, checkpoint)
			if err != nil {
				t.Fatal(err)
			}
			batches := 1
			for !done {
				start := stage.NextIndex()
				budget := uint64(3)
				if mode == "resume-every-record" {
					budget = 1
				}
				plan, done, err = stage.Advance(ctx, budget)
				batches++
				if err != nil || stage.NextIndex()-start > budget || (!done && plan.destination != "") {
					t.Fatal("invalid batch", done, err, start, stage.NextIndex())
				}
				if !reflect.DeepEqual(original, m.roots["owner"]) {
					t.Fatal("staging published authority")
				}
				checkpoint = stageCheckpoint(t, stage)
				stage, err = p.ResumePrefixCompaction(ctx, checkpoint)
				if err != nil {
					t.Fatal(err)
				}
			}
			plan, done, err = stage.Advance(ctx, 1)
			if err != nil || !done {
				t.Fatal(done, err)
			}
			published, err := p.CommitPrefixCompaction(ctx, plan)
			if mode == "stale-target" {
				if err == nil || !reflect.DeepEqual(original, m.roots["owner"]) {
					t.Fatal("stale target admitted", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if published.Graph.Count != root.Graph.Count-5 {
				t.Fatal("wrong live population")
			}
			if _, err = p.ResumePrefixCompaction(ctx, checkpoint); !errors.Is(err, ErrConflict) {
				t.Fatal("committed descriptor became authority", err)
			}
			if _, err = p.SweepWithReaders(ctx, epoch.Add(time.Hour)); err != nil {
				t.Fatal(err)
			}
			for index := uint64(0); index < root.Graph.Count; index++ {
				graph, nextIndex := published.Graph, index-5
				if index < 5 {
					graph = selectGraph(published.Graph, published.Streams, PrefixArchiveStream)
					nextIndex = graph.Count - 5 + index
				}
				record, err := retainedgraph.Read(ctx, stageStore{protocol: p}, graph, nextIndex)
				if err != nil || len(record.Blobs) != 1 {
					t.Fatal(record, err)
				}
				payload, err := m.Get(ctx, record.Blobs[0], 1024)
				if err != nil || string(payload) != "shared" {
					t.Fatal(string(payload), err)
				}
			}
			// Restart deduplication must not consume one grant location per batch.
			for _, grant := range m.blobs {
				if grant.Fence.Owner == plan.publication.Token {
					for _, intent := range grant.Fence.Intents {
						payloadLocations := 0
						for _, location := range intent.Locations {
							if location.Kind == "payload" {
								payloadLocations++
							}
						}
						if payloadLocations > 2 {
							t.Fatal("resumption grew shared-payload locations", payloadLocations)
						}
					}
				}
			}
			t.Logf("COMPACTION_STAGE mode=%s source_records=%d batches=%d checkpoint_bytes=%d archive=%d live=%d", mode, root.Graph.Count, batches, len(checkpoint), selectGraph(published.Graph, published.Streams, PrefixArchiveStream).Count, published.Graph.Count)
		})
	}
}

func TestGraphPrefixCompactionStageRejectsCheckpointMutations(t *testing.T) {
	for _, mode := range []string{"duplicate", "alias", "unknown", "trailing", "oversize", "schema", "head", "source", "progress", "cut", "readers", "streams"} {
		t.Run(mode, func(t *testing.T) {
			m, p, root := stageFixture(t, mode, 4)
			stage, err := p.BeginPrefixCompaction(ctx, "owner", root.Head, 2, 1024, epoch.Add(time.Minute), nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, done, err := stage.Advance(ctx, 1); err != nil || done {
				t.Fatal(done, err)
			}
			data := stageCheckpoint(t, stage)
			var v compactionCheckpoint
			if err = json.Unmarshal(data, &v); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "duplicate":
				data = append([]byte(`{"Schema":"`+CompactionCheckpointSchema+`",`), data[1:]...)
			case "alias":
				data = bytes.Replace(data, []byte(`"Schema"`), []byte(`"schema"`), 1)
			case "unknown":
				data = append([]byte(`{"foreign":1,`), data[1:]...)
			case "trailing":
				data = append(data, []byte(`{}`)...)
			case "oversize":
				data = bytes.Repeat([]byte(" "), MaxCompactionCheckpointBytes+1)
			case "schema":
				v.Schema = "foreign"
			case "head":
				v.Expected++
			case "source":
				v.Base.Token = "forged-source"
			case "progress":
				v.Next++
			case "cut":
				v.First = 0
			case "readers":
				v.Publication.Readers = []ReaderPin{{ID: "forged", Expires: epoch.Add(time.Hour), Graph: root.Graph}}
			case "streams":
				setStream(&v.Publication, "foreign", retainedgraph.Empty())
			}
			if mode != "duplicate" && mode != "alias" && mode != "unknown" && mode != "trailing" && mode != "oversize" {
				data, err = json.Marshal(v)
				if err != nil {
					t.Fatal(err)
				}
			}
			before := cloneRoot(m.roots["owner"])
			if resumed, err := p.ResumePrefixCompaction(ctx, data); resumed != nil || err == nil {
				t.Fatal("forged descriptor accepted", resumed, err)
			}
			if !reflect.DeepEqual(before, m.roots["owner"]) {
				t.Fatal("failed resume changed authority")
			}
		})
	}
}

func TestGraphPrefixCompactionStageSeededBudgets(t *testing.T) {
	for seed := int64(1); seed <= 32; seed++ {
		rng := rand.New(rand.NewSource(seed))
		m, p, root := stageFixture(t, fmt.Sprint(seed), 3+rng.Intn(14))
		cut := uint64(1 + rng.Intn(int(root.Graph.Count)))
		stage, err := p.BeginPrefixCompaction(ctx, "owner", root.Head, cut, 1024, epoch.Add(time.Minute), nil)
		if err != nil {
			t.Fatal(seed, err)
		}
		for {
			budget := uint64(1 + rng.Intn(5))
			before := stage.NextIndex()
			plan, done, err := stage.Advance(ctx, budget)
			if err != nil || stage.NextIndex()-before > budget {
				t.Fatal(seed, before, stage.NextIndex(), err)
			}
			if done {
				if _, err = p.CommitPrefixCompaction(ctx, plan); err != nil {
					t.Fatal(seed, err)
				}
				break
			}
			if rng.Intn(2) == 0 {
				stage, err = p.ResumePrefixCompaction(ctx, stageCheckpoint(t, stage))
				if err != nil {
					t.Fatal(seed, err)
				}
			}
			if m.roots["owner"].Head != root.Head {
				t.Fatal("partial random batch published")
			}
		}
	}
}

func TestGraphPrefixCompactionStageDoesNotCertifyForgedPrefix(t *testing.T) {
	for _, mode := range []string{"data", "payload"} {
		t.Run(mode, func(t *testing.T) {
			m, p, root := stageFixture(t, "forged-"+mode, 4)
			stage, err := p.BeginPrefixCompaction(ctx, "owner", root.Head, 2, 1024, epoch.Add(time.Minute), nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, done, err := stage.Advance(ctx, 1); err != nil || done {
				t.Fatal(done, err)
			}
			var v compactionCheckpoint
			if err = json.Unmarshal(stageCheckpoint(t, stage), &v); err != nil {
				t.Fatal(err)
			}
			archive := selectGraph(v.Publication.Graph, v.Publication.Streams, PrefixArchiveStream)
			record, err := retainedgraph.Read(ctx, stageStore{protocol: p}, archive, 0)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "data" {
				record.Data = []byte("forged-record")
			} else {
				data := []byte("forged-payload")
				intent := Intent{Destination: "owner", Expected: root.Head, Expires: v.Expires, Locations: []Location{{Kind: "payload", First: 0, Stream: PrefixArchiveStream}}}
				ref, err := p.acquire(ctx, key(data), data, v.Publication.Token, intent)
				if err != nil {
					t.Fatal(err)
				}
				record.Blobs = []retainedgraph.Link{{Hash: key(data), Reference: ref}}
			}
			archive, err = retainedgraph.Append(ctx, stageStore{protocol: p, destination: "owner", expected: root.Head, expires: v.Expires, token: v.Publication.Token, index: 0, stream: PrefixArchiveStream}, retainedgraph.Empty(), record)
			if err != nil {
				t.Fatal(err)
			}
			setStream(&v.Publication, PrefixArchiveStream, archive)
			data, err := json.Marshal(v)
			if err != nil {
				t.Fatal(err)
			}
			resumed, err := p.ResumePrefixCompaction(ctx, data)
			if err != nil {
				t.Fatal("structurally valid staging input rejected before independent verification", err)
			}
			plan, done, err := resumed.Advance(ctx, 4)
			if err != nil || !done {
				t.Fatal(done, err)
			}
			if _, err = p.CommitPrefixCompaction(ctx, plan); err == nil || !reflect.DeepEqual(root, m.roots["owner"]) {
				t.Fatal("forged progress certified prefix", err)
			}
		})
	}
}

func TestGraphPrefixCompactionStageReturnedPlanOwnership(t *testing.T) {
	_, p, root := stageFixture(t, "result-copy", 4)
	stage, err := p.BeginPrefixCompaction(ctx, "owner", root.Head, 2, 1024, epoch.Add(time.Minute), []byte("cursor"))
	if err != nil {
		t.Fatal(err)
	}
	plan, done, err := stage.Advance(ctx, 4)
	if err != nil || !done {
		t.Fatal(done, err)
	}
	before := stageCheckpoint(t, stage)
	plan.publication.Application[0] = 'x'
	plan.publication.Graph.Frontier[0].Link.Hash = "forged"
	plan.publication.Streams[0].Graph.Frontier[0].Link.Hash = "forged"
	plan.base.Graph.Frontier[0].Link.Hash = "forged"
	if after := stageCheckpoint(t, stage); !bytes.Equal(before, after) {
		t.Fatal("caller mutated private staging state")
	}
	plan, done, err = stage.Advance(ctx, 1)
	if err != nil || !done {
		t.Fatal(done, err)
	}
	if _, err = p.CommitPrefixCompaction(ctx, plan); err != nil {
		t.Fatal(err)
	}
}
