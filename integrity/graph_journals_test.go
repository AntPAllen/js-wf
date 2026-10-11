package integrity

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/internal/blobpublication"
	"js-wf/internal/graphpublication"
	"js-wf/internal/retainedgraph"
	"js-wf/journal"
)

func rawJournalFixture(t *testing.T, encoding journal.Encoding, archive bool, alter func([]journal.Entry), wrongEnvelope bool, missingEntry bool) (GraphJournalSnapshot, map[string][]byte) {
	return rawJournalCheckpointFixture(t, encoding, archive, alter, wrongEnvelope, missingEntry, "")
}

func rawJournalCheckpointFixture(t *testing.T, encoding journal.Encoding, archive bool, alter func([]journal.Entry), wrongEnvelope bool, missingEntry bool, control string) (GraphJournalSnapshot, map[string][]byte) {
	t.Helper()
	entries := []journal.Entry{
		{Index: 0, Epoch: 1, WorkerID: "worker", Kind: journal.Started},
		{Index: 1, Epoch: 1, WorkerID: "worker", Kind: journal.StepRequested, Payload: json.RawMessage(`{"kind":"run","name":"x","input_hash":"` + digest([]byte("1")) + `"}`)},
		{Index: 2, Epoch: 1, WorkerID: "worker", Kind: journal.StepCompleted, Payload: json.RawMessage(`{"result":42}`)},
		{Index: 3, Epoch: 1, WorkerID: "worker", Kind: journal.Completed, Payload: json.RawMessage(`{"inv_seq":10,"result":"NDI="}`)},
	}
	var frameBytes []byte
	var pointer auditedCheckpointPointer
	if archive {
		// Independently encoded frame and pointer; no production writer/reader.
		frame := map[string]any{"version": 1, "identity": map[string]any{"type": "kind", "id": "id", "inv_seq": 10}, "stage": "next", "data": map[string]int{"x": 1}, "anchor": map[string]uint64{"index": 2, "epoch": 1}, "step_position": 2}
		switch control {
		case "frame-generation":
			frame["identity"] = map[string]any{"type": "kind", "id": "id", "inv_seq": 11}
		case "frame-anchor":
			frame["anchor"] = map[string]uint64{"index": 3, "epoch": 1}
		case "frame-stage":
			frame["stage"] = "wrong"
		case "frame-position":
			frame["step_position"] = 4
		case "frame-version":
			frame["version"] = 9
		case "frame-locals":
			frame["data"] = map[string]int{"x": 2}
		}
		frameBytes, _ = json.Marshal(frame)
		hash := digest(frameBytes)
		entries[1].Payload = json.RawMessage(`{"kind":"checkpoint","name":"next","input_hash":"` + digest([]byte(`{"x":1}`)) + `"}`)
		entries[2].Payload = json.RawMessage(`{"result_ref":"step-result-` + hash + `","result_hash":"` + hash + `"}`)
		pointer.RequestIndex = 1
		pointer.Runtime.InvSeq, pointer.Runtime.Stage, pointer.Runtime.Sequence, pointer.Runtime.Index, pointer.Runtime.Epoch, pointer.Runtime.StepPosition, pointer.Runtime.Object, pointer.Runtime.SHA256 = 10, "next", 103, 2, 1, 2, "step-result-"+hash, hash
		switch control {
		case "pointer-generation":
			pointer.Runtime.InvSeq++
		case "pointer-sequence":
			pointer.Runtime.Sequence++
		case "pointer-request":
			pointer.RequestIndex = 2
		case "pointer-position":
			pointer.Runtime.StepPosition = 4
		case "pointer-object":
			pointer.Runtime.Object = "other"
		case "completion-result":
			entries[2].Payload = json.RawMessage(`{"result":42}`)
		case "request-kind":
			entries[1].Payload = json.RawMessage(`{"kind":"run","name":"next","input_hash":"` + digest([]byte(`{"x":1}`)) + `"}`)
		}
	}
	if alter != nil {
		alter(entries)
	}
	objects := map[string][]byte{}
	projections := map[string][]byte{"kind.id": entries[3].Payload}
	graph := GraphReferenceSnapshot{Roots: map[string]graphpublication.Root{}, Fences: map[string]graphpublication.Fence{}, PayloadLimit: 1024}
	graph.LoadObject = func(ctx context.Context, name string, limit int) ([]byte, error) {
		data, ok := objects[name]
		if !ok {
			return nil, fmt.Errorf("missing object")
		}
		return data, nil
	}
	destination := graphDestinationForKey("kind.id")
	put := func(data []byte, location graphpublication.Location) retainedgraph.Link {
		hash := digest(data)
		name := hash + "/1/" + hex.EncodeToString([]byte("owner")) + "-" + hex.EncodeToString([]byte("attempt"))
		objects[name] = data
		scope := digest([]byte("graph-authority/" + hash + "/owner"))
		graph.Fences[scope] = graphpublication.Fence{Hash: hash, Owner: "owner", Generation: 1, Phase: "ready", Object: name, Intents: map[string]graphpublication.Intent{"owner": {Destination: destination, Expected: 0, Expires: time.Unix(100, 0), Locations: []graphpublication.Location{location}}}}
		return retainedgraph.Link{Hash: hash, Reference: blobpublication.Reference{Generation: 1, Object: name}}
	}
	leaf := func(index, first uint64, stream string) retainedgraph.Link {
		data, err := journal.MarshalEntry(entries[index], encoding)
		if err != nil {
			t.Fatal(err)
		}
		edge := put(data, graphpublication.Location{Kind: "payload", First: first, Stream: stream})
		inv := uint64(10)
		if wrongEnvelope && index == 1 {
			inv = 9
		}
		envelope, _ := json.Marshal(struct {
			Schema      string `json:"schema"`
			Invocation  uint64 `json:"invocation"`
			Sequence    uint64 `json:"sequence"`
			EntrySHA256 string `json:"entry_sha256"`
		}{"js-wf-graph-journal-entry-v1", inv, 101 + index, edge.Hash})
		blobs := []retainedgraph.Link{edge}
		if archive && index == 2 && control != "frame-unowned" {
			blobs = append(blobs, put(frameBytes, graphpublication.Location{Kind: "payload", First: first, Stream: stream}))
		}
		if missingEntry && index == 1 {
			blobs = nil
		}
		node, _ := json.Marshal(struct {
			Schema string
			First  uint64
			Height uint8
			Record *retainedgraph.Record
		}{retainedgraph.Schema, first, 0, &retainedgraph.Record{Data: envelope, Blobs: blobs}})
		return put(node, graphpublication.Location{Kind: "node", First: first, Stream: stream})
	}
	var tree func(uint64, uint64, uint8, string) retainedgraph.Tree
	tree = func(index, first uint64, height uint8, stream string) retainedgraph.Tree {
		if height == 0 {
			return retainedgraph.Tree{First: first, Link: leaf(index, first, stream)}
		}
		width := uint64(1) << (height - 1)
		left, right := tree(index, first, height-1, stream), tree(index+width, first+width, height-1, stream)
		data, _ := json.Marshal(struct {
			Schema   string
			First    uint64
			Height   uint8
			Children []retainedgraph.Link
		}{retainedgraph.Schema, first, height, []retainedgraph.Link{left.Link, right.Link}})
		return retainedgraph.Tree{First: first, Height: height, Link: put(data, graphpublication.Location{Kind: "node", First: first, Height: height, Stream: stream})}
	}
	cursor := auditedGraphCursor{Schema: "js-wf-graph-journal-cursor-v1", Invocation: 10, Base: 100, Count: 4, Epoch: 1, Kind: journal.Completed}
	root := graphpublication.Root{Schema: graphpublication.ApplicationSchema, Head: 20, Token: "owner", Graph: retainedgraph.Root{Schema: retainedgraph.Schema, Count: 4, Frontier: []retainedgraph.Tree{tree(0, 0, 2, "")}}}
	source := &jetstream.RawStreamMsg{Sequence: 10, Subject: "wf.inv.kind.id", Data: []byte("input")}
	if archive {
		// Rebuild physical forests with their actual per-forest coordinates.
		objects = map[string][]byte{}
		graph.Fences = map[string]graphpublication.Fence{}
		cursor.Schema = "js-wf-graph-runtime-cursor-v6"
		cursor.RetainedFrom = 1
		cursor.Checkpoint, _ = json.Marshal(pointer)
		start := journal.GraphStart{Schema: "js-wf-canonical-start-v1", Token: "start", Request: journal.GraphStartRequest{Type: "kind", ID: "id"}, InputSHA256: digest([]byte("input")), InputSize: 5}
		cursor.Start = &start
		inputLink := put([]byte("input"), graphpublication.Location{Kind: "payload", First: 0, Stream: "input"})
		startData, _ := json.Marshal(start)
		inputNode, _ := json.Marshal(struct {
			Schema string
			First  uint64
			Height uint8
			Record *retainedgraph.Record
		}{retainedgraph.Schema, 0, 0, &retainedgraph.Record{Data: startData, Blobs: []retainedgraph.Link{inputLink}}})
		root.Schema = graphpublication.StreamsSchema
		root.Graph = retainedgraph.Root{Schema: retainedgraph.Schema, Count: 3, Frontier: []retainedgraph.Tree{tree(1, 0, 1, ""), tree(3, 2, 0, "")}}
		root.Streams = []graphpublication.StreamGraph{{Name: "archive", Graph: retainedgraph.Root{Schema: retainedgraph.Schema, Count: 1, Frontier: []retainedgraph.Tree{tree(0, 0, 0, "archive")}}}, {Name: "input", Graph: retainedgraph.Root{Schema: retainedgraph.Schema, Count: 1, Frontier: []retainedgraph.Tree{{Link: put(inputNode, graphpublication.Location{Kind: "node", Stream: "input"})}}}}}
		source.Data = []byte(`{"schema":"js-wf-canonical-start-pointer-v1","token":"start"}`)
		source.Header = nats.Header{"Wf-Graph-Start-Token": []string{"start"}, "Wf-Input-SHA256": []string{start.InputSHA256}}
	}
	root.Application, _ = json.Marshal(cursor)
	graph.Roots[destination] = root
	return GraphJournalSnapshot{Graph: graph, Invocations: map[string]*jetstream.RawStreamMsg{"kind.id": source}, ReadProjection: func(ctx context.Context, key string) ([]byte, error) {
		v, ok := projections[key]
		if !ok {
			return nil, fmt.Errorf("missing projection")
		}
		return v, nil
	}}, projections
}

func TestRawGraphJournalGenerationsOutcomesAndArchive(t *testing.T) {
	for _, encoding := range []journal.Encoding{journal.JSON, journal.ProtobufV1} {
		for _, archive := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/archive%v", encoding, archive), func(t *testing.T) {
				s, _ := rawJournalFixture(t, encoding, archive, nil, false, false)
				r, err := CheckGraphJournals(context.Background(), s)
				if err != nil || r.Invocations != 1 || r.Journals != 1 || r.Entries != 4 || r.Terminal != 1 {
					t.Fatal(r, err)
				}
			})
		}
	}
	controls := []struct {
		name              string
		alter             func([]journal.Entry)
		envelope, missing bool
	}{
		{"envelope-generation", nil, true, false},
		{"entry-edge-missing", nil, false, true},
		{"index-gap", func(e []journal.Entry) { e[1].Index = 9 }, false, false},
		{"epoch-decrease", func(e []journal.Entry) { e[2].Epoch = 0 }, false, false},
		{"epoch-two-workers", func(e []journal.Entry) { e[2].WorkerID = "another" }, false, false},
		{"completion-without-request", func(e []journal.Entry) { e[1].Kind = journal.StepCompleted }, false, false},
		{"overlapping-requests", func(e []journal.Entry) { e[2].Kind = journal.StepRequested }, false, false},
		{"entry-after-terminal", func(e []journal.Entry) { e[1].Kind = journal.Completed; e[1].Payload = e[3].Payload }, false, false},
		{"terminal-generation", func(e []journal.Entry) { e[3].Payload = json.RawMessage(`{"inv_seq":11,"result":"NDI="}`) }, false, false},
		{"result-unowned", func(e []journal.Entry) {
			e[3].Payload = json.RawMessage(`{"inv_seq":10,"result_ref":"external","result_hash":"` + digest([]byte("result")) + `"}`)
		}, false, false},
	}
	for _, c := range controls {
		t.Run(c.name, func(t *testing.T) {
			s, _ := rawJournalFixture(t, journal.JSON, false, c.alter, c.envelope, c.missing)
			if _, err := CheckGraphJournals(context.Background(), s); err == nil {
				t.Fatal("corrupt raw journal accepted")
			}
		})
	}
	for _, mode := range []string{"source-sequence", "source-missing", "projection-differs", "projection-missing", "input-source-token", "archive-count"} {
		t.Run(mode, func(t *testing.T) {
			s, p := rawJournalFixture(t, journal.JSON, mode == "input-source-token" || mode == "archive-count", nil, false, false)
			switch mode {
			case "source-sequence":
				s.Invocations["kind.id"].Sequence++
			case "source-missing":
				delete(s.Invocations, "kind.id")
			case "projection-differs":
				p["kind.id"] = []byte(`{}`)
			case "projection-missing":
				delete(p, "kind.id")
			case "input-source-token":
				s.Invocations["kind.id"].Header.Set("Wf-Graph-Start-Token", "other")
			case "archive-count":
				for k, r := range s.Graph.Roots {
					var c auditedGraphCursor
					_ = json.Unmarshal(r.Application, &c)
					c.RetainedFrom = 2
					r.Application, _ = json.Marshal(c)
					s.Graph.Roots[k] = r
				}
			}
			if _, err := CheckGraphJournals(context.Background(), s); err == nil {
				t.Fatal("corrupt raw binding accepted")
			}
		})
	}
}

func TestRawGraphCheckpointPointerAndFrameBindings(t *testing.T) {
	for _, control := range []string{"pointer-generation", "pointer-sequence", "pointer-request", "pointer-position", "pointer-object", "completion-result", "request-kind", "frame-unowned", "frame-generation", "frame-anchor", "frame-stage", "frame-position", "frame-version", "frame-locals"} {
		t.Run(control, func(t *testing.T) {
			s, _ := rawJournalCheckpointFixture(t, journal.JSON, true, nil, false, false, control)
			// Reference integrity must still pass, isolating semantic rejection.
			if _, err := CheckGraphReferences(context.Background(), s.Graph); err != nil {
				t.Fatal("fixture reference corruption", err)
			}
			if _, err := CheckGraphJournals(context.Background(), s); err == nil {
				t.Fatal("corrupt checkpoint accepted")
			}
		})
	}
}
