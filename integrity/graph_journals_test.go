package integrity

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
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
	history := strings.HasPrefix(control, "history-")
	requestIndex, frameIndex, position := uint64(1), uint64(2), uint64(2)
	var stateResult []byte
	if history {
		requestIndex, frameIndex, position = 3, 4, 4
		stateRequest := json.RawMessage(`{"kind":"state_set","name":"value","input_hash":"` + digest([]byte(`42`)) + `"}`)
		stateCompletion := json.RawMessage(`{"result":{"found":true,"value":42}}`)
		switch control {
		case "history-write-hash":
			stateRequest = json.RawMessage(`{"kind":"state_set","name":"value","input_hash":"` + digest([]byte(`43`)) + `"}`)
		case "history-write-missing":
			stateCompletion = json.RawMessage(`{"result":{"found":false}}`)
		case "history-write-error", "history-write-error-valid":
			stateCompletion = json.RawMessage(`{"error":"failed"}`)
		case "history-read-absent-valid":
			stateRequest = json.RawMessage(`{"kind":"state_get","name":"value","input_hash":"` + digest([]byte(`null`)) + `"}`)
			stateCompletion = json.RawMessage(`{"result":{"found":false}}`)
		case "history-read-fabricated":
			stateRequest = json.RawMessage(`{"kind":"state_get","name":"value","input_hash":"` + digest([]byte(`null`)) + `"}`)
		case "history-null-valid":
			stateRequest = json.RawMessage(`{"kind":"state_set","name":"value","input_hash":"` + digest([]byte(`null`)) + `"}`)
			stateCompletion = json.RawMessage(`{"result":{"found":true,"value":null}}`)
		case "history-result-null":
			stateCompletion = json.RawMessage(`{"result":null}`)
		case "history-result-unknown":
			stateCompletion = json.RawMessage(`{"result":{"found":true,"value":42,"other":1}}`)
		}
		if strings.HasPrefix(control, "history-external-") {
			stateResult = []byte(`{"found":true,"value":42}`)
			stateCompletion = json.RawMessage(`{"result_ref":"step-result-` + digest(stateResult) + `","result_hash":"` + digest(stateResult) + `"}`)
			if control == "history-external-wrong" {
				stateResult = []byte(`{"found":true,"value":43}`)
			}
		}
		entries = []journal.Entry{entries[0], {Index: 1, Epoch: 1, WorkerID: "worker", Kind: journal.StepRequested, Payload: stateRequest}, {Index: 2, Epoch: 1, WorkerID: "worker", Kind: journal.StepCompleted, Payload: stateCompletion}, {Index: 3, Epoch: 1, WorkerID: "worker", Kind: journal.StepRequested}, {Index: 4, Epoch: 1, WorkerID: "worker", Kind: journal.StepCompleted}, {Index: 5, Epoch: 1, WorkerID: "worker", Kind: journal.Completed, Payload: entries[3].Payload}}
	}
	var frameBytes []byte
	var promiseBytes [][]byte
	var metadataBytes []byte
	var pointer auditedCheckpointPointer
	if archive {
		// Independently encoded frame and pointer; no production writer/reader.
		frame := map[string]any{"version": 1, "identity": map[string]any{"type": "kind", "id": "id", "inv_seq": 10}, "stage": "next", "data": map[string]int{"x": 1}, "anchor": map[string]uint64{"index": frameIndex, "epoch": 1}, "step_position": position}
		switch control {
		case "promise-owned-valid", "promise-owned-missing", "promise-owned-wrong", "promise-owned-conflict":
			outcomes := map[string]any{"child": map[string]any{"inv_seq": 3, "result_ref": "promise-result", "result_hash": digest([]byte(`42`))}}
			frame["promise_outcomes"] = outcomes
			if control != "promise-owned-missing" {
				promiseBytes = [][]byte{[]byte(`42`)}
			}
			if control == "promise-owned-wrong" {
				promiseBytes = [][]byte{[]byte(`43`)}
			}
			if control == "promise-owned-conflict" {
				outcomes["second"] = map[string]any{"inv_seq": 4, "result_ref": "promise-result", "result_hash": digest([]byte(`43`))}
				promiseBytes = append(promiseBytes, []byte(`43`))
			}
		case "materialized-valid":
			frame["state"] = map[string]any{"value": 42}
			frame["promise_outcomes"] = map[string]any{"child": map[string]any{"inv_seq": 3, "result": "NDI="}}
			frame["signal_cursor"] = 3
			frame["pending_signals"] = []map[string]any{{"sequence": 2, "name": "gate", "payload": "NDI="}}
			frame["consumed_signals"] = []uint64{1, 3}
			frame["cancelled_timers"] = []uint64{0}
		case "state-name":
			frame["state"] = map[string]any{"bad.name": 42}
		case "promise-name":
			frame["promise_outcomes"] = map[string]any{"bad.name": map[string]any{}}
		case "promise-nonobject":
			frame["promise_outcomes"] = map[string]any{"child": 42}
		case "promise-unknown-field":
			frame["promise_outcomes"] = map[string]any{"child": map[string]any{"unknown": 42}}
		case "promise-hash-no-ref":
			frame["promise_outcomes"] = map[string]any{"child": map[string]any{"result_hash": digest([]byte("42"))}}
		case "promise-ref-bad-hash":
			frame["promise_outcomes"] = map[string]any{"child": map[string]any{"result_ref": "external", "result_hash": "bad"}}
		case "promise-inline-and-ref":
			frame["promise_outcomes"] = map[string]any{"child": map[string]any{"result_ref": "external", "result_hash": digest([]byte("42")), "result": "NDI="}}
		case "consumed-zero":
			frame["consumed_signals"] = []uint64{0}
		case "consumed-duplicate":
			frame["consumed_signals"] = []uint64{1, 1}
		case "consumed-unsorted":
			frame["consumed_signals"] = []uint64{3, 1}
		case "pending-zero":
			frame["signal_cursor"] = 3
			frame["pending_signals"] = []map[string]any{{"sequence": 0, "name": "gate"}}
		case "pending-beyond-cursor":
			frame["signal_cursor"] = 1
			frame["pending_signals"] = []map[string]any{{"sequence": 2, "name": "gate"}}
		case "pending-consumed":
			frame["signal_cursor"] = 1
			frame["consumed_signals"] = []uint64{1}
			frame["pending_signals"] = []map[string]any{{"sequence": 1, "name": "gate"}}
		case "pending-name":
			frame["signal_cursor"] = 1
			frame["pending_signals"] = []map[string]any{{"sequence": 1, "name": "bad.name"}}
		case "pending-duplicate":
			frame["signal_cursor"] = 1
			frame["pending_signals"] = []map[string]any{{"sequence": 1, "name": "gate"}, {"sequence": 1, "name": "gate"}}
		case "timer-odd":
			frame["cancelled_timers"] = []uint64{1}
		case "timer-beyond-position":
			frame["cancelled_timers"] = []uint64{2}
		case "timer-duplicate":
			frame["cancelled_timers"] = []uint64{0, 0}
		case "panic-prefix-mismatch":
			frame["panic_attempts"] = 1
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
		if history {
			frame["state"] = map[string]any{"value": 42}
			switch control {
			case "history-frame-value":
				frame["state"] = map[string]any{"value": 43}
			case "history-frame-missing", "history-read-absent-valid", "history-write-error-valid":
				frame["state"] = map[string]any{}
			case "history-null-valid":
				frame["state"] = map[string]any{"value": nil}
			case "history-frame-extra":
				frame["state"] = map[string]any{"value": 42, "extra": 1}
			}
		}
		frameBytes, _ = json.Marshal(frame)
		hash := digest(frameBytes)
		entries[requestIndex].Payload = json.RawMessage(`{"kind":"checkpoint","name":"next","input_hash":"` + digest([]byte(`{"x":1}`)) + `"}`)
		entries[frameIndex].Payload = json.RawMessage(`{"result_ref":"step-result-` + hash + `","result_hash":"` + hash + `"}`)
		if len(control) >= 9 && control[:9] == "metadata-" || history {
			meta := map[string]any{"version": 1, "identity": map[string]any{"type": "kind", "id": "id", "inv_seq": 10}, "anchor": map[string]uint64{"index": frameIndex, "epoch": 1}, "frame_sha256": hash, "children": map[string]any{}, "signals": map[string]any{}, "signal_next": 0, "signal_last": 0}
			switch control {
			case "metadata-generation":
				meta["identity"] = map[string]any{"type": "kind", "id": "id", "inv_seq": 11}
			case "metadata-anchor":
				meta["anchor"] = map[string]uint64{"index": 3, "epoch": 1}
			case "metadata-frame":
				meta["frame_sha256"] = digest([]byte("other"))
			case "metadata-version":
				meta["version"] = 9
			case "metadata-child":
				meta["children"] = map[string]any{"child": map[string]any{"type": "child", "id": "id", "inv_seq": 0}}
			case "metadata-signal-prefix":
				meta["signal_next"], meta["signal_last"] = 1, 1
			case "metadata-buffered-child":
				meta["signals"] = map[string]any{"1": map[string]any{"sig_seq": 1, "name": "child"}}
			}
			metadataBytes, _ = json.Marshal(meta)
			metadataHash := digest(metadataBytes)
			completion := map[string]any{"result_ref": "step-result-" + hash, "result_hash": hash, "checkpoint_metadata_ref": "step-result-" + metadataHash, "checkpoint_metadata_hash": metadataHash}
			if control == "metadata-half-pointer" {
				delete(completion, "checkpoint_metadata_hash")
			}
			if control == "metadata-wrong-edge" {
				completion["checkpoint_metadata_ref"], completion["checkpoint_metadata_hash"] = "step-result-"+digest([]byte("wrong")), digest([]byte("wrong"))
			}
			entries[frameIndex].Payload, _ = json.Marshal(completion)
		}
		pointer.RequestIndex = requestIndex
		pointer.Runtime.InvSeq, pointer.Runtime.Stage, pointer.Runtime.Sequence, pointer.Runtime.Index, pointer.Runtime.Epoch, pointer.Runtime.StepPosition, pointer.Runtime.Object, pointer.Runtime.SHA256 = 10, "next", 101+frameIndex, frameIndex, 1, position, "step-result-"+hash, hash
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
	projections := map[string][]byte{"kind.id": entries[len(entries)-1].Payload}
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
		if archive && index == frameIndex && control != "frame-unowned" {
			blobs = append(blobs, put(frameBytes, graphpublication.Location{Kind: "payload", First: first, Stream: stream}))
		}
		if archive && index == frameIndex && len(metadataBytes) > 0 && control != "metadata-unowned" {
			blobs = append(blobs, put(metadataBytes, graphpublication.Location{Kind: "payload", First: first, Stream: stream}))
		}
		if archive && index == frameIndex {
			for _, data := range promiseBytes {
				blobs = append(blobs, put(data, graphpublication.Location{Kind: "payload", First: first, Stream: stream}))
			}
		}
		if history && index == 2 && len(stateResult) > 0 && control != "history-external-unowned" {
			blobs = append(blobs, put(stateResult, graphpublication.Location{Kind: "payload", First: first, Stream: stream}))
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
	cursor := auditedGraphCursor{Schema: "js-wf-graph-journal-cursor-v1", Invocation: 10, Base: 100, Count: uint64(len(entries)), Epoch: 1, Kind: journal.Completed}
	root := graphpublication.Root{Schema: graphpublication.ApplicationSchema, Head: 20, Token: "owner", Graph: retainedgraph.Root{Schema: retainedgraph.Schema, Count: 4, Frontier: []retainedgraph.Tree{tree(0, 0, 2, "")}}}
	source := &jetstream.RawStreamMsg{Sequence: 10, Subject: "wf.inv.kind.id", Data: []byte("input")}
	if archive {
		// Rebuild physical forests with their actual per-forest coordinates.
		objects = map[string][]byte{}
		graph.Fences = map[string]graphpublication.Fence{}
		cursor.Schema = "js-wf-graph-runtime-cursor-v6"
		cursor.RetainedFrom = requestIndex
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
		root.Graph = retainedgraph.Root{Schema: retainedgraph.Schema, Count: 3, Frontier: []retainedgraph.Tree{tree(requestIndex, 0, 1, ""), tree(requestIndex+2, 2, 0, "")}}
		root.Streams = []graphpublication.StreamGraph{{Name: "archive", Graph: retainedgraph.Root{Schema: retainedgraph.Schema, Count: 1, Frontier: []retainedgraph.Tree{tree(0, 0, 0, "archive")}}}, {Name: "input", Graph: retainedgraph.Root{Schema: retainedgraph.Schema, Count: 1, Frontier: []retainedgraph.Tree{{Link: put(inputNode, graphpublication.Location{Kind: "node", Stream: "input"})}}}}}
		if history {
			root.Streams[0].Graph = retainedgraph.Root{Schema: retainedgraph.Schema, Count: 3, Frontier: []retainedgraph.Tree{tree(0, 0, 1, "archive"), tree(2, 2, 0, "archive")}}
		}
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

func TestRawGraphCheckpointMaterializedState(t *testing.T) {
	for _, encoding := range []journal.Encoding{journal.JSON, journal.ProtobufV1} {
		t.Run(string(encoding)+"/valid", func(t *testing.T) {
			s, _ := rawJournalCheckpointFixture(t, encoding, true, nil, false, false, "materialized-valid")
			if _, err := CheckGraphJournals(context.Background(), s); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, control := range []string{"state-name", "promise-name", "promise-nonobject", "promise-unknown-field", "promise-hash-no-ref", "promise-ref-bad-hash", "promise-inline-and-ref", "consumed-zero", "consumed-duplicate", "consumed-unsorted", "pending-zero", "pending-beyond-cursor", "pending-consumed", "pending-name", "pending-duplicate", "timer-odd", "timer-beyond-position", "timer-duplicate", "panic-prefix-mismatch"} {
		t.Run(control, func(t *testing.T) {
			s, _ := rawJournalCheckpointFixture(t, journal.JSON, true, nil, false, false, control)
			if _, err := CheckGraphReferences(context.Background(), s.Graph); err != nil {
				t.Fatal("fixture reference corruption", err)
			}
			if _, err := CheckGraphJournals(context.Background(), s); err == nil {
				t.Fatal("corrupt materialized state accepted")
			}
		})
	}
}

func TestRawGraphCheckpointPromiseOwnership(t *testing.T) {
	for _, encoding := range []journal.Encoding{journal.JSON, journal.ProtobufV1} {
		t.Run(string(encoding)+"/valid", func(t *testing.T) {
			s, _ := rawJournalCheckpointFixture(t, encoding, true, nil, false, false, "promise-owned-valid")
			if _, err := CheckGraphJournals(context.Background(), s); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, control := range []string{"promise-owned-missing", "promise-owned-wrong", "promise-owned-conflict"} {
		t.Run(control, func(t *testing.T) {
			s, _ := rawJournalCheckpointFixture(t, journal.JSON, true, nil, false, false, control)
			if _, err := CheckGraphReferences(context.Background(), s.Graph); err != nil {
				t.Fatal("fixture reference corruption", err)
			}
			if _, err := CheckGraphJournals(context.Background(), s); err == nil {
				t.Fatal("unowned or conflicting promise accepted")
			}
		})
	}
}

func TestRawGraphCheckpointMetadataBindings(t *testing.T) {
	for _, encoding := range []journal.Encoding{journal.JSON, journal.ProtobufV1} {
		t.Run(string(encoding)+"/valid", func(t *testing.T) {
			s, _ := rawJournalCheckpointFixture(t, encoding, true, nil, false, false, "metadata-valid")
			if _, err := CheckGraphJournals(context.Background(), s); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, control := range []string{"metadata-generation", "metadata-anchor", "metadata-frame", "metadata-version", "metadata-child", "metadata-signal-prefix", "metadata-buffered-child", "metadata-half-pointer", "metadata-wrong-edge", "metadata-unowned"} {
		t.Run(control, func(t *testing.T) {
			s, _ := rawJournalCheckpointFixture(t, journal.JSON, true, nil, false, false, control)
			if _, err := CheckGraphReferences(context.Background(), s.Graph); err != nil {
				t.Fatal("fixture reference corruption", err)
			}
			if _, err := CheckGraphJournals(context.Background(), s); err == nil {
				t.Fatal("corrupt worker metadata accepted")
			}
		})
	}
}
