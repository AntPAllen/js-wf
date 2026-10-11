package integrity

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"js-wf/internal/blobpublication"
	"js-wf/internal/graphpublication"
	"js-wf/internal/retainedgraph"
	"js-wf/journal"
)

// Encode physical forests independently; every negative passes reference audit.
func rawSignalFixture(t *testing.T, enc journal.Encoding, control string) GraphJournalSnapshot {
	t.Helper()
	s, _ := rawJournalFixture(t, enc, true, nil, false, false)
	dest := graphDestinationForKey("kind.id")
	r := s.Graph.Roots[dest]
	var c auditedGraphCursor
	json.Unmarshal(r.Application, &c)
	c.SignalInputs, c.SignalBindings, c.SignalSource = 1, 1, 7
	objects := map[string][]byte{}
	oldLoad := s.Graph.LoadObject
	s.Graph.LoadObject = func(ctx context.Context, name string, limit int) ([]byte, error) {
		if b, ok := objects[name]; ok {
			return b, nil
		}
		return oldLoad(ctx, name, limit)
	}
	put := func(data []byte, stream, kind string, first uint64) retainedgraph.Link {
		hash := digest(data)
		owner := "signal-" + stream + "-" + kind
		name := hash + "/1/" + hex.EncodeToString([]byte(owner)) + "-" + hex.EncodeToString([]byte("attempt"))
		objects[name] = data
		s.Graph.Fences[digest([]byte("graph-authority/"+hash+"/"+owner))] = graphpublication.Fence{Hash: hash, Owner: owner, Generation: 1, Phase: "ready", Object: name, Intents: map[string]graphpublication.Intent{owner: {Destination: dest, Expected: 0, Expires: time.Unix(100, 0), Locations: []graphpublication.Location{{Kind: kind, Stream: stream, First: first}}}}}
		return retainedgraph.Link{Hash: hash, Reference: blobpublication.Reference{Generation: 1, Object: name}}
	}
	body := []byte(`42`)
	input := journal.GraphSignalInput{Schema: "js-wf-canonical-signal-input-v1", Request: journal.GraphSignalRequest{Type: "kind", ID: "id", Invocation: 10, Name: "gate", Key: "key"}, Index: 0, Token: "signal", InputSHA256: digest(body), InputSize: len(body)}
	switch control {
	case "generation":
		input.Request.Invocation++
	case "reservation-index":
		input.Index++
	case "name":
		input.Request.Name = "bad.name"
	case "key":
		input.Request.Key = ""
	case "size":
		input.InputSize++
	case "body":
		body = []byte(`43`)
	case "token":
		input.Token = "bad/token"
	}
	binding := journal.GraphSignalBinding{Schema: "js-wf-canonical-signal-binding-v1", Input: input, Index: 0, Sequence: 7}
	switch control {
	case "binding-input":
		binding.Input.Token = "foreign"
	case "binding-index":
		binding.Index++
	case "source-zero":
		binding.Sequence = 0
	case "source-frontier":
		binding.Sequence++
	case "consumption-census":
		c.SignalConsumed = 1
	}
	for _, stream := range []string{"signal-input", "signal-queue"} {
		var data []byte
		if stream == "signal-input" {
			data, _ = json.Marshal(map[string]any{"input": input, "index_packet": []byte("opaque-index")})
		} else {
			data, _ = json.Marshal(map[string]any{"binding": binding, "index_packet": []byte("opaque-index")})
		}
		blobs := []retainedgraph.Link{put(body, stream, "payload", 0)}
		if control == "missing-body" {
			blobs = nil
		}
		node, _ := json.Marshal(struct {
			Schema string
			First  uint64
			Height uint8
			Record *retainedgraph.Record
		}{retainedgraph.Schema, 0, 0, &retainedgraph.Record{Data: data, Blobs: blobs}})
		r.Streams = append(r.Streams, graphpublication.StreamGraph{Name: stream, Graph: retainedgraph.Root{Schema: retainedgraph.Schema, Count: 1, Frontier: []retainedgraph.Tree{{Link: put(node, stream, "node", 0)}}}})
	}
	if strings.HasPrefix(control, "consumed-") {
		event := auditedCheckpointSignal{Sequence: 7, Name: "gate", Ref: "graph-signal-" + input.InputSHA256, Hash: input.InputSHA256}
		event.Canonical = &struct {
			Index uint64 `json:"index"`
			Token string `json:"token"`
		}{0, input.Token}
		switch control {
		case "consumed-index":
			event.Canonical.Index++
		case "consumed-token":
			event.Canonical.Token = "foreign"
		case "consumed-name":
			event.Name = "other"
		case "consumed-sequence":
			event.Sequence++
		case "consumed-ref":
			event.Ref = "foreign"
		case "consumed-hash":
			event.Hash = digest([]byte("other"))
			event.Ref = "graph-signal-" + event.Hash
		case "consumed-no-marker":
			event.Canonical = nil
		}
		payload, _ := json.Marshal(event)
		entry, err := journal.MarshalEntry(journal.Entry{Index: 3, Epoch: 1, WorkerID: "worker", Kind: journal.SignalConsumed, Payload: payload}, enc)
		if err != nil {
			t.Fatal(err)
		}
		blobs := []retainedgraph.Link{put(entry, "", "payload", 2)}
		if control != "consumed-unowned" {
			blobs = append(blobs, put(body, "", "payload", 2))
		}
		envelope, _ := json.Marshal(map[string]any{"schema": "js-wf-graph-journal-entry-v1", "invocation": 10, "sequence": 104, "entry_sha256": digest(entry)})
		node, _ := json.Marshal(struct {
			Schema string
			First  uint64
			Height uint8
			Record *retainedgraph.Record
		}{retainedgraph.Schema, 2, 0, &retainedgraph.Record{Data: envelope, Blobs: blobs}})
		r.Graph.Frontier[1].Link = put(node, "", "node", 2)
		c.Kind, c.SignalConsumed = journal.SignalConsumed, 1
	}
	r.Application, _ = json.Marshal(c)
	s.Graph.Roots[dest] = r
	return s
}

func TestRawGraphSignalReservationAndBinding(t *testing.T) {
	for _, enc := range []journal.Encoding{journal.JSON, journal.ProtobufV1} {
		t.Run(fmt.Sprint(enc), func(t *testing.T) {
			if _, err := CheckGraphJournals(context.Background(), rawSignalFixture(t, enc, "")); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, enc := range []journal.Encoding{journal.JSON, journal.ProtobufV1} {
		t.Run(fmt.Sprint(enc)+"/consumed-valid", func(t *testing.T) {
			if _, err := CheckGraphJournals(context.Background(), rawSignalFixture(t, enc, "consumed-valid")); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, control := range []string{"generation", "reservation-index", "name", "key", "size", "body", "token", "binding-input", "binding-index", "source-zero", "source-frontier", "consumption-census", "missing-body", "consumed-index", "consumed-token", "consumed-name", "consumed-sequence", "consumed-ref", "consumed-hash", "consumed-no-marker", "consumed-unowned"} {
		t.Run(control, func(t *testing.T) {
			s := rawSignalFixture(t, journal.JSON, control)
			if _, err := CheckGraphReferences(context.Background(), s.Graph); err != nil {
				t.Fatal("control is not reference-valid", err)
			}
			if _, err := CheckGraphJournals(context.Background(), s); err == nil {
				t.Fatal("corrupt signals accepted")
			}
		})
	}
}
