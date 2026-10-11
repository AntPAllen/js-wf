package integrity

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"js-wf/internal/blobpublication"
	"js-wf/internal/graphpublication"
	"js-wf/internal/retainedgraph"
)

func graphAuditFixture(t *testing.T) (GraphReferenceSnapshot, map[string][]byte) {
	t.Helper()
	objects := map[string][]byte{}
	s := GraphReferenceSnapshot{Roots: map[string]graphpublication.Root{}, Fences: map[string]graphpublication.Fence{}, PayloadLimit: 1024}
	s.LoadObject = func(ctx context.Context, name string, limit int) ([]byte, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		data, ok := objects[name]
		if !ok {
			return nil, fmt.Errorf("missing object")
		}
		return append([]byte(nil), data...), nil
	}
	put := func(data []byte, location graphpublication.Location) retainedgraph.Link {
		hash := digest(data)
		owner := "owner"
		object := hash + "/1/" + hex.EncodeToString([]byte(owner)) + "-" + hex.EncodeToString([]byte("attempt"))
		link := retainedgraph.Link{Hash: hash, Reference: blobpublication.Reference{Generation: 1, Object: object}}
		objects[object] = data
		scope := digest([]byte("graph-authority/" + hash + "/" + owner))
		f := s.Fences[scope]
		if f.Intents == nil {
			f = graphpublication.Fence{Hash: hash, Owner: owner, Generation: 1, Phase: "ready", Object: object, Intents: map[string]graphpublication.Intent{}}
		}
		intent := f.Intents[owner]
		intent.Destination = "journal"
		intent.Expected = 0
		intent.Expires = time.Unix(100, 0)
		intent.Locations = append(intent.Locations, location)
		f.Intents[owner] = intent
		s.Fences[scope] = f
		return link
	}
	payload := put([]byte("payload"), graphpublication.Location{Kind: "payload", First: 0})
	leaf := func(first uint64, stream string) retainedgraph.Link {
		data, _ := json.Marshal(struct {
			Schema string
			First  uint64
			Height uint8
			Record *retainedgraph.Record
		}{retainedgraph.Schema, first, 0, &retainedgraph.Record{Data: []byte("record"), Blobs: []retainedgraph.Link{payload}}})
		return put(data, graphpublication.Location{Kind: "node", First: first, Stream: stream})
	}
	l0, l1, l2 := leaf(0, ""), leaf(1, ""), leaf(2, "")
	data, _ := json.Marshal(struct {
		Schema   string
		First    uint64
		Height   uint8
		Children []retainedgraph.Link
	}{retainedgraph.Schema, 0, 1, []retainedgraph.Link{l0, l1}})
	branch := put(data, graphpublication.Location{Kind: "node", First: 0, Height: 1})
	signalLeaf := leaf(0, "signals")
	// The same immutable payload has original edges in both indexed forests.
	put([]byte("payload"), graphpublication.Location{Kind: "payload", First: 0, Stream: "signals"})
	s.Roots["journal"] = graphpublication.Root{Schema: graphpublication.StreamsSchema, Head: 4, Token: "owner", Graph: retainedgraph.Root{Schema: retainedgraph.Schema, Count: 3, Frontier: []retainedgraph.Tree{{First: 0, Height: 1, Link: branch}, {First: 2, Link: l2}}}, Streams: []graphpublication.StreamGraph{{Name: "signals", Graph: retainedgraph.Root{Schema: retainedgraph.Schema, Count: 1, Frontier: []retainedgraph.Tree{{Link: signalLeaf}}}}}, Readers: []graphpublication.ReaderPin{{ID: "reader", Expires: time.Unix(100, 0), Graph: retainedgraph.Root{Schema: retainedgraph.Schema, Count: 1, Frontier: []retainedgraph.Tree{{Link: l0}}}}}}
	return s, objects
}

func TestRawGraphReferencesAndIndependentCorruptionControls(t *testing.T) {
	s, objects := graphAuditFixture(t)
	r, err := CheckGraphReferences(context.Background(), s)
	if err != nil || r != (GraphReferenceReport{Roots: 1, Forests: 3, ReaderPins: 1, Records: 5, Nodes: 6, PayloadEdges: 5}) {
		t.Fatal(r, err)
	}
	tests := []struct {
		name, want string
		mutate     func(*GraphReferenceSnapshot, map[string][]byte)
	}{
		{"missing-object", "read graph object", func(s *GraphReferenceSnapshot, o map[string][]byte) {
			for k := range o {
				delete(o, k)
				break
			}
		}},
		{"changed-bytes", "dangling graph receipt", func(s *GraphReferenceSnapshot, o map[string][]byte) {
			for k := range o {
				o[k] = []byte("corrupt")
				break
			}
		}},
		{"closed-ready-grant", "missing graph ownership", func(s *GraphReferenceSnapshot, o map[string][]byte) {
			for k, f := range s.Fences {
				f.Phase = "closed"
				f.Object = ""
				f.Intents = nil
				s.Fences[k] = f
				break
			}
		}},
		{"wrong-generation", "invalid ready physical identity", func(s *GraphReferenceSnapshot, o map[string][]byte) {
			for k, f := range s.Fences {
				f.Generation++
				s.Fences[k] = f
				break
			}
		}},
		{"wrong-destination", "missing graph ownership", func(s *GraphReferenceSnapshot, o map[string][]byte) {
			for k, f := range s.Fences {
				i := f.Intents[f.Owner]
				i.Destination = "another"
				f.Intents[f.Owner] = i
				s.Fences[k] = f
				break
			}
		}},
		{"future-grant", "missing graph ownership", func(s *GraphReferenceSnapshot, o map[string][]byte) {
			for k, f := range s.Fences {
				i := f.Intents[f.Owner]
				i.Expected = 4
				f.Intents[f.Owner] = i
				s.Fences[k] = f
				break
			}
		}},
		{"wrong-coordinate", "node lacks exact coordinate grant", func(s *GraphReferenceSnapshot, o map[string][]byte) {
			for k, f := range s.Fences {
				i := f.Intents[f.Owner]
				if i.Locations[0].Kind == "node" {
					for n := range i.Locations {
						i.Locations[n].First = 9
					}
					f.Intents[f.Owner] = i
					s.Fences[k] = f
					return
				}
			}
		}},
		{"lost-payload-origin", "reused payload lost its canonical origin edge", func(s *GraphReferenceSnapshot, o map[string][]byte) {
			for k, f := range s.Fences {
				i := f.Intents[f.Owner]
				if i.Locations[0].Kind == "payload" {
					for n := range i.Locations {
						i.Locations[n].First = 99
					}
					f.Intents[f.Owner] = i
					s.Fences[k] = f
					return
				}
			}
		}},
		{"duplicate-reader", "invalid reader pin", func(s *GraphReferenceSnapshot, o map[string][]byte) {
			root := s.Roots["journal"]
			root.Readers = append(root.Readers, root.Readers[0])
			s.Roots["journal"] = root
		}},
		{"wrong-count", "invalid graph authority", func(s *GraphReferenceSnapshot, o map[string][]byte) {
			root := s.Roots["journal"]
			root.Graph.Count++
			s.Roots["journal"] = root
		}},
		{"duplicate-stream", "invalid graph stream", func(s *GraphReferenceSnapshot, o map[string][]byte) {
			root := s.Roots["journal"]
			root.Streams = append(root.Streams, root.Streams[0])
			s.Roots["journal"] = root
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s, o := graphAuditFixture(t)
			test.mutate(&s, o)
			_, err := CheckGraphReferences(context.Background(), s)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("corruption accepted or wrong failure: %v", err)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := CheckGraphReferences(ctx, s); !errors.Is(err, context.Canceled) {
		t.Fatal("audit ignored cancellation", err)
	}
	s.PayloadLimit = 2
	if _, err := CheckGraphReferences(context.Background(), s); err == nil {
		t.Fatal("oversized payload accepted")
	}
	_ = objects
}
