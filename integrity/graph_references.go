// Package integrity checks persisted state independently of runtime decisions.
package integrity

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"js-wf/internal/graphpublication"
	"js-wf/internal/retainedgraph"
)

// GraphReferenceSnapshot contains raw persisted authority and a bounded physical
// object reader. The caller must hold the namespace quiescent for the entire
// audit. Application cursor semantics and runtime invariants are separate.
type GraphReferenceSnapshot struct {
	Roots        map[string]graphpublication.Root
	Fences       map[string]graphpublication.Fence
	LoadObject   func(context.Context, string, int) ([]byte, error)
	PayloadLimit int
	// VisitRecord runs after validating each raw leaf and its payload grants.
	// ReaderID is empty for the current live forest. Archive precedes the live
	// journal, so callers can reduce logical journal order without prefix buffers.
	VisitRecord func(context.Context, GraphAuditRecord) error
}

type GraphAuditRecord struct {
	Destination, Stream, ReaderID string
	Index                         uint64
	Record                        retainedgraph.Record
}

type GraphReferenceReport struct {
	Roots, Forests, ReaderPins, Records, Nodes, PayloadEdges int
}

// CheckGraphReferences walks encoded nodes and exact original ownership grants.
// It does not call the production graph walk, membership or journal readers.
// Counts include retained reader snapshots, so shared records may be visited
// more than once. Extra abandoned objects/scopes are allowed, not reclaimed.
func CheckGraphReferences(ctx context.Context, snapshot GraphReferenceSnapshot) (report GraphReferenceReport, err error) {
	if snapshot.LoadObject == nil || snapshot.PayloadLimit <= 0 {
		return report, fmt.Errorf("invalid graph audit reader/bounds")
	}
	err = checkGraphReferences(ctx, snapshot, &report)
	return report, err
}

func digest(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }

func checkGraphReferences(ctx context.Context, snapshot GraphReferenceSnapshot, report *GraphReferenceReport) error {
	for scope, fence := range snapshot.Fences {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := graphAuditFence(scope, fence); err != nil {
			return err
		}
	}
	destinations := make([]string, 0, len(snapshot.Roots))
	for destination := range snapshot.Roots {
		destinations = append(destinations, destination)
	}
	sort.Strings(destinations)
	for _, destination := range destinations {
		root := snapshot.Roots[destination]
		if destination == "" || len(destination) > 256 || !utf8.ValidString(destination) {
			return fmt.Errorf("invalid graph destination")
		}
		report.Roots++
		if graphSchemaRank(root.Schema) == 0 || (root.Schema == graphpublication.Schema && len(root.Readers) != 0) || len(root.Readers) > graphpublication.MaxReaders || len(root.Application) > graphpublication.MaxApplicationBytes || (graphSchemaRank(root.Schema) >= 3 && root.Head == 0) || (graphSchemaRank(root.Schema) < 3 && len(root.Application) != 0) || !graphAuditRoot(root.Graph) {
			return fmt.Errorf("invalid graph authority")
		}
		if root.Graph.Count > 0 && root.Token == "" {
			return fmt.Errorf("missing live publication token")
		}
		for _, stream := range root.Streams {
			if stream.Graph.Count > 0 && root.Token == "" {
				return fmt.Errorf("missing live stream token")
			}
		}
		if err := checkGraphStreams(root.Streams, root.Schema); err != nil {
			return err
		}
		type auditForest struct {
			graphpublication.StreamGraph
			reader string
		}
		graphs := []auditForest{{StreamGraph: graphpublication.StreamGraph{Graph: root.Graph}}}
		for _, stream := range root.Streams {
			graphs = append(graphs, auditForest{StreamGraph: stream})
		}
		seenReaders := map[string]bool{}
		for _, pin := range root.Readers {
			report.ReaderPins++
			if pin.ID == "" || pin.Expires.IsZero() || seenReaders[pin.ID] || !graphAuditRoot(pin.Graph) {
				return fmt.Errorf("invalid reader pin")
			}
			seenReaders[pin.ID] = true
			if err := checkGraphStreams(pin.Streams, root.Schema); err != nil {
				return err
			}
			graphs = append(graphs, auditForest{StreamGraph: graphpublication.StreamGraph{Graph: pin.Graph}, reader: pin.ID})
			for _, stream := range pin.Streams {
				graphs = append(graphs, auditForest{StreamGraph: stream, reader: pin.ID})
			}
		}
		sort.SliceStable(graphs, func(i, j int) bool {
			if graphs[i].reader != graphs[j].reader {
				return graphs[i].reader < graphs[j].reader
			}
			return graphs[i].Name == "archive" && graphs[j].Name != "archive"
		})
		for _, forest := range graphs {
			report.Forests++
			graph := forest.Graph
			leaves := uint64(0)
			edges := map[uint64]map[retainedgraph.Link]bool{}
			origins := map[retainedgraph.Link][]graphpublication.Location{}
			check := func(link retainedgraph.Link, location graphpublication.Location) ([]byte, error) {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				limit := snapshot.PayloadLimit
				if location.Kind == "node" {
					limit = retainedgraph.MaxNodeBytes
				}
				data, readErr := snapshot.LoadObject(ctx, link.Reference.Object, limit)
				if readErr != nil {
					return nil, fmt.Errorf("read graph object %s: %w", link.Reference.Object, readErr)
				}
				if len(data) > limit || digest(data) != link.Hash || !graphAuditLink(link) {
					return nil, fmt.Errorf("dangling graph receipt")
				}
				parts := strings.Split(link.Reference.Object, "/")
				ids := strings.Split(parts[2], "-")
				if len(ids) != 2 {
					return nil, fmt.Errorf("invalid graph physical scope")
				}
				ownerBytes, err := hex.DecodeString(ids[0])
				if err != nil {
					return nil, err
				}
				owner := string(ownerBytes)
				scope := digest([]byte("graph-authority/" + link.Hash + "/" + owner))
				f := snapshot.Fences[scope]
				intent, ok := f.Intents[owner]
				if f.Hash != link.Hash || f.Owner != owner || f.Phase != "ready" || f.Generation != link.Reference.Generation || f.Object != link.Reference.Object || !ok || intent.Destination != destination || intent.Expected >= root.Head {
					return nil, fmt.Errorf("missing graph ownership")
				}
				// Reused payload edges retain their original index; append preserves it.
				if location.Kind == "node" {
					found := false
					for _, registered := range intent.Locations {
						if registered == location {
							found = true
						}
					}
					if !found {
						return nil, fmt.Errorf("node lacks exact coordinate grant")
					}
				} else {
					origins[link] = append([]graphpublication.Location{}, intent.Locations...)
				}
				return data, nil
			}
			var visit func(retainedgraph.Tree) error
			visit = func(tree retainedgraph.Tree) error {
				report.Nodes++
				data, err := check(tree.Link, graphpublication.Location{Kind: "node", First: tree.First, Height: tree.Height, Stream: forest.Name})
				if err != nil {
					return err
				}
				var node struct {
					Schema   string
					First    uint64
					Height   uint8
					Children []retainedgraph.Link
					Record   *retainedgraph.Record
				}
				if err = json.Unmarshal(data, &node); err != nil {
					return err
				}
				if node.Schema != retainedgraph.Schema || node.First != tree.First || node.Height != tree.Height {
					return fmt.Errorf("graph position changed")
				}
				if node.Height == 0 {
					if node.Record == nil || len(node.Children) != 0 || len(node.Record.Data) > retainedgraph.MaxDataBytes || len(node.Record.Blobs) > retainedgraph.MaxBlobReferences {
						return fmt.Errorf("invalid graph leaf")
					}
					for _, link := range node.Record.Blobs {
						report.PayloadEdges++
						if _, err = check(link, graphpublication.Location{Kind: "payload", First: node.First, Stream: forest.Name}); err != nil {
							return err
						}
						if edges[node.First] == nil {
							edges[node.First] = map[retainedgraph.Link]bool{}
						}
						edges[node.First][link] = true
					}
					if snapshot.VisitRecord != nil {
						if err := snapshot.VisitRecord(ctx, GraphAuditRecord{Destination: destination, Stream: forest.Name, ReaderID: forest.reader, Index: node.First, Record: *node.Record}); err != nil {
							return err
						}
					}
					report.Records++
					leaves++
					return nil
				}
				if node.Record != nil || len(node.Children) != 2 {
					return fmt.Errorf("invalid graph branch")
				}
				if err = visit(retainedgraph.Tree{First: tree.First, Height: tree.Height - 1, Link: node.Children[0]}); err != nil {
					return err
				}
				return visit(retainedgraph.Tree{First: tree.First + (uint64(1) << (tree.Height - 1)), Height: tree.Height - 1, Link: node.Children[1]})
			}
			for _, tree := range graph.Frontier {
				if err := visit(tree); err != nil {
					return err
				}
			}
			if leaves != graph.Count {
				return fmt.Errorf("graph census lost population")
			}
			for link, locations := range origins {
				found := false
				for _, location := range locations {
					if location.Kind == "payload" && location.Stream == forest.Name && edges[location.First][link] {
						found = true
					}
				}
				if !found {
					return fmt.Errorf("reused payload lost its canonical origin edge")
				}
			}
		}
	}
	return ctx.Err()
}

func graphAuditHash(value string) bool {
	b, err := hex.DecodeString(value)
	return err == nil && len(b) == 32 && hex.EncodeToString(b) == value
}

func graphAuditID(value string) bool {
	if len(value) == 0 || len(value) > 32 {
		return false
	}
	for _, c := range value {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-') {
			return false
		}
	}
	return true
}

func graphAuditLink(link retainedgraph.Link) bool {
	if !graphAuditHash(link.Hash) || link.Reference.Generation == 0 || len(link.Reference.Object) > 256 {
		return false
	}
	prefix := link.Hash + "/" + strconv.FormatUint(link.Reference.Generation, 10) + "/"
	if !strings.HasPrefix(link.Reference.Object, prefix) {
		return false
	}
	parts := strings.Split(strings.TrimPrefix(link.Reference.Object, prefix), "-")
	if len(parts) != 2 {
		return false
	}
	for _, part := range parts {
		value, err := hex.DecodeString(part)
		if err != nil || hex.EncodeToString(value) != part || !graphAuditID(string(value)) {
			return false
		}
	}
	return true
}

// Independent frontier validation keeps the audit independent of the producer's
// retainedgraph.Root.Validate implementation as well as its traversal.
func graphAuditRoot(root retainedgraph.Root) bool {
	if root.Schema != retainedgraph.Schema || root.Count > math.MaxInt64 || root.Frontier == nil || len(root.Frontier) > 63 {
		return false
	}
	next := uint64(0)
	previous := uint8(63)
	for _, tree := range root.Frontier {
		if tree.Height >= previous || tree.Height > 62 || tree.First != next || !graphAuditLink(tree.Link) {
			return false
		}
		width := uint64(1) << tree.Height
		if next > root.Count || width > root.Count-next {
			return false
		}
		next += width
		previous = tree.Height
	}
	return next == root.Count
}

func graphAuditFence(scope string, f graphpublication.Fence) error {
	if !graphAuditHash(f.Hash) || !graphAuditID(f.Owner) || scope != digest([]byte("graph-authority/"+f.Hash+"/"+f.Owner)) || f.Generation == 0 {
		return fmt.Errorf("invalid graph ownership scope/generation")
	}
	switch f.Phase {
	case "closed":
		if f.Object != "" || len(f.Intents) != 0 {
			return fmt.Errorf("invalid closed graph fence")
		}
		return nil
	case "uploading":
		if f.Object != "" {
			return fmt.Errorf("invalid uploading graph fence")
		}
	case "ready":
		prefix := f.Hash + "/" + strconv.FormatUint(f.Generation, 10) + "/"
		if !strings.HasPrefix(f.Object, prefix) || len(f.Object) > 256 {
			return fmt.Errorf("invalid ready physical identity")
		}
		parts := strings.Split(strings.TrimPrefix(f.Object, prefix), "-")
		if len(parts) != 2 {
			return fmt.Errorf("invalid ready physical attempt")
		}
		owner, e1 := hex.DecodeString(parts[0])
		upload, e2 := hex.DecodeString(parts[1])
		if e1 != nil || e2 != nil || string(owner) != f.Owner || !graphAuditID(string(upload)) || hex.EncodeToString(owner) != parts[0] || hex.EncodeToString(upload) != parts[1] {
			return fmt.Errorf("invalid ready physical owner/upload")
		}
	default:
		return fmt.Errorf("invalid graph fence phase")
	}
	intent, ok := f.Intents[f.Owner]
	if !ok || len(f.Intents) != 1 || intent.Destination == "" || len(intent.Destination) > 256 || !utf8.ValidString(intent.Destination) || intent.Expected == math.MaxUint64 || intent.Expires.IsZero() || len(intent.Locations) == 0 || len(intent.Locations) > graphpublication.MaxIntentLocations {
		return fmt.Errorf("invalid graph grant")
	}
	for _, location := range intent.Locations {
		if location.Kind != "node" && location.Kind != "payload" || location.Height > 62 || location.Kind == "payload" && location.Height != 0 {
			return fmt.Errorf("invalid graph grant location")
		}
	}
	return nil
}

func graphSchemaRank(schema string) int {
	switch schema {
	case graphpublication.Schema:
		return 1
	case graphpublication.RetentionSchema:
		return 2
	case graphpublication.ApplicationSchema:
		return 3
	case graphpublication.StreamsSchema:
		return 4
	}
	return 0
}
func checkGraphStreams(streams []graphpublication.StreamGraph, schema string) error {
	if len(streams) > graphpublication.MaxStreams || len(streams) != 0 && schema != graphpublication.StreamsSchema {
		return fmt.Errorf("invalid graph streams")
	}
	for i, stream := range streams {
		if stream.Name == "" || len(stream.Name) > 32 || i > 0 && streams[i-1].Name >= stream.Name || !graphAuditRoot(stream.Graph) {
			return fmt.Errorf("invalid graph stream name/order/snapshot")
		}
		for _, c := range stream.Name {
			if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
				return fmt.Errorf("invalid graph stream name")
			}
		}
	}
	return nil
}
