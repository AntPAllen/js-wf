package graphpublication

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"js-wf/internal/blobpublication"
	"js-wf/internal/retainedgraph"
)

// Measure structural authority validation only; this fixture supplies no stored
// nodes and makes no payload-ownership or native transport conformance claim.
func BenchmarkGraphAuthorityNormalization(b *testing.B) {
	graph := retainedgraph.Empty()
	hash := strings.Repeat("1", 64)
	var first uint64
	for height := 12; height >= 0; height-- {
		graph.Frontier = append(graph.Frontier, retainedgraph.Tree{First: first, Height: uint8(height), Link: retainedgraph.Link{Hash: hash, Reference: blobpublication.Reference{Generation: 1, Object: hash + "/1/6f776e6572-6e6f6465"}}})
		first += uint64(1) << height
	}
	graph.Count = first
	for _, readers := range []int{0, MaxReaders} {
		b.Run(fmt.Sprintf("readers-%d", readers), func(b *testing.B) {
			root := Root{Schema: StreamsSchema, Head: 1, Token: "owner", Graph: graph}
			for _, name := range []string{"input", "queue", "results", "signals"} {
				root.Streams = append(root.Streams, StreamGraph{Name: name, Graph: graph})
			}
			for i := 0; i < readers; i++ {
				root.Readers = append(root.Readers, ReaderPin{ID: fmt.Sprintf("reader-%d", i), Expires: time.Unix(1000, 0), Graph: graph, Streams: copyStreams(root.Streams)})
			}
			if _, e := normalizeRoot(root); e != nil {
				b.Fatal(e)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, e := normalizeRoot(root); e != nil {
					b.Fatal(e)
				}
			}
		})
	}
}
