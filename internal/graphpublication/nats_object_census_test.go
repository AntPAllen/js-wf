package graphpublication

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/internal/retainedgraph"
)

type graphObjectCensusStream struct {
	jetstream.Stream
	mode                  string
	infoCalls, chunkCalls int
}

func (s *graphObjectCensusStream) Info(ctx context.Context, opts ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
	s.infoCalls++
	if s.mode == "unknown_info" {
		return nil, errors.New("unknown current object configuration/census")
	}
	info, err := s.Stream.Info(ctx, opts...)
	if err != nil {
		return nil, err
	}
	copy := *info
	switch s.mode {
	case "no_rollup":
		copy.Config.AllowRollup = false
	case "allow_direct":
		copy.Config.AllowDirect = true
	case "sealed":
		copy.Config.Sealed = true
	case "wrong_format":
		copy.Config.Metadata = map[string]string{"js-wf-blob-format": "foreign"}
	case "foreign_subject", "missing_chunks":
		copy.State.Subjects = make(map[string]uint64, len(info.State.Subjects))
		for name, count := range info.State.Subjects {
			copy.State.Subjects[name] = count
		}
		if s.mode == "foreign_subject" {
			copy.State.Subjects["$O.GRAPH_OBJECTS.C.foreign"] = 1
		} else {
			for name := range copy.State.Subjects {
				delete(copy.State.Subjects, name)
			}
		}
	}
	return &copy, nil
}

func (s *graphObjectCensusStream) GetMsg(ctx context.Context, seq uint64, opts ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
	s.chunkCalls++
	return s.Stream.GetMsg(ctx, seq, opts...)
}

func TestNativeGraphObjectGetFreshCombinedCensus(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			port, ctx := nativeGraphObjectFixture(t, replicas)
			data := bytes.Repeat([]byte("chunk"), nativeChunkSize/5+10)
			_, name := nativeGraphPendingObject(t, port, ctx, data)
			if err := port.Put(ctx, name, data); err != nil {
				t.Fatal(err)
			}
			object, _ := parsePhysicalObject(name)
			link := retainedgraph.Link{Hash: object.Key, Reference: object.Reference}
			for _, mode := range []string{"healthy", "no_rollup", "allow_direct", "sealed", "wrong_format", "foreign_subject", "missing_chunks", "unknown_info"} {
				t.Run(mode, func(t *testing.T) {
					stream := &graphObjectCensusStream{Stream: port.objectStream}
					wrapped := *port
					wrapped.objectStream = stream
					got, err := wrapped.Get(ctx, link, len(data))
					if err != nil || !bytes.Equal(got, data) || stream.infoCalls != 1 || stream.chunkCalls != 2 {
						t.Fatalf("healthy read: info=%d chunks=%d bytes=%d err=%v", stream.infoCalls, stream.chunkCalls, len(got), err)
					}
					// Admission is fresh for every read, including a read after this
					// same port has successfully consumed a valid configuration.
					stream.mode = mode
					got, err = wrapped.Get(ctx, link, len(data))
					if stream.infoCalls != 2 {
						t.Fatal("configuration/census was cached or separately fetched", stream.infoCalls)
					}
					if mode == "healthy" {
						if err != nil || !bytes.Equal(got, data) || stream.chunkCalls != 4 {
							t.Fatal("second healthy read", got, err, stream.chunkCalls)
						}
					} else if err == nil || got != nil || stream.chunkCalls != 2 {
						t.Fatalf("unsafe/unknown read reached chunks: mode=%s bytes=%d err=%v chunks=%d", mode, len(got), err, stream.chunkCalls)
					}
				})
			}
		})
	}
}
