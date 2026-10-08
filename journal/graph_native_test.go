package journal

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/internal/graphpublication"
	"js-wf/testcluster"
)

func TestNativeGraphJournalGenerationAndDrain(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			cluster, err := testcluster.Start(t.TempDir(), replicas)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(cluster.Close)
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			if replicas > 1 {
				for {
					ready := false
					for _, server := range cluster.Servers {
						ready = ready || (server.JetStreamIsLeader() && len(server.JetStreamClusterPeers()) == replicas)
					}
					if ready {
						break
					}
					select {
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					case <-time.After(20 * time.Millisecond):
					}
				}
			}
			js, err := jetstream.New(cluster.Clients[0])
			if err != nil {
				t.Fatal(err)
			}
			if _, err = js.CreateStream(ctx, graphpublication.AuthorityStreamConfig("GRAPH_JOURNAL_AUTH", "wf.graph.journal", replicas)); err != nil {
				t.Fatal(err)
			}
			if _, err = js.CreateStream(ctx, graphpublication.NativeObjectStreamConfig("GRAPH_JOURNAL_OBJECTS", replicas)); err != nil {
				t.Fatal(err)
			}
			authority, err := graphpublication.OpenNativeAuthority(ctx, js, "GRAPH_JOURNAL_AUTH", "wf.graph.journal")
			if err != nil {
				t.Fatal(err)
			}
			port, err := graphpublication.OpenNativePort(ctx, authority, "GRAPH_JOURNAL_OBJECTS")
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC()
			p := graphpublication.Protocol{Port: port}
			cfg := GraphConfig{Protocol: p, Now: func() time.Time { return now }, PinTTL: time.Hour, IntentTTL: time.Minute}
			s, err := NewGraphStore(cfg)
			if err != nil {
				t.Fatal(err)
			}
			tail, err := s.Begin(ctx, "flow", "native", 42)
			if err != nil {
				t.Fatal(err)
			}
			tail, err = s.Append(ctx, "flow", "native", 42, Entry{Kind: Started, Epoch: 7}, tail, [][]byte{[]byte("native-input")}, nil)
			if err != nil {
				t.Fatal(err)
			}
			view, err := s.Open(ctx, "flow", "native", 42)
			if err != nil {
				t.Fatal(err)
			}
			first, err := view.Read(ctx, 0)
			if err != nil || len(first.Blobs) != 1 {
				t.Fatal(first, err)
			}
			large := []byte(`"` + string(bytes.Repeat([]byte("x"), 100000)) + `"`)
			for i := uint64(1); i < 65; i++ {
				kind := StepCompleted
				if i == 64 {
					kind = Completed
				}
				tail, err = s.Append(ctx, "flow", "native", 42, Entry{Kind: kind, Index: i, Epoch: 7 + i, Payload: large}, tail, nil, []graphpublication.OwnedPayload{{Index: 0, Link: first.Blobs[0]}})
				if err != nil {
					t.Fatal(i, err)
				}
			}
			// New store and reopened native adapters recover only from durable state.
			freshAuthority, err := graphpublication.OpenNativeAuthority(ctx, js, "GRAPH_JOURNAL_AUTH", "wf.graph.journal")
			if err != nil {
				t.Fatal(err)
			}
			freshPort, err := graphpublication.OpenNativePort(ctx, freshAuthority, "GRAPH_JOURNAL_OBJECTS")
			if err != nil {
				t.Fatal(err)
			}
			cfg.Protocol = graphpublication.Protocol{Port: freshPort}
			s, err = NewGraphStore(cfg)
			if err != nil {
				t.Fatal(err)
			}
			records, readTail, err := s.Read(ctx, "flow", "native", 42)
			if err != nil || len(records) != 65 || readTail != 65 || !bytes.Equal(records[64].Payload, large) {
				t.Fatal(len(records), readTail, err)
			}
			if err = s.Retire(ctx, "flow", "native", 42, tail); err != nil {
				t.Fatal(err)
			}
			tail, err = s.Begin(ctx, "flow", "native", 43)
			if err != nil || tail != 65 {
				t.Fatal(tail, err)
			}
			now = now.Add(2 * time.Minute)
			if _, err = p.SweepWithReaders(ctx, now); err != nil {
				t.Fatal(err)
			}
			data, err := view.Payload(ctx, 0, first.Blobs[0], 100)
			if err != nil || string(data) != "native-input" {
				t.Fatal(string(data), err)
			}
			if err = view.Close(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err = p.SweepWithReaders(ctx, now); err != nil {
				t.Fatal(err)
			}
			objects, err := port.Objects(ctx)
			if err != nil || len(objects) != 0 {
				t.Fatal(len(objects), err)
			}
			stream, err := js.Stream(ctx, "OBJ_GRAPH_JOURNAL_OBJECTS")
			if err != nil {
				t.Fatal(err)
			}
			info, err := stream.Info(ctx, jetstream.WithSubjectFilter("$O.GRAPH_JOURNAL_OBJECTS.>"))
			if err != nil {
				t.Fatal(err)
			}
			for subject := range info.State.Subjects {
				if strings.Contains(subject, ".C.") {
					t.Fatal("chunk subject remains", subject)
				}
			}
			tail, err = s.Append(ctx, "flow", "native", 43, Entry{Kind: Started}, tail, nil, nil)
			if err != nil || tail != 66 {
				t.Fatal(tail, err)
			}
			records, readTail, err = s.Read(ctx, "flow", "native", 43)
			if err != nil || len(records) != 1 || records[0].Index != 0 || readTail != 66 {
				t.Fatal(records, readTail, err)
			}
		})
	}
}
