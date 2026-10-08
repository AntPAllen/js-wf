package retainedindex_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/internal/graphpublication"
	"js-wf/internal/retainedindex"
	"js-wf/testcluster"
)

func TestNativeOwnedIndexReopenedSnapshotAndPhysicalDrain(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			cluster, err := testcluster.Start(t.TempDir(), replicas)
			if err != nil {
				t.Fatal(err)
			}
			defer cluster.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if replicas > 1 {
				for {
					ready := false
					for _, server := range cluster.Servers {
						ready = ready || server.JetStreamIsLeader() && len(server.JetStreamClusterPeers()) == replicas
					}
					if ready {
						break
					}
					select {
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					case <-time.After(10 * time.Millisecond):
					}
				}
			}
			js, err := jetstream.New(cluster.Clients[0])
			if err != nil {
				t.Fatal(err)
			}
			const authorityName, prefix, bucket = "INDEX_AUTH", "wf.graph.index", "INDEX_OBJECTS"
			for _, config := range []jetstream.StreamConfig{graphpublication.AuthorityStreamConfig(authorityName, prefix, replicas), graphpublication.NativeObjectStreamConfig(bucket, replicas)} {
				if _, err = js.CreateStream(ctx, config); err != nil {
					t.Fatal(err)
				}
			}
			open := func() graphpublication.Protocol {
				a, e := graphpublication.OpenNativeAuthority(ctx, js, authorityName, prefix)
				if e != nil {
					t.Fatal(e)
				}
				port, e := graphpublication.OpenNativePort(ctx, a, bucket)
				if e != nil {
					t.Fatal(e)
				}
				return graphpublication.Protocol{Port: port}
			}
			p := open()
			clock := time.Now().UTC()
			now := func() time.Time { return clock }
			root := graphpublication.EmptyRoot()
			var snapshot []byte
			for i := 0; i < 16; i++ {
				reader, next, e := p.AcquireReader(ctx, "incoming", root.Head, clock.Add(time.Hour))
				if e != nil {
					t.Fatal(e)
				}
				root = next
				view, e := retainedindex.OpenView(ctx, p, reader, "signals", now)
				if e != nil {
					t.Fatal(e)
				}
				packet, _, found, e := view.Update(ctx, key(fmt.Sprint(i)), uint64(i))
				if e != nil || found {
					t.Fatal(found, e)
				}
				root, e = p.ReleaseReader(ctx, reader, root.Head)
				if e != nil {
					t.Fatal(e)
				}
				pending, e := p.PrepareStreamAppendWithApplication(ctx, "incoming", root.Head, "signals", packet, [][]byte{[]byte(fmt.Sprintf("input:%d", i))}, nil, clock.Add(time.Hour), []byte("active"))
				if e != nil {
					t.Fatal(e)
				}
				root, e = p.Commit(ctx, pending)
				if e != nil {
					t.Fatal(e)
				}
				if i == 7 {
					reader, root, e = p.AcquireReader(ctx, "incoming", root.Head, clock.Add(2*time.Hour))
					if e != nil {
						t.Fatal(e)
					}
					snapshot, e = reader.Checkpoint()
					if e != nil {
						t.Fatal(e)
					}
				}
			}
			root, err = p.RetireLiveWithApplication(ctx, "incoming", root.Head, []byte("retired"))
			if err != nil {
				t.Fatal(err)
			}
			// Reopen with no process-local index state; checkpoint restores only
			// the already-authoritative retained pin, not ownership by presence.
			p = open()
			clock = clock.Add(90 * time.Minute)
			reader, root, err := p.ResumeReader(ctx, snapshot, clock)
			if err != nil {
				t.Fatal(err)
			}
			view, err := retainedindex.OpenView(ctx, p, reader, "signals", now)
			if err != nil {
				t.Fatal(err)
			}
			if view.Count() != 8 {
				t.Fatal("restored wrong forest", view.Count())
			}
			if _, err = p.SweepWithReaders(ctx, clock); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 16; i++ {
				value, found, e := view.Lookup(ctx, key(fmt.Sprint(i)))
				if e != nil || found != (i < 8) || found && value != uint64(i) {
					t.Fatal("reopened snapshot differs", i, value, found, e)
				}
				if found {
					r, e := p.ReadRetainedStream(ctx, reader, "signals", value, clock)
					if e != nil || len(r.Blobs) != 1 {
						t.Fatal("indexed input ownership differs", e)
					}
					data, e := p.Port.Get(ctx, r.Blobs[0], 64)
					if e != nil || string(data) != fmt.Sprintf("input:%d", i) {
						t.Fatal("indexed input bytes differ", e)
					}
				}
			}
			root, err = p.Port.ReadRoot(ctx, "incoming")
			if err != nil {
				t.Fatal(err)
			}
			root, err = p.ReleaseReader(ctx, reader, root.Head)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, e := view.Lookup(ctx, key("missing")); !errors.Is(e, graphpublication.ErrRevoked) {
				t.Fatal("released snapshot proved absence", e)
			}
			clock = clock.Add(2 * time.Hour)
			if _, err = p.SweepWithReaders(ctx, clock); err != nil {
				t.Fatal(err)
			}
			objects, err := p.Port.Objects(ctx)
			if err != nil || len(objects) != 0 {
				t.Fatal("live native objects remain", len(objects), err)
			}
			stream, err := js.Stream(ctx, "OBJ_"+bucket)
			if err != nil {
				t.Fatal(err)
			}
			info, err := stream.Info(ctx, jetstream.WithSubjectFilter("$O."+bucket+".>"))
			if err != nil {
				t.Fatal(err)
			}
			if uint64(len(info.State.Subjects)) != info.State.NumSubjects {
				t.Fatal("incomplete physical subject census", len(info.State.Subjects), info.State.NumSubjects)
			}
			for subject := range info.State.Subjects {
				if strings.Contains(subject, ".C.") {
					t.Fatal("physical chunks remain", subject)
				}
			}
			t.Log("reopened retained index: 8 captured keys/inputs, 8 later keys absent, released pin rejected; zero objects and physical chunks")
		})
	}
}
