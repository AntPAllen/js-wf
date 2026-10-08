package graphpublication

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

func TestNativeGraphStreamsAtomicLifecycleAndRetainedReaders(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			port, c := nativeGraphObjectFixture(t, replicas)
			p := Protocol{Port: port}
			now := time.Now().UTC()
			root := EmptyRoot()
			payload := bytes.Repeat([]byte("incoming-large-payload"), 40000)
			for _, stream := range []string{"input", "signals"} {
				pending, err := p.PrepareStreamAppendWithApplication(c, "workflow", root.Head, stream, []byte(stream), [][]byte{payload}, nil, now.Add(time.Hour), []byte("active"))
				if err != nil {
					t.Fatal(err)
				}
				root, err = p.Commit(c, pending)
				if err != nil {
					t.Fatal(err)
				}
			}
			pending, err := p.PrepareAppendWithApplication(c, "workflow", root.Head, []byte("started"), nil, nil, now.Add(time.Hour), []byte("active"))
			if err != nil {
				t.Fatal(err)
			}
			root, err = p.Commit(c, pending)
			if err != nil || root.Schema != StreamsSchema || root.Graph.Count != 1 || len(root.Streams) != 2 {
				t.Fatal(root, err)
			}
			reader, root, err := p.AcquireReader(c, "workflow", root.Head, now.Add(2*time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			checkpoint, err := reader.Checkpoint()
			if err != nil {
				t.Fatal(err)
			}
			stale, err := p.PrepareStreamAppendWithApplication(c, "workflow", root.Head, "signals", []byte("late"), nil, nil, now.Add(time.Hour), []byte("active"))
			if err != nil {
				t.Fatal(err)
			}
			root, err = p.RetireLiveWithApplication(c, "workflow", root.Head, []byte("retired"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err = p.Commit(c, stale); err == nil {
				t.Fatal("retirement did not fence signal")
			}
			// Reopen the adapters using only committed native bytes and a durable
			// checkpoint. This is adapter restart, not process-crash qualification.
			authority, err := OpenNativeAuthority(c, port.js, port.name, port.prefix)
			if err != nil {
				t.Fatal(err)
			}
			port, err = OpenNativePort(c, authority, port.bucket)
			if err != nil {
				t.Fatal(err)
			}
			p = Protocol{Port: port}
			reader, root, err = p.ResumeReader(c, checkpoint, now)
			if err != nil || root.Schema != StreamsSchema || root.Graph.Count != 0 || root.Streams[0].Graph.Count != 0 || root.Streams[1].Graph.Count != 0 || string(root.Application) != "retired" {
				t.Fatal(root, err)
			}
			if _, err = p.SweepWithReaders(c, now.Add(time.Hour)); err != nil {
				t.Fatal(err)
			}
			var previousObject string
			for _, stream := range []string{"input", "signals"} {
				r, err := p.ReadRetainedStream(c, reader, stream, 0, now.Add(time.Hour))
				if err != nil || string(r.Data) != stream || len(r.Blobs) != 1 {
					t.Fatal(r, err)
				}
				data, err := port.Get(c, r.Blobs[0], len(payload))
				if err != nil || !bytes.Equal(data, payload) {
					t.Fatal("retained external payload", err)
				}
				if previousObject == r.Blobs[0].Reference.Object {
					t.Fatal("cross-stream transfer reused origin grant")
				}
				previousObject = r.Blobs[0].Reference.Object
			}
			if r, err := p.ReadRetained(c, reader, 0, now); err != nil || string(r.Data) != "started" {
				t.Fatal(r, err)
			}
			root, err = p.ReleaseReader(c, reader, root.Head)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = p.SweepWithReaders(c, now.Add(3*time.Hour)); err != nil {
				t.Fatal(err)
			}
			nativeGraphNoObjects(t, port, c)
			for _, schema := range []string{Schema, RetentionSchema, ApplicationSchema} {
				next := root
				next.Schema = schema
				next.Streams = nil
				next.Application = nil
				if _, err = port.CASRoot(c, "workflow", root.Head, next); err == nil {
					t.Fatal("erased permanent streams schema", schema)
				}
			}
		})
	}
}
