package graphpublication

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"js-wf/internal/retainedgraph"
)

func TestNativeGraphReadersRetirementAndExpiry(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			port, c := nativeGraphObjectFixture(t, replicas)
			p := Protocol{Port: port}
			now := time.Now().UTC()
			prepared, err := p.PrepareAppend(c, "history", 0, []byte("retained"), [][]byte{[]byte("shared-native")}, now.Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			root, err := p.Commit(c, prepared)
			if err != nil {
				t.Fatal(err)
			}
			reader, root, err := p.AcquireReader(c, "history", root.Head, now.Add(2*time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			original, err := p.ReadRetained(c, reader, 0, now)
			if err != nil {
				t.Fatal(err)
			}
			prepared, err = p.PrepareAppendWithOwned(c, "history", root.Head, []byte("later"), nil, []OwnedPayload{{Index: 0, Link: original.Blobs[0]}}, now.Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			root, err = p.Commit(c, prepared)
			if err != nil {
				t.Fatal(err)
			}
			if err = p.Retire(c, "history", root.Head); err == nil {
				t.Fatal("v1 retirement downgraded schema")
			}
			if err = p.RetireLive(c, "history", root.Head); err != nil {
				t.Fatal(err)
			}
			if _, err = p.Sweep(c, now.Add(time.Hour)); err != nil {
				t.Fatal(err)
			}
			retired, err := port.ReadRoot(c, "history")
			if err != nil || retired.Graph.Count != 0 || retired.Schema != RetentionSchema || len(retired.Readers) != 1 {
				t.Fatal(retired, err)
			}
			// Reopen the same isolated authority/object adapter; no graph bytes or pins
			// are loaded into process-local protection maps.
			reopened, err := OpenNativePort(c, port.NativeAuthority, port.bucket)
			if err != nil {
				t.Fatal(err)
			}
			p.Port = reopened
			got, err := p.ReadRetained(c, reader, 0, now.Add(time.Hour))
			if err != nil || !reflect.DeepEqual(got, original) {
				t.Fatal(got, err)
			}
			payload, err := reopened.Get(c, got.Blobs[0], 100)
			if err != nil || string(payload) != "shared-native" {
				t.Fatal(string(payload), err)
			}
			retired, err = p.RenewReader(c, reader, retired.Head, now.Add(4*time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			if _, err = p.Sweep(c, now.Add(3*time.Hour)); err != nil {
				t.Fatal(err)
			}
			if _, err = retainedgraph.Read(c, nativeGraphReadStore{reopened}, reader.Snapshot(), 0); err != nil {
				t.Fatal(err)
			}
			if _, err = p.Sweep(c, now.Add(4*time.Hour)); err != nil {
				t.Fatal(err)
			}
			final, err := reopened.ReadRoot(c, "history")
			if err != nil || final.Head != retired.Head+1 || len(final.Readers) != 0 || final.Schema != RetentionSchema {
				t.Fatal(final, err)
			}
			if _, err = p.RenewReader(c, reader, final.Head, now.Add(5*time.Hour)); !errors.Is(err, ErrRevoked) {
				t.Fatal(err)
			}
			if _, err = reopened.CASRoot(c, "history", final.Head, EmptyRoot()); err == nil {
				t.Fatal("downgrade after expiry")
			}
			nativeGraphNoObjects(t, reopened, c)
		})
	}
}

// All peers stop before any is reopened. This is a graceful full-store restart,
// not SIGKILL/power loss. Recovery retains only serialized checkpoint bytes.
func TestNativeGraphReaderCheckpointStoreRestart(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			cluster, a, c := nativeGraphFixture(t, replicas)
			if _, err := a.js.CreateStream(c, NativeObjectStreamConfig("READER_RESTART", replicas)); err != nil {
				t.Fatal(err)
			}
			port, err := OpenNativePort(c, a, "READER_RESTART")
			if err != nil {
				t.Fatal(err)
			}
			p := Protocol{Port: port}
			now := time.Now().UTC()
			prepared, err := p.PrepareAppend(c, "history", 0, []byte("restart-snapshot"), [][]byte{[]byte("restart-payload")}, now.Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			root, err := p.Commit(c, prepared)
			if err != nil {
				t.Fatal(err)
			}
			reader, root, err := p.AcquireReader(c, "history", root.Head, now.Add(2*time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			checkpoint, err := reader.Checkpoint()
			if err != nil {
				t.Fatal(err)
			}
			file := filepath.Join(t.TempDir(), "reader-checkpoint.json")
			if err = os.WriteFile(file, checkpoint, 0o600); err != nil {
				t.Fatal(err)
			}
			if err = p.RetireLive(c, "history", root.Head); err != nil {
				t.Fatal(err)
			}
			head := root.Head + 1
			reader = Reader{}
			root = Root{}
			p = Protocol{}
			port = nil
			a = nil
			checkpoint = nil
			for i := range cluster.Servers {
				cluster.KillNode(i)
			}
			for _, server := range cluster.Servers {
				if server.Running() {
					t.Fatal("peer still running at store cut")
				}
			}
			for i := range cluster.Servers {
				if err = cluster.RestartNode(i); err != nil {
					t.Fatal(err)
				}
			}
			// Wait for the restarted metadata quorum without changing any native
			// stream or creating replacement stores. Original fixture context bounds it.
			if replicas > 1 {
				ticker := time.NewTicker(20 * time.Millisecond)
				defer ticker.Stop()
				for {
					ready := false
					for _, server := range cluster.Servers {
						ready = ready || (server.JetStreamIsLeader() && len(server.JetStreamClusterPeers()) == replicas)
					}
					if ready {
						break
					}
					select {
					case <-c.Done():
						t.Fatal(c.Err())
					case <-ticker.C:
					}
				}
			}
			js, err := jetstream.New(cluster.Clients[0])
			if err != nil {
				t.Fatal(err)
			}
			a, err = OpenNativeAuthority(c, js, "GRAPH_AUTH", "wf.graph.authority")
			if err != nil {
				t.Fatal(err)
			}
			port, err = OpenNativePort(c, a, "READER_RESTART")
			if err != nil {
				t.Fatal(err)
			}
			p = Protocol{Port: port}
			checkpoint, err = os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			reader, root, err = p.ResumeReader(c, checkpoint, now.Add(time.Hour))
			if err != nil || root.Head != head || root.Graph.Count != 0 {
				t.Fatal(root, err)
			}
			record, err := p.ReadRetained(c, reader, 0, now.Add(time.Hour))
			if err != nil || string(record.Data) != "restart-snapshot" || len(record.Blobs) != 1 {
				t.Fatal(record, err)
			}
			payload, err := port.Get(c, record.Blobs[0], 100)
			if err != nil || string(payload) != "restart-payload" {
				t.Fatal(string(payload), err)
			}
			root, err = p.RenewReader(c, reader, root.Head, now.Add(3*time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			reader, root, err = p.ResumeReader(c, checkpoint, now.Add(2*time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			if _, err = p.ReleaseReader(c, reader, root.Head); err != nil {
				t.Fatal(err)
			}
			if _, _, err = p.ResumeReader(c, checkpoint, now.Add(2*time.Hour)); !errors.Is(err, ErrRevoked) {
				t.Fatal("released checkpoint resumed", err)
			}
			if _, err = p.Sweep(c, now.Add(2*time.Hour)); err != nil {
				t.Fatal(err)
			}
			nativeGraphNoObjects(t, port, c)
		})
	}
}
