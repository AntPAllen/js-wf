package graphpublication

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/internal/retainedgraph"
)

func TestNativeGraphCompactionStageStoreRestart(t *testing.T) {
	testNativeGraphCompactionStageStoreRestart(t, false)
}

func TestNativeGraphCompactionStageRenewalStoreRestart(t *testing.T) {
	testNativeGraphCompactionStageStoreRestart(t, true)
}

func testNativeGraphCompactionStageStoreRestart(t *testing.T, renew bool) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			cluster, authority, c := nativeGraphFixture(t, replicas)
			if _, err := authority.js.CreateStream(c, NativeObjectStreamConfig("STAGE_RESTART", replicas)); err != nil {
				t.Fatal(err)
			}
			port, err := OpenNativePort(c, authority, "STAGE_RESTART")
			if err != nil {
				t.Fatal(err)
			}
			p := Protocol{Port: port}
			root := EmptyRoot()
			for i := 0; i < 4; i++ {
				prepared, err := p.PrepareAppend(c, "history", root.Head, []byte(fmt.Sprint("record-", i)), [][]byte{[]byte("shared-native")}, time.Now().Add(time.Hour))
				if err != nil {
					t.Fatal(err)
				}
				root, err = p.Commit(c, prepared)
				if err != nil {
					t.Fatal(err)
				}
			}
			oldExpiry := time.Now().UTC().Add(time.Hour)
			if renew {
				oldExpiry = time.Now().UTC().Add(time.Minute)
			}
			stage, err := p.BeginPrefixCompaction(c, "history", root.Head, 2, 1024, oldExpiry, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, done, err := stage.Advance(c, 1); err != nil || done {
				t.Fatal(done, err)
			}
			data, err := stage.Checkpoint()
			if err != nil {
				t.Fatal(err)
			}
			file := filepath.Join(t.TempDir(), "stage.json")
			if err = os.WriteFile(file, data, 0o600); err != nil {
				t.Fatal(err)
			}
			stage = nil
			data = nil
			p = Protocol{}
			port = nil
			authority = nil
			root = Root{}
			for i := range cluster.Servers {
				cluster.KillNode(i)
			}
			for _, server := range cluster.Servers {
				if server.Running() {
					t.Fatal("peer still running")
				}
			}
			for i := range cluster.Servers {
				if err = cluster.RestartNode(i); err != nil {
					t.Fatal(err)
				}
			}
			// As with the existing reader restart fixture, witness a restored
			// metadata quorum before using the bounded authority opener.
			ticker := time.NewTicker(20 * time.Millisecond)
			defer ticker.Stop()
			for {
				ready := replicas == 1
				for _, server := range cluster.Servers {
					ready = ready || (server.JetStreamIsLeader() && len(server.JetStreamClusterPeers()) == replicas)
				}
				if ready && cluster.Clients[0].IsConnected() {
					break
				}
				select {
				case <-ticker.C:
				case <-c.Done():
					t.Fatal(c.Err())
				}
			}
			js, err := jetstream.New(cluster.Clients[0])
			if err != nil {
				t.Fatal(err)
			}
			authority, err = OpenNativeAuthority(c, js, "GRAPH_AUTH", "wf.graph.authority")
			if err != nil {
				t.Fatal(err)
			}
			port, err = OpenNativePort(c, authority, "STAGE_RESTART")
			if err != nil {
				t.Fatal(err)
			}
			p = Protocol{Port: port}
			data, err = os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			stage, err = p.ResumePrefixCompaction(c, data)
			if err != nil || stage.NextIndex() != 1 {
				t.Fatal(err)
			}
			if renew {
				before, err := port.ReadRoot(c, "history")
				if err != nil {
					t.Fatal(err)
				}
				nextExpiry := oldExpiry.Add(2 * time.Minute)
				renewal, err := stage.BeginIntentRenewal(c, func() time.Time { return time.Now().UTC() }, nextExpiry)
				if err != nil {
					t.Fatal(err)
				}
				for {
					prior := renewal.ExaminedScopes()
					done, err := renewal.Advance(c, 1)
					if err != nil || renewal.ExaminedScopes()-prior > 1 || stage.NextIndex() != 1 {
						t.Fatal("native renewal exceeded budget", err)
					}
					if done {
						break
					}
				}
				data, err = stage.Checkpoint()
				if err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(file, data, 0o600); err != nil {
					t.Fatal(err)
				}
				stage = nil
				data, err = os.ReadFile(file)
				if err != nil {
					t.Fatal(err)
				}
				stage, err = p.ResumePrefixCompaction(c, data)
				if err != nil || stage.NextIndex() != 1 || !stage.prepared.expires.Equal(nextExpiry) {
					t.Fatal("native renewed checkpoint lost expiry", err)
				}
				// Sweep uses a controlled authority clock beyond the old expiry;
				// this is no claim about server/VM clock-jump qualification.
				if _, err = p.SweepWithReaders(c, oldExpiry.Add(time.Second)); err != nil {
					t.Fatal(err)
				}
				after, err := port.ReadRoot(c, "history")
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatal("old expiry fenced renewed stage", before, after, err)
				}
				t.Logf("NATIVE_STAGE_RENEWAL replicas=%d checkpoint_next=1 examined=%d renewed=%d scope_budget=1 root_unchanged_after_old_expiry=true", replicas, renewal.ExaminedScopes(), renewal.RenewedScopes())
			}
			var plan PreparedCompaction
			for {
				var done bool
				plan, done, err = stage.Advance(c, 1)
				if err != nil {
					t.Fatal(err)
				}
				if done {
					break
				}
				data, err = stage.Checkpoint()
				if err != nil {
					t.Fatal(err)
				}
				stage, err = p.ResumePrefixCompaction(c, data)
				if err != nil {
					t.Fatal(err)
				}
			}
			commit, err := p.BeginCompactionCommit(c, plan)
			if err != nil {
				t.Fatal(err)
			}
			for {
				beforeRecords, beforeNodes := commit.NextIndex(), commit.VerifiedNodes()
				var done bool
				root, done, err = commit.Advance(c, 1, 1)
				if err != nil || commit.NextIndex()-beforeRecords > 1 || commit.VerifiedNodes()-beforeNodes > 1 {
					t.Fatal("native verification exceeded batch", err)
				}
				if done {
					break
				}
				current, err := port.ReadRoot(c, "history")
				if err != nil || current.Head != plan.expected || current.Graph.Count != 4 || root.Head != 0 {
					t.Fatal("native partial verification published", current, root, err)
				}
			}
			if commit.NextIndex() != 4 || commit.VerifiedNodes() != 6 {
				t.Fatal("native verification omitted records or nodes", commit.NextIndex(), commit.VerifiedNodes())
			}
			deleted, err := p.SweepWithReaders(c, time.Now().Add(2*time.Hour))
			if err != nil || deleted == 0 {
				t.Fatal("old objects not reclaimed", deleted, err)
			}
			for i := uint64(0); i < 4; i++ {
				graph, index := root.Graph, i-2
				if i < 2 {
					graph, index = selectGraph(root.Graph, root.Streams, PrefixArchiveStream), i
				}
				record, err := retainedgraph.Read(c, nativeGraphReadStore{port}, graph, index)
				if err != nil || string(record.Data) != fmt.Sprint("record-", i) || len(record.Blobs) != 1 {
					t.Fatal(record, err)
				}
				payload, err := port.Get(c, record.Blobs[0], 1024)
				if err != nil || string(payload) != "shared-native" {
					t.Fatal(string(payload), err)
				}
			}
			t.Logf("NATIVE_COMPACTION_STAGE replicas=%d checkpoint_next=1 records=4 archive=2 live=2 original_objects_deleted=%d checkpoint_bytes=%d", replicas, deleted, len(data))
			t.Logf("NATIVE_COMPACTION_COMMIT replicas=%d compared_records=4 verified_nodes=6 record_budget=1 node_budget=1", replicas)
		})
	}
}
