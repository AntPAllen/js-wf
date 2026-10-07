package blobpublication

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/testcluster"
)

func nativeObjectFixture(t *testing.T, replicas int) (*testcluster.Cluster, *NativePort, context.Context, string) {
	t.Helper()
	directory := nativeAuthorityDirectory(t)
	cluster, err := testcluster.Start(directory, replicas)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(func() { cancel(); cluster.Close() })
	if replicas > 1 {
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			ready := false
			for _, s := range cluster.Servers {
				ready = ready || s.JetStreamIsLeader() && len(s.JetStreamClusterPeers()) == replicas
			}
			if ready {
				break
			}
			select {
			case <-ctx.Done():
				t.Fatal("object fixture metadata admission", ctx.Err())
			case <-ticker.C:
			}
		}
	}
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err = js.CreateStream(ctx, AuthorityStreamConfig("BLOB_AUTH", "wf.blob.authority", replicas)); err != nil {
		t.Fatal(err)
	}
	if _, err = js.CreateStream(ctx, NativeObjectStreamConfig("RECOVERABLE_BLOB", replicas)); err != nil {
		t.Fatal(err)
	}
	authority, err := OpenNativeAuthority(ctx, js, "BLOB_AUTH", "wf.blob.authority")
	if err != nil {
		t.Fatal(err)
	}
	port, err := OpenNativePort(ctx, authority, "RECOVERABLE_BLOB")
	if err != nil {
		t.Fatal(err)
	}
	peers := make([]map[string]any, 0, replicas)
	for _, server := range cluster.Servers {
		varz, err := server.Varz(nil)
		if err != nil {
			t.Fatal(err)
		}
		peers = append(peers, map[string]any{"id": server.ID(), "name": server.Name(), "version": varz.Version, "embedding_commit": varz.GitCommit})
	}
	identity, err := json.MarshalIndent(map[string]any{"test": t.Name(), "replicas": replicas, "parent_budget_seconds": 30, "peers": peers}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(directory, "object-fixture-identity.json"), append(identity, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	return cluster, port, ctx, directory
}
func nativePrepare(t *testing.T, p Protocol, ctx context.Context, destination string, blobs ...[]byte) Prepared {
	t.Helper()
	prepared, err := p.Prepare(ctx, destination, []byte(destination), blobs, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	return prepared
}
func nativeCommit(t *testing.T, p Protocol, ctx context.Context, prepared Prepared) Root {
	t.Helper()
	r, err := p.Commit(ctx, prepared)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func nativeSweep(t *testing.T, p Protocol, ctx context.Context) int {
	t.Helper()
	n, err := p.Sweep(ctx, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	return n
}
func nativeCheckRoot(t *testing.T, p *NativePort, ctx context.Context, destination string) {
	t.Helper()
	root, err := p.ReadRoot(ctx, destination)
	if err != nil {
		t.Fatal(err)
	}
	for k, ref := range root.Blobs {
		data, err := p.GetBytes(ctx, ref.Object)
		if err != nil || key(data) != k {
			t.Fatal("native dangling reference", destination, ref, err)
		}
	}
}
func nativeNoChunks(t *testing.T, p *NativePort, ctx context.Context) {
	t.Helper()
	info, err := p.objectStream.Info(ctx, jetstream.WithSubjectFilter("$O."+p.bucket+".>"))
	if err != nil {
		t.Fatal(err)
	}
	for subject := range info.State.Subjects {
		if bytes.Contains([]byte(subject), []byte(".C.")) {
			t.Fatal("native partial chunks leaked", subject)
		}
	}
}
func nativeObjectProof(t *testing.T, directory string, proof map[string]any) {
	t.Helper()
	data, err := json.MarshalIndent(proof, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(directory, "object-proof.json"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestNativeObjectPublicationLifecycle(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			_, port, ctx, directory := nativeObjectFixture(t, replicas)
			p := Protocol{Port: port}
			shared := bytes.Repeat([]byte("shared-native"), 30000)
			private := []byte("private-native")
			a := nativeCommit(t, p, ctx, nativePrepare(t, p, ctx, "a", shared, private, shared))
			b := nativeCommit(t, p, ctx, nativePrepare(t, p, ctx, "b", shared))
			if n := nativeSweep(t, p, ctx); n != 0 {
				t.Fatal("published blobs reclaimed", n)
			}
			nativeCheckRoot(t, port, ctx, "a")
			nativeCheckRoot(t, port, ctx, "b")
			if err := p.Retire(ctx, "a", a.Head); err != nil {
				t.Fatal(err)
			}
			if n := nativeSweep(t, p, ctx); n != 1 {
				t.Fatal("private reference not reclaimed", n)
			}
			nativeCheckRoot(t, port, ctx, "b")
			if err := p.Retire(ctx, "b", b.Head); err != nil {
				t.Fatal(err)
			}
			if n := nativeSweep(t, p, ctx); n != 1 {
				t.Fatal("shared reference not reclaimed", n)
			}
			nativeNoChunks(t, port, ctx)
			info, err := port.objectStream.Info(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if info.State.Msgs != 2 {
				t.Fatal("expected exactly two permanent attempt tombstones", info.State)
			}
			nativeObjectProof(t, directory, map[string]any{"scenario": "shared-lifecycle", "replicas": replicas, "object_stream": info, "payload_bytes": len(shared), "standard_reader_verified": true, "all_roots_retired": true, "all_chunks_reclaimed": true})
			t.Logf("native object lifecycle: R%d payload=%d standard reads verified; shared protection and two retirements; chunks=0 tombstones=2", replicas, len(shared))
		})
	}
}

// Pause the actual metadata publication after all real chunks have committed.
// Collection creates a tombstone before the old packet reaches the server.
func TestNativePartialUploadPublicationFence(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		for _, mode := range []string{"resume", "crash"} {
			t.Run(fmt.Sprintf("R%d/%s", replicas, mode), func(t *testing.T) {
				cluster, controller, ctx, directory := nativeObjectFixture(t, replicas)
				payload := bytes.Repeat([]byte("partial-native"), 22000)
				k := key(payload)
				name := objectName(k, 1, "attempt")
				proxy, err := testcluster.NewClientProxy(cluster.Servers[0].ClientURL())
				if err != nil {
					t.Fatal(err)
				}
				defer proxy.Close()
				if err = proxy.HoldFirstPublication(controller.metaSubject(name)); err != nil {
					t.Fatal(err)
				}
				if err = proxy.EnableTrafficTrace(2 << 20); err != nil {
					t.Fatal(err)
				}
				conn, err := nats.Connect(proxy.URL(), nats.NoReconnect())
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()
				js, err := jetstream.New(conn)
				if err != nil {
					t.Fatal(err)
				}
				authority, err := OpenNativeAuthority(ctx, js, "BLOB_AUTH", "wf.blob.authority")
				if err != nil {
					t.Fatal(err)
				}
				writer, err := OpenNativePort(ctx, authority, controller.bucket)
				if err != nil {
					t.Fatal(err)
				}
				ids := []string{"transaction", "attempt"}
				index := 0
				p := Protocol{Port: writer, NewID: func() (string, error) {
					if index >= len(ids) {
						return "", errors.New("unexpected ID allocation")
					}
					id := ids[index]
					index++
					return id, nil
				}}
				result := make(chan error, 1)
				go func() {
					_, err := p.Prepare(ctx, "paused", nil, [][]byte{payload}, time.Now().Add(time.Minute))
					result <- err
				}()
				ticker := time.NewTicker(time.Millisecond)
				defer ticker.Stop()
				for proxy.PendingAPI() == nil {
					select {
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					case <-ticker.C:
					}
				}
				held := proxy.PendingAPI()
				if held.Subject != controller.metaSubject(name) || held.ForwardedBytes != 0 || held.Disposition != "held" {
					t.Fatal("wrong held upload metadata", held)
				}
				before, err := controller.objectStream.Info(ctx, jetstream.WithSubjectFilter("$O."+controller.bucket+".>"))
				if err != nil {
					t.Fatal(err)
				}
				if before.State.Msgs != 3 || before.State.NumSubjects != 1 {
					t.Fatal("expected three chunks without metadata", before.State)
				}
				if n := nativeSweep(t, Protocol{Port: controller}, ctx); n != 1 {
					t.Fatal("known partial upload not reclaimed", n)
				}
				nativeNoChunks(t, controller, ctx)
				tombstone, _, err := controller.metadata(ctx, name)
				if err != nil || tombstone == nil || !tombstone.Deleted {
					t.Fatal("partial attempt not fenced", tombstone, err)
				}
				if mode == "resume" {
					proxy.ReleaseFirstAPI()
				} else {
					conn.Close()
					proxy.Close()
				}
				select {
				case err = <-result:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				if mode == "resume" && !errors.Is(err, ErrConflict) || mode == "crash" && err == nil {
					t.Fatal("old upload metadata accepted", mode, err)
				}
				nativeSweep(t, Protocol{Port: controller}, ctx)
				nativeNoChunks(t, controller, ctx)
				root, err := controller.ReadRoot(ctx, "paused")
				if err != nil || root.Head != 1 || root.Token != "" {
					t.Fatal("expired upload root not fenced", root, err)
				}
				// The same logical content can be published at a new physical generation.
				fresh := nativeCommit(t, Protocol{Port: controller}, ctx, nativePrepare(t, Protocol{Port: controller}, ctx, "fresh", payload))
				nativeCheckRoot(t, controller, ctx, "fresh")
				for _, ref := range fresh.Blobs {
					if ref.Generation != 2 || ref.Object == name {
						t.Fatal("physical generation reused")
					}
				}
				if err = (&Protocol{Port: controller}).Retire(ctx, "fresh", fresh.Head); err != nil {
					t.Fatal(err)
				}
				nativeSweep(t, Protocol{Port: controller}, ctx)
				nativeNoChunks(t, controller, ctx)
				after, err := controller.objectStream.Info(ctx)
				if err != nil {
					t.Fatal(err)
				}
				nativeObjectProof(t, directory, map[string]any{"scenario": "paused-partial-" + mode, "replicas": replicas, "held": held, "final_packet": proxy.PendingAPI(), "wire": proxy.TrafficTrace(), "partial_before": before, "deleted_partial": tombstone, "expired_root": root, "fresh_root": fresh, "object_stream": after, "all_chunks_reclaimed": true})
				t.Logf("native partial upload: R%d mode=%s chunks=3 metadata held; reclaimed then old metadata rejected/canceled; generation2 readable; final chunks=0", replicas, mode)
			})
		}
	}
}

func TestNativeUnknownChunksBlockCollection(t *testing.T) {
	_, port, ctx, directory := nativeObjectFixture(t, 1)
	p := Protocol{Port: port}
	data := []byte("orphan")
	pending := nativePrepare(t, p, ctx, "orphan", data)
	var name string
	for _, ref := range pending.publication.Blobs {
		name = ref.Object
	}
	unknown := "$O." + port.bucket + ".C.opaque-foreign-id"
	if _, err := port.js.Publish(ctx, unknown, []byte("opaque")); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Sweep(ctx, time.Now().Add(time.Hour)); !errors.Is(err, ErrUntrackedChunks) {
		t.Fatal("unknown chunks did not block", err)
	}
	if _, err := port.GetBytes(ctx, name); err != nil {
		t.Fatal("collection deleted before complete census", err)
	}
	if err := port.objectStream.Purge(ctx, jetstream.WithPurgeSubject(unknown)); err != nil {
		t.Fatal(err)
	}
	nativeSweep(t, p, ctx)
	nativeNoChunks(t, port, ctx)
	nativeObjectProof(t, directory, map[string]any{"scenario": "unknown-chunk-control", "opaque_subject": unknown, "blocked_before_deletion": true, "all_chunks_reclaimed": true})
}

func nativeProxyWriter(t *testing.T, cluster *testcluster.Cluster, controller *NativePort, ctx context.Context, subject string) (*NativePort, *testcluster.ClientProxy, *nats.Conn) {
	t.Helper()
	proxy, err := testcluster.NewClientProxy(cluster.Servers[0].ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(proxy.Close)
	if err = proxy.HoldFirstPublication(subject); err != nil {
		t.Fatal(err)
	}
	if err = proxy.EnableTrafficTrace(2 << 20); err != nil {
		t.Fatal(err)
	}
	conn, err := nats.Connect(proxy.URL(), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(conn.Close)
	js, err := jetstream.New(conn)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := OpenNativeAuthority(ctx, js, "BLOB_AUTH", "wf.blob.authority")
	if err != nil {
		t.Fatal(err)
	}
	writer, err := OpenNativePort(ctx, authority, controller.bucket)
	if err != nil {
		t.Fatal(err)
	}
	return writer, proxy, conn
}
func nativeWait(t *testing.T, ctx context.Context, ready func() bool) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for !ready() {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}
}
func nativeAttemptProtocol(writer *NativePort) Protocol {
	ids := []string{"transaction", "attempt"}
	index := 0
	return Protocol{Port: writer, NewID: func() (string, error) {
		if index >= len(ids) {
			return "", errors.New("unexpected ID allocation")
		}
		id := ids[index]
		index++
		return id, nil
	}}
}

// A first chunk held before the server sees any bytes is absent from the
// collector's census. Its later arrival must remain an orphan and cannot damage
// an already committed new generation of the same logical content.
func TestNativeLateChunkAfterGenerationClosed(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			cluster, controller, ctx, directory := nativeObjectFixture(t, replicas)
			payload := bytes.Repeat([]byte("late-chunk-native"), 19000)
			name := objectName(key(payload), 1, "attempt")
			writer, proxy, _ := nativeProxyWriter(t, cluster, controller, ctx, controller.chunkSubject(name))
			result := make(chan error, 1)
			go func() {
				_, err := nativeAttemptProtocol(writer).Prepare(ctx, "late", nil, [][]byte{payload}, time.Now().Add(time.Minute))
				result <- err
			}()
			nativeWait(t, ctx, func() bool { return proxy.PendingAPI() != nil })
			held := proxy.PendingAPI()
			if held.Subject != controller.chunkSubject(name) || held.ForwardedBytes != 0 || held.Disposition != "held" {
				t.Fatal("wrong held chunk", held)
			}
			before, err := controller.objectStream.Info(ctx)
			if err != nil || before.State.Msgs != 0 {
				t.Fatal("chunk reached server before release", before, err)
			}
			if n := nativeSweep(t, Protocol{Port: controller}, ctx); n != 0 {
				t.Fatal("absent chunk reported deleted", n)
			}
			closed, err := controller.ReadBlob(ctx, key(payload))
			if err != nil || closed.Fence.Phase != "closed" || closed.Fence.Generation != 1 {
				t.Fatal("generation not closed", closed, err)
			}
			fresh := nativeCommit(t, Protocol{Port: controller}, ctx, nativePrepare(t, Protocol{Port: controller}, ctx, "fresh", payload))
			if fresh.Blobs[key(payload)].Generation != 2 || fresh.Blobs[key(payload)].Object == name {
				t.Fatal("fresh generation reused")
			}
			proxy.ReleaseFirstAPI()
			select {
			case err = <-result:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if !errors.Is(err, ErrRevoked) {
				t.Fatal("late upload adopted", err)
			}
			late, _, err := controller.metadata(ctx, name)
			if err != nil || late == nil || late.Deleted || late.Chunks != 3 {
				t.Fatal("late physical orphan not retained", late, err)
			}
			if n := nativeSweep(t, Protocol{Port: controller}, ctx); n != 1 {
				t.Fatal("late orphan not reclaimed", n)
			}
			nativeCheckRoot(t, controller, ctx, "fresh")
			if _, err = controller.GetBytes(ctx, name); !errors.Is(err, jetstream.ErrObjectNotFound) {
				t.Fatal("old orphan remains readable", err)
			}
			tombstone, _, err := controller.metadata(ctx, name)
			if err != nil || tombstone == nil || !tombstone.Deleted {
				t.Fatal("late orphan not fenced", err)
			}
			if err = (&Protocol{Port: controller}).Retire(ctx, "fresh", fresh.Head); err != nil {
				t.Fatal(err)
			}
			nativeSweep(t, Protocol{Port: controller}, ctx)
			nativeNoChunks(t, controller, ctx)
			after, err := controller.objectStream.Info(ctx)
			if err != nil || after.State.Msgs != 2 {
				t.Fatal("late chunk cleanup", after, err)
			}
			nativeObjectProof(t, directory, map[string]any{"scenario": "late-chunk", "replicas": replicas, "held": held, "final_packet": proxy.PendingAPI(), "wire": proxy.TrafficTrace(), "partial_before": before, "closed_generation": closed, "late_orphan": late, "deleted_partial": tombstone, "fresh_root": fresh, "object_stream": after, "fresh_root_survived_old_delete": true, "all_chunks_reclaimed": true})
			t.Logf("native late chunks: R%d first chunk held before arrival; generation1 closed, generation2 committed; old uploader revoked, orphan reclaimed, fresh reference readable", replicas)
		})
	}
}

// Hold server replies only after all chunks are acknowledged and the metadata
// packet is captured. Independent leader reads prove metadata committed before
// caller cancellation. A failed Prepare must never infer upload success from it.
func TestNativeCommittedMetadataReplyLost(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			cluster, controller, ctx, directory := nativeObjectFixture(t, replicas)
			payload := bytes.Repeat([]byte("lost-metadata-native"), 16000)
			name := objectName(key(payload), 1, "attempt")
			writer, proxy, conn := nativeProxyWriter(t, cluster, controller, ctx, controller.metaSubject(name))
			writeCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			result := make(chan error, 1)
			go func() {
				_, err := nativeAttemptProtocol(writer).Prepare(writeCtx, "lost", nil, [][]byte{payload}, time.Now().Add(time.Minute))
				result <- err
			}()
			nativeWait(t, ctx, func() bool { return proxy.PendingAPI() != nil })
			held := proxy.PendingAPI()
			if held.ForwardedBytes != 0 || held.Subject != controller.metaSubject(name) {
				t.Fatal("wrong lost-reply boundary", held)
			}
			proxy.HoldResponses()
			proxy.ReleaseFirstAPI()
			var metadata *jetstream.ObjectInfo
			nativeWait(t, ctx, func() bool {
				var err error
				metadata, _, err = controller.metadata(ctx, name)
				if err != nil {
					t.Fatal(err)
				}
				return metadata != nil && proxy.Stats().HeldBytes > 0
			})
			if metadata.Deleted || metadata.Chunks != 3 {
				t.Fatal("metadata not committed", metadata)
			}
			if data, err := controller.GetBytes(ctx, name); err != nil || !bytes.Equal(data, payload) {
				t.Fatal("committed metadata unreadable", err)
			}
			stats := proxy.Stats()
			if !stats.ResponsesHeld || stats.BufferedBytes == 0 || stats.BufferOverflows != 0 {
				t.Fatal("invalid reply hold", stats)
			}
			cancel()
			select {
			case err := <-result:
				if !errors.Is(err, context.Canceled) {
					t.Fatal("lost publication acknowledged", err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			conn.Close()
			proxy.Close()
			unpromoted, err := controller.ReadBlob(ctx, key(payload))
			if err != nil || unpromoted.Fence.Phase != "uploading" || unpromoted.Fence.Object != "" {
				t.Fatal("ambiguous metadata adopted", unpromoted, err)
			}
			root, err := controller.ReadRoot(ctx, "lost")
			if err != nil || root.Head != 0 || root.Token != "" {
				t.Fatal("failed prepare published root", root, err)
			}
			if n := nativeSweep(t, Protocol{Port: controller}, ctx); n != 1 {
				t.Fatal("ambiguous upload leaked", n)
			}
			nativeNoChunks(t, controller, ctx)
			after, err := controller.objectStream.Info(ctx)
			if err != nil || after.State.Msgs != 1 {
				t.Fatal("lost reply cleanup", after, err)
			}
			nativeObjectProof(t, directory, map[string]any{"scenario": "committed-metadata-reply-lost", "replicas": replicas, "held": held, "final_packet": proxy.PendingAPI(), "wire": proxy.TrafficTrace(), "held_reply_stats": stats, "committed_metadata": metadata, "unpromoted_fence": unpromoted, "unpublished_root": root, "object_stream": after, "cancelled_before_reply": true, "all_chunks_reclaimed": true})
			t.Logf("native metadata reply loss: R%d committed metadata and standard bytes independently read; response buffered, caller canceled; no ready adoption/root publication; orphan reclaimed", replicas)
		})
	}
}

func TestNativeObjectSameStoreColdRestart(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			cluster, port, ctx, directory := nativeObjectFixture(t, replicas)
			payload := bytes.Repeat([]byte("cold-object-native"), 18000)
			p := Protocol{Port: port}
			live := nativeCommit(t, p, ctx, nativePrepare(t, p, ctx, "live", payload))
			dead := nativeCommit(t, p, ctx, nativePrepare(t, p, ctx, "dead", []byte("dead-native")))
			if err := p.Retire(ctx, "dead", dead.Head); err != nil {
				t.Fatal(err)
			}
			if n := nativeSweep(t, p, ctx); n != 1 {
				t.Fatal("retired object not deleted", n)
			}
			var deadName string
			for _, ref := range dead.Blobs {
				deadName = ref.Object
			}
			oldIDs := make([]string, replicas)
			for i, s := range cluster.Servers {
				oldIDs[i] = s.ID()
				s.Shutdown()
			}
			for _, s := range cluster.Servers {
				s.WaitForShutdown()
			}
			for i := range cluster.Servers {
				if err := cluster.RestartNode(i); err != nil {
					t.Fatal(err)
				}
			}
			if replicas > 1 {
				nativeWait(t, ctx, func() bool {
					meta, auth, object := false, false, false
					for _, s := range cluster.Servers {
						meta = meta || s.JetStreamIsLeader() && len(s.JetStreamClusterPeers()) == replicas
						auth = auth || s.JetStreamIsStreamLeader("$G", "BLOB_AUTH")
						object = object || s.JetStreamIsStreamLeader("$G", "OBJ_RECOVERABLE_BLOB")
					}
					return meta && auth && object && cluster.Servers[0].JetStreamIsCurrent()
				})
			}
			js, err := jetstream.New(cluster.Clients[0])
			if err != nil {
				t.Fatal(err)
			}
			authority, err := OpenNativeAuthority(ctx, js, "BLOB_AUTH", "wf.blob.authority")
			if err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenNativePort(ctx, authority, port.bucket)
			if err != nil {
				t.Fatal(err)
			}
			nativeCheckRoot(t, reopened, ctx, "live")
			retired, err := reopened.ReadRoot(ctx, "dead")
			if err != nil || retired.Head != 2 || retired.Token != "" {
				t.Fatal("cold root fence reset", retired, err)
			}
			tombstone, _, err := reopened.metadata(ctx, deadName)
			if err != nil || tombstone == nil || !tombstone.Deleted {
				t.Fatal("cold attempt tombstone lost", tombstone, err)
			}
			oldInfo := *tombstone
			oldInfo.Deleted = false
			if err = reopened.publishMetadata(ctx, oldInfo, 0); !errors.Is(err, ErrConflict) {
				t.Fatal("cold attempt reopened", err)
			}
			newIDs := make([]string, replicas)
			for i, s := range cluster.Servers {
				newIDs[i] = s.ID()
				if newIDs[i] == oldIDs[i] {
					t.Fatal("server instance not replaced")
				}
			}
			if n := nativeSweep(t, Protocol{Port: reopened}, ctx); n != 0 {
				t.Fatal("cold live object deleted", n)
			}
			if err = (&Protocol{Port: reopened}).Retire(ctx, "live", live.Head); err != nil {
				t.Fatal(err)
			}
			if n := nativeSweep(t, Protocol{Port: reopened}, ctx); n != 1 {
				t.Fatal("cold live retirement failed", n)
			}
			nativeNoChunks(t, reopened, ctx)
			info, err := reopened.objectStream.Info(ctx)
			if err != nil || info.State.Msgs != 2 {
				t.Fatal("cold final state", info, err)
			}
			nativeObjectProof(t, directory, map[string]any{"scenario": "same-store-cold-restart", "replicas": replicas, "old_server_ids": oldIDs, "new_server_ids": newIDs, "live_root": live, "retired_root": retired, "deleted_partial": tombstone, "old_metadata_rejected": true, "standard_reader_after_cold_restart": true, "object_stream": info, "all_chunks_reclaimed": true})
			t.Logf("native object cold restart: R%d all peers shut down/joined/replaced on same stores; live bytes and attempt tombstones retained, stale metadata rejected; chunks=0", replicas)
		})
	}
}

func TestNativeObjectUnsafeConfiguration(t *testing.T) {
	_, port, ctx, directory := nativeObjectFixture(t, 1)
	cases := []struct {
		name   string
		change func(*jetstream.StreamConfig)
	}{
		{"age", func(c *jetstream.StreamConfig) { c.MaxAge = time.Hour }},
		{"messages", func(c *jetstream.StreamConfig) { c.MaxMsgs = 10 }},
		{"bytes", func(c *jetstream.StreamConfig) { c.MaxBytes = 1 << 20 }},
		{"chunks_per_subject", func(c *jetstream.StreamConfig) { c.MaxMsgsPerSubject = 1 }},
		{"direct", func(c *jetstream.StreamConfig) { c.AllowDirect = true }},
		{"memory", func(c *jetstream.StreamConfig) { c.Storage = jetstream.MemoryStorage }},
		{"ttl", func(c *jetstream.StreamConfig) { c.AllowMsgTTL = true }},
		{"no_rollup", func(c *jetstream.StreamConfig) { c.AllowRollup = false }},
		{"purge_denied", func(c *jetstream.StreamConfig) { c.DenyPurge = true }},
		{"delete_allowed", func(c *jetstream.StreamConfig) { c.DenyDelete = false }},
		{"wrong_format", func(c *jetstream.StreamConfig) { c.Metadata = nil }},
		{"async_persist", func(c *jetstream.StreamConfig) { c.PersistMode = jetstream.AsyncPersistMode }},
		{"atomic", func(c *jetstream.StreamConfig) { c.AllowAtomicPublish = true }},
		{"schedules", func(c *jetstream.StreamConfig) { c.AllowMsgSchedules = true }},
	}
	var rejected []string
	for i, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			bucket := fmt.Sprintf("UNSAFE_OBJECT_%d", i)
			c := NativeObjectStreamConfig(bucket, 1)
			test.change(&c)
			if _, err := port.js.CreateStream(ctx, c); err != nil {
				var api *jetstream.APIError
				if test.name != "purge_denied" || !errors.As(err, &api) || api.ErrorCode != 10052 {
					t.Fatal(err)
				}
				rejected = append(rejected, test.name)
				return
			}
			if test.name == "purge_denied" {
				t.Fatal("server allowed rollup without purge")
			}
			if _, err := OpenNativePort(ctx, port.NativeAuthority, bucket); err == nil {
				t.Fatal("unsafe object format accepted")
			}
			rejected = append(rejected, test.name)
		})
	}
	if _, err := OpenNativePort(ctx, port.NativeAuthority, "MISSING"); !errors.Is(err, jetstream.ErrStreamNotFound) {
		t.Fatal("missing bucket created", err)
	}
	c := NativeObjectStreamConfig(port.bucket, 1)
	c.MaxAge = time.Hour
	if _, err := port.js.UpdateStream(ctx, c); err != nil {
		t.Fatal(err)
	}
	if _, err := port.Objects(ctx); err == nil {
		t.Fatal("unsafe post-open census accepted")
	}
	if err := port.Delete(ctx, objectName(key([]byte("blocked")), 1, "attempt")); err == nil {
		t.Fatal("unsafe post-open deletion accepted")
	}
	nativeObjectProof(t, directory, map[string]any{"scenario": "unsafe-object-config", "replicas": 1, "rejected": rejected, "missing_not_created": true, "post_open_collection_blocked": true, "server_rejected_rollup_without_purge": true})
}
