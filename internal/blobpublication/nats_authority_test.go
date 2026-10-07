package blobpublication

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/testcluster"
)

// Native testing covers only the durable authority, not ObjectStore cleanup or
// migration of the runtime's workflow streams to this publication destination.
func TestNativeAuthorityHeadAndGenerationPersistence(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) { nativeAuthorityPersistence(t, replicas) })
	}
}
func nativeAuthorityPersistence(t *testing.T, replicas int) {
	directory := nativeAuthorityDirectory(t)
	cluster, err := testcluster.Start(directory, replicas)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
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
			case <-ctx.Done():
				t.Fatal("metadata admission:", ctx.Err())
			case <-ticker.C:
			}
		}
	}
	config := AuthorityStreamConfig("BLOB_AUTH", "wf.blob.authority", replicas)
	stream, err := js.CreateStream(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := OpenNativeAuthority(ctx, js, config.Name, "wf.blob.authority")
	if err != nil {
		t.Fatal(err)
	}
	// Interleaved subjects force global sequences to differ from logical heads.
	a, err := authority.CASRoot(ctx, "a", 0, Root{Token: "first", Data: []byte("keep")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = authority.CASRoot(ctx, "b", 0, Root{}); err != nil {
		t.Fatal(err)
	}
	fenced, err := authority.CASRoot(ctx, "a", a.Head, a)
	if err != nil || fenced.Head != 2 || string(fenced.Data) != "keep" {
		t.Fatal("native fence lost logical head or content", err, fenced)
	}
	if _, err = authority.CASRoot(ctx, "a", a.Head, Root{}); !errors.Is(err, ErrConflict) {
		t.Fatal("stale publication admitted", err)
	}
	if err = stream.Purge(ctx); err == nil {
		t.Fatal("authority allowed purge")
	}
	raw, err := stream.GetLastMsgForSubject(ctx, authority.subject("root", "a"))
	if err != nil {
		t.Fatal(err)
	}
	if raw.Sequence == fenced.Head {
		t.Fatal("fixture didn't separate global and logical heads")
	}
	if err = stream.DeleteMsg(ctx, raw.Sequence); err == nil {
		t.Fatal("authority allowed delete")
	}
	wireProof := nativeLateAuthorityPublication(t, ctx, cluster, authority)
	if err = (&Protocol{Port: &authorityOnlyPort{Authority: authority}}).Retire(ctx, "a", fenced.Head); err != nil {
		t.Fatal(err)
	}
	blobKey := key([]byte("blob"))
	intent := Intent{Root: "pending", Expires: time.Now().Add(time.Hour)}
	pending, err := authority.CASBlob(ctx, blobKey, 0, Fence{Generation: 1, Phase: "uploading", Intents: map[string]Intent{"pending": intent}})
	if err != nil {
		t.Fatal(err)
	}
	selected := pending.Fence
	selected.Phase = "ready"
	selected.Object = objectName(blobKey, 1, "attempt")
	ready, err := authority.CASBlob(ctx, blobKey, pending.Revision, selected)
	if err != nil {
		t.Fatal(err)
	}
	changed := ready.Fence
	changed.Object = objectName(blobKey, 1, "other")
	if _, err = authority.CASBlob(ctx, blobKey, ready.Revision, changed); err == nil {
		t.Fatal("selected object changed")
	}
	closed, err := authority.CASBlob(ctx, blobKey, ready.Revision, Fence{Generation: 1, Phase: "closed"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = authority.CASBlob(ctx, blobKey, closed.Revision, pending.Fence); err == nil {
		t.Fatal("closed generation reopened")
	}
	keys, err := authority.BlobKeys(ctx)
	if err != nil || len(keys) != 1 || keys[0] != blobKey {
		t.Fatal("authority census", keys, err)
	}
	cluster.Servers[0].Shutdown()
	cluster.Servers[0].WaitForShutdown()
	if err = cluster.RestartNode(0); err != nil {
		t.Fatal(err)
	}
	js, err = jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	if replicas > 1 {
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			meta, streamLeader := false, false
			for _, server := range cluster.Servers {
				meta = meta || server.JetStreamIsLeader()
				streamLeader = streamLeader || server.JetStreamIsStreamLeader("$G", config.Name)
			}
			if meta && streamLeader && cluster.Servers[0].NumRoutes() > 0 && cluster.Servers[0].JetStreamIsCurrent() {
				break
			}
			select {
			case <-ctx.Done():
				t.Fatal("reopen readiness:", ctx.Err())
			case <-ticker.C:
			}
		}
	}
	// Reopening uses the same durable stream; it cannot provision an empty one.
	authority, err = OpenNativeAuthority(ctx, js, config.Name, "wf.blob.authority")
	if err != nil {
		t.Fatal(err)
	}
	retired, err := authority.ReadRoot(ctx, "a")
	if err != nil || retired.Head != 3 || retired.Token != "" {
		t.Fatal("retirement authority reset after restart", retired, err)
	}
	if _, err = authority.CASRoot(ctx, "a", 0, Root{Token: "late"}); !errors.Is(err, ErrConflict) {
		t.Fatal("late initial publication admitted after restart", err)
	}
	if _, err = authority.CASBlob(ctx, blobKey, 0, pending.Fence); !errors.Is(err, ErrConflict) {
		t.Fatal("late initial metadata admitted after restart", err)
	}
	next, err := authority.CASBlob(ctx, blobKey, closed.Revision, Fence{Generation: 2, Phase: "uploading", Intents: map[string]Intent{"new": intent}})
	if err != nil || next.Fence.Generation != 2 {
		t.Fatal("generation advance", err)
	}
	info, err := authority.stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	proof := map[string]any{"replicas": replicas, "stream": info, "retired_root": retired, "next_generation": next, "late_wire": wireProof, "scope": "Native durable authority only; no ObjectStore or workflow-stream integration qualification."}
	data, err := json.MarshalIndent(proof, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(directory, "authority-proof.json"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("native authority: root head=%d generation=%d retired old CAS rejected; purge/delete denied; same-store restart retained fences", retired.Head, next.Fence.Generation)
}

// Embedding the native Authority allows testing Retire through the real
// protocol while making any unexpected ObjectStore use fail explicitly.
type authorityOnlyPort struct{ Authority }

func (*authorityOnlyPort) Put(context.Context, string, []byte) error {
	return errors.New("ObjectStore not qualified")
}
func (*authorityOnlyPort) Delete(context.Context, string) error {
	return errors.New("ObjectStore not qualified")
}
func (*authorityOnlyPort) Objects(context.Context) ([]Object, error) {
	return nil, errors.New("ObjectStore not qualified")
}

func TestNativeAuthorityRejectsUnsafeStream(t *testing.T) {
	cluster, err := testcluster.Start(nativeAuthorityDirectory(t), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err = OpenNativeAuthority(ctx, js, "MISSING", "wf.missing"); err == nil {
		t.Fatal("missing authority silently created")
	}
	cases := []struct {
		name   string
		change func(*jetstream.StreamConfig)
	}{
		{"age", func(c *jetstream.StreamConfig) { c.MaxAge = time.Hour }},
		{"message_limit", func(c *jetstream.StreamConfig) { c.MaxMsgs = 10 }},
		{"byte_limit", func(c *jetstream.StreamConfig) { c.MaxBytes = 1 << 20 }},
		{"direct", func(c *jetstream.StreamConfig) { c.AllowDirect = true }},
		{"allow_purge", func(c *jetstream.StreamConfig) { c.DenyPurge = false }},
		{"allow_delete", func(c *jetstream.StreamConfig) { c.DenyDelete = false }},
		{"memory", func(c *jetstream.StreamConfig) { c.Storage = jetstream.MemoryStorage }},
		{"history", func(c *jetstream.StreamConfig) { c.MaxMsgsPerSubject = 2 }},
		{"ttl", func(c *jetstream.StreamConfig) { c.AllowMsgTTL = true }},
	}
	for i, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := AuthorityStreamConfig(fmt.Sprintf("UNSAFE_%d", i), fmt.Sprintf("wf.unsafe%d", i), 1)
			test.change(&c)
			if _, err := js.CreateStream(ctx, c); err != nil {
				t.Fatal(err)
			}
			if _, err := OpenNativeAuthority(ctx, js, c.Name, c.Subjects[0][:len(c.Subjects[0])-2]); err == nil {
				t.Fatal("unsafe configuration accepted")
			}
		})
	}
	c := AuthorityStreamConfig("CHANGING", "wf.changing", 1)
	if _, err = js.CreateStream(ctx, c); err != nil {
		t.Fatal(err)
	}
	authority, err := OpenNativeAuthority(ctx, js, c.Name, "wf.changing")
	if err != nil {
		t.Fatal(err)
	}
	c.MaxAge = time.Hour
	if _, err = js.UpdateStream(ctx, c); err != nil {
		t.Fatal(err)
	}
	if _, err = authority.ReadRoot(ctx, "a"); err == nil {
		t.Fatal("unsafe post-open configuration accepted")
	}
}

// Explicit retained fixtures keep native stores after their servers close.
func nativeAuthorityDirectory(t *testing.T) string {
	t.Helper()
	root := os.Getenv("WF_BLOB_AUTHORITY_ROOT")
	if root == "" {
		return filepath.Join(t.TempDir(), "cluster")
	}
	if !filepath.IsAbs(root) {
		t.Fatal("retained authority root must be absolute")
	}
	directory := filepath.Join(root, strings.ReplaceAll(t.Name(), "/", "-"))
	if err := os.Mkdir(directory, 0700); err != nil {
		t.Fatal(err)
	}
	return directory
}

func nativeLateAuthorityPublication(t *testing.T, ctx context.Context, cluster *testcluster.Cluster, authority *NativeAuthority) map[string]any {
	t.Helper()
	subject := authority.subject("root", "late")
	proxy, err := testcluster.NewClientProxy(cluster.Servers[0].ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	if err = proxy.HoldFirstPublication(subject); err != nil {
		t.Fatal(err)
	}
	if err = proxy.EnableTrafficTrace(1 << 20); err != nil {
		t.Fatal(err)
	}
	connection, err := nats.Connect(proxy.URL(), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	js, err := jetstream.New(connection)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := OpenNativeAuthority(ctx, js, authority.name, authority.prefix)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { _, err := writer.CASRoot(ctx, "late", 0, Root{Token: "late"}); result <- err }()
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
	if held.Subject != subject || held.Disposition != "held" || held.ForwardedBytes != 0 || !bytes.Contains(held.Packet, []byte(jetstream.ExpectedLastSubjSeqHeader+": 0\r\n")) {
		t.Fatal("wrong held publication", held)
	}
	// Near matches must flow while the exact target is held. Metadata requests
	// already passed above; a different raw subject is not fenced by this proxy.
	fenced, err := authority.CASRoot(ctx, "late", 0, Root{})
	if err != nil || fenced.Head != 1 {
		t.Fatal(err)
	}
	proxy.ReleaseFirstAPI()
	select {
	case err = <-result:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if !errors.Is(err, ErrConflict) {
		t.Fatal("server admitted stale wire CAS", err)
	}
	forwarded := proxy.PendingAPI()
	if forwarded.ForwardedBytes != uint64(len(held.Packet)) {
		t.Fatal("held CAS not completely forwarded", forwarded)
	}
	current, err := authority.ReadRoot(ctx, "late")
	if err != nil || current.Head != 1 || current.Token != "" {
		t.Fatal("late writer changed fence", current, err)
	}
	t.Logf("native authority late wire: subject=%s held=%d forwarded=%d stale CAS rejected", subject, len(held.Packet), forwarded.ForwardedBytes)
	return map[string]any{"held": held, "forwarded": forwarded, "fenced_root": fenced, "after_rejection": current, "wire": proxy.TrafficTrace()}
}
