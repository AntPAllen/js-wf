package graphpublication

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/internal/retainedgraph"
	"js-wf/testcluster"
)

func TestGraphSubjectAccessRejectsUnsafeNamespaces(t *testing.T) {
	for _, values := range [][4]string{
		{"", "wf.graph", "$JS.API", "GRAPH"}, {"AUTH.*", "wf.graph", "$JS.API", "GRAPH"},
		{"AUTH", "wf.>", "$JS.API", "GRAPH"}, {"AUTH", "wf..graph", "$JS.API", "GRAPH"},
		{"AUTH", "wf.graph", "$JS.API.>", "GRAPH"}, {"AUTH", "wf.graph", "$JS.API", "GRAPH.*"},
		{"AUTH", "wf.graph", "$JS.API", ""}, {"AUTH", "wf.graph", "$JS.API", "GRAPH.name"},
		{"AUTH", "wf.graph", "$JS.API", "GRAPH\nname"}, {"AUTH", "wf.graph", "$JS.API", strings.Repeat("x", 33)},
		{"AUTH", string([]byte{0xff}), "$JS.API", "GRAPH"},
	} {
		if _, err := NativeSubjectAccess(values[0], values[1], values[2], values[3], false); err == nil {
			t.Fatal("unsafe namespace", values)
		}
	}
}

func TestNativeGraphRuntimePermissions(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			users := []*server.User{{Username: "provisioner", Password: "fixture-admin"}}
			for _, name := range []string{"publisher", "collector"} {
				policy, err := NativeSubjectAccess("GRAPH_AUTH", "wf.graph.authority", "$JS.API", "GRAPH_OBJECTS", name == "collector")
				if err != nil {
					t.Fatal(err)
				}
				users = append(users, &server.User{Username: name, Password: "fixture-" + name, Permissions: &server.Permissions{Publish: &server.SubjectPermission{Allow: policy.Publish}, Subscribe: &server.SubjectPermission{Allow: policy.Subscribe}}})
			}
			cluster, err := testcluster.StartWithUsers(t.TempDir(), replicas, users, nats.UserInfo("provisioner", "fixture-admin"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(cluster.Close)
			c, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			if replicas > 1 {
				ticker := time.NewTicker(20 * time.Millisecond)
				defer ticker.Stop()
				for {
					ready := false
					for _, node := range cluster.Servers {
						ready = ready || node.JetStreamIsLeader() && len(node.JetStreamClusterPeers()) == replicas
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
			admin, err := jetstream.New(cluster.Clients[0])
			if err != nil {
				t.Fatal(err)
			}
			authStream, err := admin.CreateStream(c, AuthorityStreamConfig("GRAPH_AUTH", "wf.graph.authority", replicas))
			if err != nil {
				t.Fatal(err)
			}
			objectStream, err := admin.CreateStream(c, NativeObjectStreamConfig("GRAPH_OBJECTS", replicas))
			if err != nil {
				t.Fatal(err)
			}
			type role struct {
				conn       *nats.Conn
				port       *NativePort
				violations chan error
			}
			roles := map[string]role{}
			connect := func(name string) role {
				violations := make(chan error, 64)
				nc, err := nats.Connect(cluster.Servers[0].ClientURL(), nats.NoReconnect(), nats.UserInfo(name, "fixture-"+name), nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) { violations <- err }))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(nc.Close)
				js, err := jetstream.New(nc)
				if err != nil {
					t.Fatal(err)
				}
				authority, err := OpenNativeAuthority(c, js, "GRAPH_AUTH", "wf.graph.authority")
				if err != nil {
					t.Fatal(err)
				}
				port, err := OpenNativePort(c, authority, "GRAPH_OBJECTS")
				if err != nil {
					t.Fatal(err)
				}
				return role{nc, port, violations}
			}
			for _, name := range []string{"publisher", "collector"} {
				roles[name] = connect(name)
			}
			publisher := Protocol{Port: roles["publisher"].port}
			collector := Protocol{Port: roles["collector"].port}
			data := bytes.Repeat([]byte("graph-role-data"), 20000)
			prepared, err := publisher.PrepareAppend(c, "workflow", 0, []byte("first"), [][]byte{data}, time.Now().UTC().Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			root, err := publisher.Commit(c, prepared)
			if err != nil {
				t.Fatal(err)
			}
			record, err := retainedgraph.Read(c, nativeGraphReadStore{roles["publisher"].port}, root.Graph, 0)
			if err != nil || len(record.Blobs) != 1 {
				t.Fatal(record, err)
			}
			owned := record.Blobs[0]
			read, err := roles["publisher"].port.Get(c, owned, len(data))
			if err != nil || !bytes.Equal(read, data) {
				t.Fatal("restricted physical read", err)
			}
			prepared, err = publisher.PrepareAppendWithOwned(c, "workflow", root.Head, []byte("second"), nil, []OwnedPayload{{Index: 0, Link: owned}}, time.Now().UTC().Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			root, err = publisher.Commit(c, prepared)
			if err != nil {
				t.Fatal(err)
			}
			if n, err := collector.Sweep(c, time.Now().Add(2*time.Hour)); err != nil || n != 0 {
				t.Fatal("restricted live preservation", n, err)
			}
			authBefore, err := authStream.Info(c)
			if err != nil {
				t.Fatal(err)
			}
			objectsBefore, err := objectStream.Info(c)
			if err != nil {
				t.Fatal(err)
			}
			denials := 0
			for _, name := range []string{"publisher", "collector"} {
				requests := []string{
					"$JS.API.STREAM.CREATE.GRAPH_AUTH", "$JS.API.STREAM.UPDATE.GRAPH_AUTH", "$JS.API.STREAM.DELETE.GRAPH_AUTH", "$JS.API.STREAM.PURGE.GRAPH_AUTH",
					"$JS.API.STREAM.CREATE.OBJ_GRAPH_OBJECTS", "$JS.API.STREAM.UPDATE.OBJ_GRAPH_OBJECTS", "$JS.API.STREAM.DELETE.OBJ_GRAPH_OBJECTS", "$JS.API.STREAM.MSG.DELETE.OBJ_GRAPH_OBJECTS",
					"$JS.API.STREAM.INFO.OBJ_OTHER", "$JS.API.STREAM.MSG.GET.OBJ_OTHER", "$JS.API.STREAM.PURGE.OBJ_OTHER", "$O.OTHER.C.attempt", "$O.OTHER.M.attempt",
					"wf.other.root.value", "$JS.API.CONSUMER.CREATE.OBJ_GRAPH_OBJECTS.reader.$O.GRAPH_OBJECTS.C.attempt", "$JS.API.CONSUMER.INFO.OBJ_GRAPH_OBJECTS.reader", "$JS.API.CONSUMER.DELETE.OBJ_GRAPH_OBJECTS.reader", "$JS.FC.OBJ_GRAPH_OBJECTS.reply",
				}
				if name == "publisher" {
					requests = append(requests, "$JS.API.STREAM.PURGE.OBJ_GRAPH_OBJECTS")
				}
				for _, subject := range requests {
					r := roles[name]
					if err = r.conn.Publish(subject, []byte(`{}`)); err != nil {
						t.Fatal(err)
					}
					if err = r.conn.FlushWithContext(c); err != nil {
						t.Fatal(err)
					}
					select {
					case violation := <-r.violations:
						if !errors.Is(violation, nats.ErrPermissionViolation) || !strings.Contains(violation.Error(), `"`+subject+`"`) {
							t.Fatal("not exact denial", name, subject, violation)
						}
						denials++
					case <-c.Done():
						t.Fatal("denial missing", c.Err())
					}
				}
			}
			authAfter, err := authStream.Info(c)
			if err != nil || authAfter.State.LastSeq != authBefore.State.LastSeq || authAfter.State.Msgs != authBefore.State.Msgs {
				t.Fatal("denial mutated authority", err)
			}
			objectsAfter, err := objectStream.Info(c)
			if err != nil || objectsAfter.State.LastSeq != objectsBefore.State.LastSeq || objectsAfter.State.Msgs != objectsBefore.State.Msgs || objectsAfter.State.Consumers != 0 {
				t.Fatal("denial mutated objects or physical read created consumer", err)
			}
			// Restart the original native store and reconnect named principals.
			cluster.Servers[0].Shutdown()
			cluster.Servers[0].WaitForShutdown()
			if err = cluster.RestartNode(0); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"publisher", "collector"} {
				roles[name].conn.Close()
				roles[name] = connect(name)
			}
			publisher = Protocol{Port: roles["publisher"].port}
			collector = Protocol{Port: roles["collector"].port}
			persisted, err := roles["publisher"].port.ReadRoot(c, "workflow")
			if err != nil || persisted.Head != root.Head || persisted.Graph.Count != 2 {
				t.Fatal("authenticated restart", persisted, err)
			}
			read, err = roles["publisher"].port.Get(c, owned, len(data))
			if err != nil || !bytes.Equal(read, data) {
				t.Fatal("authenticated post-restart bytes", err)
			}
			if err = collector.Retire(c, "workflow", persisted.Head); err != nil {
				t.Fatal(err)
			}
			if _, err = collector.Sweep(c, time.Now().Add(3*time.Hour)); err != nil {
				t.Fatal(err)
			}
			nativeGraphNoObjects(t, roles["collector"].port, c)
			meta, sequence, err := roles["collector"].port.metadata(c, owned.Reference.Object)
			if err != nil || meta == nil || !meta.Deleted || sequence == 0 {
				t.Fatal("restricted tombstone", err)
			}
			if err = roles["publisher"].port.Put(c, owned.Reference.Object, data); !errors.Is(err, ErrRevoked) {
				t.Fatal("revoked upload", err)
			}
			for _, name := range []string{"publisher", "collector"} {
				select {
				case err := <-roles[name].violations:
					t.Fatal("unexpected role violation", name, err)
				default:
				}
			}
			t.Logf("R%d named roles append/reuse/read/live collection/authenticated restart/retirement/chunk purge passed; exact denials=%d consumers=0", replicas, denials)
		})
	}
}
