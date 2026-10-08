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

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/testcluster"
)

func TestNativeObjectRuntimePermissions(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			directory := nativeAuthorityDirectory(t)
			publisher, err := NativeSubjectAccess("BLOB_AUTH", "wf.blob.authority", "$JS.API", "PROTOCOL", false)
			if err != nil {
				t.Fatal(err)
			}
			collector, err := NativeSubjectAccess("BLOB_AUTH", "wf.blob.authority", "$JS.API", "PROTOCOL", true)
			if err != nil {
				t.Fatal(err)
			}
			users := []*server.User{{Username: "provisioner", Password: "fixture-admin"}}
			for name, access := range map[string]SubjectAccess{"publisher": publisher, "collector": collector} {
				users = append(users, &server.User{Username: name, Password: "fixture-" + name, Permissions: &server.Permissions{Publish: &server.SubjectPermission{Allow: access.Publish}, Subscribe: &server.SubjectPermission{Allow: access.Subscribe}}})
			}
			cluster, err := testcluster.StartWithUsers(directory, replicas, users, nats.UserInfo("provisioner", "fixture-admin"))
			if err != nil {
				t.Fatal(err)
			}
			defer cluster.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if replicas > 1 {
				for {
					ready := false
					for _, node := range cluster.Servers {
						ready = ready || node.JetStreamIsLeader() && len(node.JetStreamClusterPeers()) == replicas
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
			admin, err := jetstream.New(cluster.Clients[0])
			if err != nil {
				t.Fatal(err)
			}
			if _, err = admin.CreateStream(ctx, AuthorityStreamConfig("BLOB_AUTH", "wf.blob.authority", replicas)); err != nil {
				t.Fatal(err)
			}
			objects, err := admin.CreateStream(ctx, NativeObjectStreamConfig("PROTOCOL", replicas))
			if err != nil {
				t.Fatal(err)
			}
			type role struct {
				conn       *nats.Conn
				port       *NativePort
				violations chan error
			}
			roles := map[string]role{}
			for _, name := range []string{"publisher", "collector"} {
				violations := make(chan error, 32)
				nc, err := nats.Connect(cluster.Servers[0].ClientURL(), nats.NoReconnect(), nats.UserInfo(name, "fixture-"+name), nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) { violations <- err }))
				if err != nil {
					t.Fatal(err)
				}
				defer nc.Close()
				js, err := jetstream.New(nc)
				if err != nil {
					t.Fatal(err)
				}
				authority, err := OpenNativeAuthority(ctx, js, "BLOB_AUTH", "wf.blob.authority")
				if err != nil {
					t.Fatal(err)
				}
				port, err := OpenNativePort(ctx, authority, "PROTOCOL")
				if err != nil {
					t.Fatal(err)
				}
				roles[name] = role{nc, port, violations}
			}
			data := bytes.Repeat([]byte("native-role-data"), 20000) // multiple native chunks
			protocol := Protocol{Port: roles["publisher"].port}
			prepared, err := protocol.Prepare(ctx, "workflow", []byte("canonical"), [][]byte{data}, time.Now().Add(time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			published, err := protocol.Commit(ctx, prepared)
			if err != nil {
				t.Fatal(err)
			}
			ref := published.Blobs[key(data)]
			read, err := roles["publisher"].port.GetBytes(ctx, ref.Object)
			if err != nil || !bytes.Equal(read, data) {
				t.Fatal("restricted SDK object read", len(read), err)
			}
			liveSweep, err := (Protocol{Port: roles["collector"].port}).Sweep(ctx, time.Now().Add(time.Hour))
			if err != nil || liveSweep != 0 {
				t.Fatal("live object collected", liveSweep, err)
			}
			before, err := objects.Info(ctx)
			if err != nil {
				t.Fatal(err)
			}
			denied := map[string]string{}
			for _, name := range []string{"publisher", "collector"} {
				requests := []string{"$JS.API.STREAM.DELETE.OBJ_PROTOCOL", "$JS.API.STREAM.UPDATE.OBJ_PROTOCOL", "$JS.API.STREAM.CREATE.OBJ_PROTOCOL", "$JS.API.STREAM.PURGE.BLOB_AUTH", "$JS.API.STREAM.MSG.DELETE.OBJ_PROTOCOL", "$JS.API.STREAM.PURGE.OBJ_OTHER", "$JS.API.STREAM.INFO.OBJ_OTHER", "$O.OTHER.C.attempt", "$JS.API.CONSUMER.CREATE.WF_INV.reader.wf.inv.type.id"}
				if name == "publisher" {
					requests = append(requests, "$JS.API.STREAM.PURGE.OBJ_PROTOCOL")
				}
				for _, subject := range requests {
					r := roles[name]
					if err = r.conn.Publish(subject, []byte(`{}`)); err != nil {
						t.Fatal(err)
					}
					if err = r.conn.FlushWithContext(ctx); err != nil {
						t.Fatal(err)
					}
					select {
					case violation := <-r.violations:
						if !errors.Is(violation, nats.ErrPermissionViolation) || !strings.Contains(violation.Error(), `"`+subject+`"`) {
							t.Fatal("not exact role denial", name, subject, violation)
						}
						denied[name+":"+subject] = violation.Error()
					case <-ctx.Done():
						t.Fatal("role denial missing", name, subject, ctx.Err())
					}
				}
			}
			after, err := objects.Info(ctx)
			if err != nil || after.State.Msgs != before.State.Msgs || after.State.LastSeq != before.State.LastSeq {
				t.Fatal("denied request changed native objects", after, err)
			}
			if err = (Protocol{Port: roles["collector"].port}).Retire(ctx, "workflow", published.Head); err != nil {
				t.Fatal(err)
			}
			deleted, err := (Protocol{Port: roles["collector"].port}).Sweep(ctx, time.Now().Add(time.Hour))
			if err != nil || deleted != 1 {
				t.Fatal("restricted retirement/sweep", deleted, err)
			}
			remaining, err := roles["collector"].port.Objects(ctx)
			if err != nil || len(remaining) != 0 {
				t.Fatal("remaining objects", remaining, err)
			}
			meta, seq, err := roles["collector"].port.metadata(ctx, ref.Object)
			if err != nil || meta == nil || !meta.Deleted || seq == 0 {
				t.Fatal("attempt tombstone lost", meta, seq, err)
			}
			if err = roles["publisher"].port.Put(ctx, ref.Object, data); !errors.Is(err, ErrRevoked) {
				t.Fatal("retired generation resurrected", err)
			}
			proof := map[string]any{"replicas": replicas, "publisher": publisher, "collector": collector, "denied": denied, "published": published, "bytes_read": len(read), "live_sweep_deleted": liveSweep, "retired_sweep_deleted": deleted, "remaining": remaining, "tombstone": meta, "tombstone_sequence": seq, "scope": "Trusted native publisher/collector roles only; collector purge bodies cannot be restricted by NATS subject permissions. No workflow migration, administrator ownership, domains/imports, arbitrary partitions or online GC adoption."}
			encoded, err := json.MarshalIndent(proof, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(filepath.Join(directory, "object-permissions-proof.json"), append(encoded, '\n'), 0600); err != nil {
				t.Fatal(err)
			}
			t.Logf("R%d publisher upload/read and collector live protection/retirement/chunk purge passed; %d exact role denials", replicas, len(denied))
		})
	}
}
