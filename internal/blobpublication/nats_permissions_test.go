package blobpublication

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/testcluster"
)

func TestAuthoritySubjectAccessRejectsInvalidNamespaces(t *testing.T) {
	for _, values := range [][3]string{{"", "wf.auth", "$JS.API"}, {"AUTH.*", "wf.auth", "$JS.API"}, {"AUTH", "wf.>", "$JS.API"}, {"AUTH", "wf.auth", "$JS..API"}, {"AUTH", "wf.auth", "$JS.API.>"}} {
		if _, err := AuthoritySubjectAccess(values[0], values[1], values[2]); err == nil {
			t.Fatal("unsafe permission namespace admitted", values)
		}
	}
}

func TestNativeAuthorityRuntimePermissions(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			directory := nativeAuthorityDirectory(t)
			access, err := AuthoritySubjectAccess("BLOB_AUTH", "wf.blob.authority", "$JS.API")
			if err != nil {
				t.Fatal(err)
			}
			users := []*server.User{
				{Username: "provisioner", Password: "fixture-admin"},
				{Username: "adapter", Password: "fixture-runtime", Permissions: &server.Permissions{
					Publish:   &server.SubjectPermission{Allow: access.Publish},
					Subscribe: &server.SubjectPermission{Allow: access.Subscribe},
				}},
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
			stream, err := admin.CreateStream(ctx, AuthorityStreamConfig("BLOB_AUTH", "wf.blob.authority", replicas))
			if err != nil {
				t.Fatal(err)
			}
			violations := make(chan error, 32)
			nc, err := nats.Connect(cluster.Servers[0].ClientURL(), nats.NoReconnect(), nats.UserInfo("adapter", "fixture-runtime"), nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) { violations <- err }))
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
			published, err := authority.CASRoot(ctx, "workflow", 0, Root{Token: "first", Data: []byte("retained")})
			if err != nil {
				t.Fatal(err)
			}
			observed, err := authority.ReadRoot(ctx, "workflow")
			if err != nil || !reflect.DeepEqual(observed, published) {
				t.Fatal("restricted read witness", observed, err)
			}
			k := key([]byte("retained"))
			blob, err := authority.CASBlob(ctx, k, 0, Fence{Generation: 1, Phase: "uploading", Intents: map[string]Intent{"pending": {Root: "workflow", Expected: published.Head, Expires: time.Now().Add(time.Hour)}}})
			if err != nil {
				t.Fatal(err)
			}
			observedBlob, err := authority.ReadBlob(ctx, k)
			if err != nil || !reflect.DeepEqual(observedBlob, blob) {
				t.Fatal("restricted blob read", observedBlob, err)
			}
			keys, err := authority.BlobKeys(ctx)
			if err != nil || !reflect.DeepEqual(keys, []string{k}) {
				t.Fatal("restricted census", keys, err)
			}
			before, err := stream.Info(ctx)
			if err != nil {
				t.Fatal(err)
			}
			unsafe := before.Config
			unsafe.MaxAge = time.Nanosecond
			configBytes, err := json.Marshal(unsafe)
			if err != nil {
				t.Fatal(err)
			}
			attempts := []struct {
				Subject string
				Data    []byte
			}{
				{"$JS.API.STREAM.UPDATE.BLOB_AUTH", configBytes},
				{"$JS.API.STREAM.DELETE.BLOB_AUTH", []byte(`{}`)},
				{"$JS.API.STREAM.CREATE.BLOB_AUTH", configBytes},
				{"$JS.API.STREAM.PURGE.BLOB_AUTH", []byte(`{}`)},
				{"$JS.API.STREAM.MSG.DELETE.BLOB_AUTH", []byte(fmt.Sprintf(`{"seq":%d}`, before.State.LastSeq))},
				{"$JS.API.STREAM.CREATE.OTHER", []byte(`{"name":"OTHER","subjects":["other.>"]}`)},
				{"$JS.API.STREAM.INFO.OTHER", []byte(`{}`)},
				{"$JS.API.STREAM.MSG.GET.OTHER", []byte(`{"seq":1}`)},
				{"wf.blob.authority.unrecognized.value", []byte(`{}`)},
				{"wf.inv.type.id", []byte(`{}`)},
			}
			denied := map[string]string{}
			for _, attempt := range attempts {
				if err = nc.Publish(attempt.Subject, attempt.Data); err != nil {
					t.Fatal(err)
				}
				if err = nc.FlushWithContext(ctx); err != nil {
					t.Fatal(err)
				}
				select {
				case violation := <-violations:
					if !errors.Is(violation, nats.ErrPermissionViolation) || !strings.Contains(violation.Error(), `"`+attempt.Subject+`"`) {
						t.Fatal("not an exact permission denial", attempt.Subject, violation)
					}
					denied[attempt.Subject] = violation.Error()
				case <-ctx.Done():
					t.Fatal("permission denial not observed", attempt.Subject, ctx.Err())
				}
				after, err := stream.Info(ctx)
				if err != nil || !reflect.DeepEqual(after.Config, before.Config) || after.State.LastSeq != before.State.LastSeq || after.State.Msgs != before.State.Msgs {
					t.Fatal("denied request changed authority", attempt.Subject, after, err)
				}
			}
			// Allowed retirement still advances permanent metadata; forbidden
			// lifecycle operations cannot reset it to admit an old initial writer.
			if err = (Protocol{Port: &authorityOnlyPort{Authority: authority}}).Retire(ctx, "workflow", published.Head); err != nil {
				t.Fatal(err)
			}
			retired, err := authority.ReadRoot(ctx, "workflow")
			if err != nil || retired.Head != 2 || retired.Token != "" {
				t.Fatal("retirement", retired, err)
			}
			if _, err = authority.CASRoot(ctx, "workflow", 0, published); !errors.Is(err, ErrConflict) {
				t.Fatal("initial writer resurrected", err)
			}
			// Separate provisioning credentials really can exercise lifecycle APIs.
			if _, err = admin.CreateStream(ctx, jetstream.StreamConfig{Name: "CONTROL", Subjects: []string{"control.>"}}); err != nil {
				t.Fatal(err)
			}
			if err = admin.DeleteStream(ctx, "CONTROL"); err != nil {
				t.Fatal(err)
			}
			nc.Close()
			cluster.KillNode(0)
			if err = cluster.RestartNode(0); err != nil {
				t.Fatal(err)
			}
			reopened, err := nats.Connect(cluster.Servers[0].ClientURL(), nats.NoReconnect(), nats.UserInfo("adapter", "fixture-runtime"), nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) { violations <- err }))
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			js, err = jetstream.New(reopened)
			if err != nil {
				t.Fatal(err)
			}
			authority, err = OpenNativeAuthority(ctx, js, "BLOB_AUTH", "wf.blob.authority")
			if err != nil {
				t.Fatal(err)
			}
			afterRestart, err := authority.ReadRoot(ctx, "workflow")
			if err != nil || !reflect.DeepEqual(afterRestart, retired) {
				t.Fatal("authenticated same-store reopen", afterRestart, err)
			}
			if err = reopened.Publish("$JS.API.STREAM.DELETE.BLOB_AUTH", []byte(`{}`)); err != nil {
				t.Fatal(err)
			}
			if err = reopened.FlushWithContext(ctx); err != nil {
				t.Fatal(err)
			}
			var restartDenial string
			select {
			case violation := <-violations:
				if !errors.Is(violation, nats.ErrPermissionViolation) || !strings.Contains(violation.Error(), `"$JS.API.STREAM.DELETE.BLOB_AUTH"`) {
					t.Fatal("reopened lifecycle permission", violation)
				}
				restartDenial = violation.Error()
			case <-ctx.Done():
				t.Fatal("reopened denial missing", ctx.Err())
			}
			confirmed, err := authority.ReadRoot(ctx, "workflow")
			if err != nil || !reflect.DeepEqual(confirmed, retired) {
				t.Fatal("denied deletion after reopen", confirmed, err)
			}
			proof := map[string]any{"replicas": replicas, "access": access, "denied": denied, "before": before, "retired": retired, "after_restart": afterRestart, "restart_denial": restartDenial, "blob": blob, "keys": keys, "provisioner_control_deleted": true, "scope": "Named trusted adapter principal cannot call stream lifecycle APIs; provisioning credentials remain privileged. No server administrator threat, ObjectStore permissions, runtime migration or domain routing qualification."}
			data, err := json.MarshalIndent(proof, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(filepath.Join(directory, "authority-permissions-proof.json"), append(data, '\n'), 0600); err != nil {
				t.Fatal(err)
			}
			t.Logf("R%d restricted authority read/write/census/retire passed; %d exact lifecycle/namespace denials, privileged positive control", replicas, len(denied))
		})
	}
}
