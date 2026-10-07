package retention

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/provision"
	"js-wf/testcluster"
)

type boundaryNativeSweep struct {
	BlobSweepPort
	beforeDelete func() error
}

func (p *boundaryNativeSweep) DeleteObject(ctx context.Context, name string) error {
	if p.beforeDelete != nil {
		hook := p.beforeDelete
		p.beforeDelete = nil
		if err := hook(); err != nil {
			return err
		}
	}
	return p.BlobSweepPort.DeleteObject(ctx, name)
}

// Verify the same unconditional-delete boundary against actual JetStream.
// The unsafe case deliberately violates the API's quiescent precondition.
func TestBlobSweepConcurrentRefreshContract(t *testing.T) {
	root := os.Getenv("WF_BLOB_BOUNDARY_ROOT")
	if root == "" {
		t.Skip("set WF_BLOB_BOUNDARY_ROOT to a fresh absolute directory")
	}
	if !filepath.IsAbs(root) {
		t.Fatal("require absolute artifact directory")
	}
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	for _, active := range []bool{false, true} {
		name := "quiescent"
		if active {
			name = "refresh_after_census"
		}
		t.Run(name, func(t *testing.T) {
			cluster, err := testcluster.Start(filepath.Join(root, name), 1)
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
			if err = provision.Ensure(ctx, js, 1); err != nil {
				t.Fatal(err)
			}
			objects, err := js.ObjectStore(ctx, "WF_BLOB")
			if err != nil {
				t.Fatal(err)
			}
			payload := bytes.Repeat([]byte("x"), client.MaxInlineInput+1)
			hash := sha256.Sum256(payload)
			object := "input-" + hex.EncodeToString(hash[:])
			old, err := objects.PutBytes(ctx, object, payload)
			if err != nil {
				t.Fatal(err)
			}
			c := client.New(js)
			var fresh *jetstream.ObjectInfo
			publish := func() error {
				_, e := c.Start(ctx, "gc-native", "input", payload)
				if e != nil {
					return e
				}
				fresh, e = objects.GetInfo(ctx, object)
				return e
			}
			port := &boundaryNativeSweep{BlobSweepPort: &jetStreamBlobSweepPort{js: js, objects: objects, streams: map[string]jetstream.Stream{}}}
			if active {
				port.beforeDelete = publish
			} else if err = publish(); err != nil {
				t.Fatal(err)
			}
			result, err := SweepBlobsQuiescentWithPort(ctx, port, 0, time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}
			inv, err := js.Stream(ctx, "WF_INV")
			if err != nil {
				t.Fatal(err)
			}
			stored, err := inv.GetLastMsgForSubject(ctx, identity.InvocationSubject("gc-native", "input"))
			if err != nil || stored.Header.Get("Wf-Input-Ref") != object {
				t.Fatalf("acked reference missing: %+v err=%v", stored, err)
			}
			if fresh == nil || fresh.NUID == old.NUID {
				t.Fatal("writer did not refresh object generation")
			}
			data, readErr := objects.GetBytes(ctx, object)
			if active {
				if result.Deleted != 1 || !errors.Is(readErr, jetstream.ErrObjectNotFound) {
					t.Fatalf("counterexample absent: result=%+v err=%v", result, readErr)
				}
			} else if result.Deleted != 0 || result.Referenced != 1 || readErr != nil || !bytes.Equal(data, payload) {
				t.Fatalf("quiescent control damaged: result=%+v err=%v", result, readErr)
			}
			proof, e := json.MarshalIndent(map[string]any{"mode": name, "old_nuid": old.NUID, "fresh_nuid": fresh.NUID, "object": object, "acknowledged_reference": stored.Header.Get("Wf-Input-Ref"), "invocation_sequence": stored.Sequence, "sweep": result, "dangling": active, "server_id": cluster.Servers[0].ID(), "scope": "Controlled boundary with quiescence deliberately violated only in active case; not an online collector."}, "", "  ")
			if e != nil {
				t.Fatal(e)
			}
			if e = os.WriteFile(filepath.Join(root, name, "boundary-proof.json"), append(proof, '\n'), 0600); e != nil {
				t.Fatal(e)
			}
			t.Logf("native blob boundary: mode=%s old_nuid=%s fresh_nuid=%s acked_ref=true deleted=%d dangling=%t server_id=%s", name, old.NUID, fresh.NUID, result.Deleted, active, cluster.Servers[0].ID())
		})
	}
}
