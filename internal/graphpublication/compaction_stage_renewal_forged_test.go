package graphpublication

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"js-wf/internal/retainedgraph"
)

func TestGraphCompactionStageRenewalDoesNotCertifyForgedPrefix(t *testing.T) {
	for _, mode := range []string{"data", "payload"} {
		t.Run(mode, func(t *testing.T) {
			m, p, root := stageFixture(t, "renew-forged", 4)
			oldExpiry := epoch.Add(time.Minute)
			stage, err := p.BeginPrefixCompaction(ctx, "owner", root.Head, 2, 1024, oldExpiry, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, done, err := stage.Advance(ctx, 1); err != nil || done {
				t.Fatal(done, err)
			}
			var v compactionCheckpoint
			if err = json.Unmarshal(stageCheckpoint(t, stage), &v); err != nil {
				t.Fatal(err)
			}
			record, err := retainedgraph.Read(ctx, stageStore{protocol: p}, selectGraph(v.Publication.Graph, v.Publication.Streams, PrefixArchiveStream), 0)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "data" {
				record.Data = []byte("forged-record")
			} else {
				data := []byte("forged-payload")
				ref, err := p.acquire(ctx, key(data), data, v.Publication.Token, Intent{Destination: "owner", Expected: root.Head, Expires: v.Expires, Locations: []Location{{Kind: "payload", First: 0, Stream: PrefixArchiveStream}}})
				if err != nil {
					t.Fatal(err)
				}
				record.Blobs = []retainedgraph.Link{{Hash: key(data), Reference: ref}}
			}
			archive, err := retainedgraph.Append(ctx, stageStore{protocol: p, destination: "owner", expected: root.Head, expires: oldExpiry, token: v.Publication.Token, stream: PrefixArchiveStream}, retainedgraph.Empty(), record)
			if err != nil {
				t.Fatal(err)
			}
			setStream(&v.Publication, PrefixArchiveStream, archive)
			data, err := json.Marshal(v)
			if err != nil {
				t.Fatal(err)
			}
			stage, err = p.ResumePrefixCompaction(ctx, data)
			if err != nil {
				t.Fatal("valid staging shape rejected before verification", err)
			}
			renewal, err := stage.BeginIntentRenewal(ctx, func() time.Time { return epoch.Add(20 * time.Second) }, epoch.Add(3*time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			for {
				done, err := renewal.Advance(ctx, 2)
				if err != nil {
					t.Fatal(err)
				}
				if done {
					break
				}
			}
			// Full renewal protects grants, including discarded originals; it
			// never establishes source equivalence for the forged target prefix.
			if _, err = p.SweepWithReaders(ctx, oldExpiry.Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(root, m.roots["owner"]) {
				t.Fatal("renewed orphan fenced source")
			}
			plan, done, err := stage.Advance(ctx, 4)
			if err != nil || !done {
				t.Fatal(done, err)
			}
			if _, err = p.CommitPrefixCompaction(ctx, plan); err == nil || !reflect.DeepEqual(root, m.roots["owner"]) {
				t.Fatal("renewal certified forged prefix", err)
			}
			t.Logf("RENEWED_FORGED_PREFIX mode=%s examined=%d renewed=%d commit_rejected=true root_unchanged=true", mode, renewal.ExaminedScopes(), renewal.RenewedScopes())
		})
	}
}
