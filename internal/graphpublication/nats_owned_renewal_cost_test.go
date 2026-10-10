package graphpublication

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type ownedRenewalNativePort struct {
	*OwnerIndexedNativePort
	roots, blobs, writes, witnesses uint64
}

func (p *ownedRenewalNativePort) ReadRoot(c context.Context, k string) (Root, error) {
	p.roots++
	return p.OwnerIndexedNativePort.ReadRoot(c, k)
}
func (p *ownedRenewalNativePort) ReadBlob(c context.Context, k string) (Record, error) {
	p.blobs++
	return p.OwnerIndexedNativePort.ReadBlob(c, k)
}
func (p *ownedRenewalNativePort) CASBlob(c context.Context, k string, r uint64, f Fence) (Record, error) {
	p.writes++
	return p.OwnerIndexedNativePort.CASBlob(c, k, r, f)
}
func (p *ownedRenewalNativePort) ValidateOwnerScope(c context.Context, o, k string) error {
	p.witnesses++
	return p.OwnerIndexedNativePort.ValidateOwnerScope(c, o, k)
}
func (p *ownedRenewalNativePort) BlobKeys(context.Context) ([]string, error) {
	return nil, errors.New("full namespace discovery forbidden")
}

type ownedRenewalLostBlobJS struct {
	jetstream.JetStream
	prefix string
	calls  int
}

func (p *ownedRenewalLostBlobJS) PublishMsg(c context.Context, m *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	if strings.HasPrefix(m.Subject, p.prefix+".blob.") {
		p.calls++
		if _, err := p.JetStream.PublishMsg(c, m, opts...); err != nil {
			return nil, err
		}
		return nil, lostReply
	}
	return p.JetStream.PublishMsg(c, m, opts...)
}

// This is a native grant-cost fixture, not a full-entry or OS-kill gate.
// Large opt-in runs retain the same 3s setup and 15s batch contexts.
func TestNativeGraphOwnedGrantRenewalCostAndRecovery(t *testing.T) {
	count := 1000
	lifetime := time.Hour
	if raw := os.Getenv("WF_GRAPH_NATIVE_COMPACTION_LIFETIME"); raw != "" {
		var err error
		lifetime, err = time.ParseDuration(raw)
		if err != nil || lifetime < time.Hour || lifetime > 6*time.Hour {
			t.Fatal("native fixture lifetime must be 1h..6h")
		}
	}
	if raw := os.Getenv("WF_GRAPH_NATIVE_OWNED_GRANTS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1000 || n > 100000 {
			t.Fatal("grant count must be 1000..100000")
		}
		count = n
	}
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			cluster, legacy, _ := nativeGraphFixture(t, replicas)
			c, cancel := context.WithTimeout(context.Background(), max(20*time.Minute, lifetime/2))
			defer cancel()
			js := legacy.js
			if _, err := js.CreateStream(c, OwnerIndexedAuthorityStreamConfig("COST_AUTH", "wf.cost", replicas)); err != nil {
				t.Fatal(err)
			}
			if _, err := js.CreateStream(c, NativeObjectStreamConfig("COST_OBJECTS", replicas)); err != nil {
				t.Fatal(err)
			}
			open := func(js jetstream.JetStream) (*NativeAuthority, *OwnerIndexedNativePort) {
				t.Helper()
				a, err := OpenOwnerIndexedNativeAuthority(c, js, "COST_AUTH", "wf.cost")
				if err != nil {
					t.Fatal(err)
				}
				p, err := OpenOwnerIndexedNativePort(c, a, "COST_OBJECTS")
				if err != nil {
					t.Fatal(err)
				}
				return a, p
			}
			a, port := open(js)
			p := Protocol{Port: port}
			root := EmptyRoot()
			for i := 0; i < 4; i++ {
				prepared, err := p.PrepareAppend(c, "history", root.Head, []byte(fmt.Sprint(i)), nil, time.Now().UTC().Add(time.Hour))
				if err != nil {
					t.Fatal(err)
				}
				root, err = p.Commit(c, prepared)
				if err != nil {
					t.Fatal(err)
				}
			}
			expires := time.Now().UTC().Add(lifetime)
			stage, err := p.BeginPrefixCompaction(c, "history", root.Head, 2, 1024, expires, nil)
			if err != nil {
				t.Fatal(err)
			}
			for done := false; !done; {
				_, done, err = stage.Advance(c, 2)
				if err != nil {
					t.Fatal(err)
				}
			}
			token := stage.prepared.publication.Token
			keys, err := port.BlobKeysForOwner(c, token)
			if err != nil {
				t.Fatal(err)
			}
			initial := len(keys)
			provisionStart := time.Now()
			for i := 0; i < count; i++ {
				h := key([]byte(fmt.Sprint("owned-cost-orphan", i)))
				k := authorityKey(h, token)
				f := Fence{Hash: h, Owner: token, Generation: 1, Phase: "uploading", Intents: map[string]Intent{token: {Destination: "history", Expected: root.Head, Expires: expires, Locations: []Location{{Kind: "payload"}}}}}
				if _, err = a.CASBlob(c, k, 0, f); err != nil {
					t.Fatal("fixture grant", i, err)
				}
				keys = append(keys, k)
				if (i+1)%1000 == 0 {
					t.Logf("NATIVE_OWNED_INPUT replicas=%d grants=%d elapsed=%s", replicas, i+1, time.Since(provisionStart))
				}
			}
			provisionWall := time.Since(provisionStart)
			saved, err := stage.Checkpoint()
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "stage.json")
			if err = os.WriteFile(path, saved, 0600); err != nil {
				t.Fatal(err)
			}
			target := expires.Add(lifetime)
			measured := &ownedRenewalNativePort{OwnerIndexedNativePort: port}
			stage.protocol.Port = measured
			start := time.Now()
			remaining := time.Until(expires)
			setup, stop := context.WithTimeout(c, 3*time.Second)
			renew, err := stage.BeginIntentRenewal(setup, func() time.Time { return time.Now().UTC() }, target)
			stop()
			if err != nil {
				t.Fatal(err)
			}
			batch, stop := context.WithTimeout(c, 15*time.Second)
			done, err := renew.Advance(batch, 128)
			stop()
			if err != nil || done || renew.RenewedScopes() != 128 {
				t.Fatal("first batch", done, err, renew.RenewedScopes())
			}
			fault := &ownedRenewalLostBlobJS{JetStream: a.js, prefix: a.prefix}
			a.js = fault
			batch, stop = context.WithTimeout(c, 15*time.Second)
			done, err = renew.Advance(batch, 128)
			stop()
			a.js = fault.JetStream
			if done || !errors.Is(err, lostReply) || fault.calls != 1 {
				t.Fatal("lost committed grant", done, err, fault.calls)
			}
			before := *measured
			if done, e := renew.Advance(c, 128); done || e != err || !reflect.DeepEqual(before, *measured) {
				t.Fatal("failed renewal retried", done, e)
			}
			if _, e := stage.Checkpoint(); e == nil {
				t.Fatal("failed stage emitted new input")
			}
			partial := renew.RenewedScopes()
			stage = nil
			renew = nil
			for i := range cluster.Servers {
				cluster.KillNode(i)
			}
			for i := range cluster.Servers {
				if err = cluster.RestartNode(i); err != nil {
					t.Fatal(err)
				}
			}
			for {
				ready := replicas == 1
				for _, s := range cluster.Servers {
					ready = ready || (s.JetStreamIsLeader() && len(s.JetStreamClusterPeers()) == replicas)
				}
				if ready {
					break
				}
				select {
				case <-time.After(20 * time.Millisecond):
				case <-c.Done():
					t.Fatal(c.Err())
				}
			}
			freshJS, err := jetstream.New(cluster.Clients[0])
			if err != nil {
				t.Fatal(err)
			}
			a, port = open(freshJS)
			fresh := &ownedRenewalNativePort{OwnerIndexedNativePort: port}
			p = Protocol{Port: fresh}
			saved, err = os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			stage, err = p.ResumePrefixCompaction(c, saved)
			if err != nil {
				t.Fatal("fresh portable stage", err)
			}
			setup, stop = context.WithTimeout(c, 3*time.Second)
			renew, err = stage.BeginIntentRenewal(setup, func() time.Time { return time.Now().UTC() }, target)
			stop()
			if err != nil {
				t.Fatal(err)
			}
			batches := 0
			var maxBatch time.Duration
			for done = false; !done; {
				previous := renew.ExaminedScopes()
				batch, stop = context.WithTimeout(c, 15*time.Second)
				tick := time.Now()
				done, err = renew.Advance(batch, 128)
				wall := time.Since(tick)
				stop()
				batches++
				maxBatch = max(maxBatch, wall)
				if err != nil || renew.ExaminedScopes()-previous > 128 {
					t.Fatal("renew batch", batches, renew.ExaminedScopes(), err)
				}
				if batches%100 == 0 {
					t.Logf("NATIVE_OWNED_PROGRESS replicas=%d examined=%d renewed=%d elapsed=%s", replicas, renew.ExaminedScopes(), renew.RenewedScopes(), time.Since(start))
				}
			}
			elapsed := time.Since(start)
			roots, reads := measured.roots+fresh.roots, measured.blobs+fresh.blobs
			writes, witnesses := measured.writes+fresh.writes, measured.witnesses+fresh.witnesses
			if elapsed >= min(remaining, lifetime/3) || renew.RenewedScopes() != uint64(len(keys)) || renew.ExaminedScopes() != uint64(len(keys)+1) {
				t.Fatal("incomplete renewal", elapsed, remaining, renew.RenewedScopes(), len(keys))
			}
			for _, k := range keys {
				r, e := a.ReadBlob(c, k)
				if e != nil || !r.Fence.Intents[token].Expires.Equal(target) {
					t.Fatal("grant audit", k, e)
				}
			}
			if r, e := a.ReadBlob(c, ownerScanBarrierKey(token)); e != nil || r.Revision != 0 {
				t.Fatal("boundary became a grant", e, r.Revision)
			}
			if r, e := a.ReadRoot(c, "history"); e != nil || !reflect.DeepEqual(r, root) {
				t.Fatal("renewal published source", e)
			}
			prepared, done, err := stage.Advance(c, 2)
			if err != nil || !done {
				t.Fatal("renewed stage", done, err)
			}
			published, err := p.CommitPrefixCompaction(c, prepared)
			if err != nil || published.Head != root.Head+1 || published.Graph.Count != 2 || selectGraph(published.Graph, published.Streams, PrefixArchiveStream).Count != 2 {
				t.Fatal("independent final commit", err, published.Head)
			}
			t.Logf("NATIVE_OWNED_COST replicas=%d orphan_grants=%d initial_grants=%d total_grants=%d examined=%d renewed=%d partial=%d batches=%d provision_wall=%s recovery_renewal_wall=%s original_remaining=%s max_batch=%s roots=%d blob_reads=%d blob_writes=%d marker_witnesses=%d restarted=all_peers lost_ack=committed portable_input=true actual_100000_entries=false", replicas, count, initial, len(keys), renew.ExaminedScopes(), renew.RenewedScopes(), partial, batches, provisionWall, elapsed, remaining, maxBatch, roots, reads, writes, witnesses)
		})
	}
}
