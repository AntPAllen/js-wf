package graphpublication

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type ownerIndexReplyFault struct {
	jetstream.JetStream
	subject string
	commit  bool
	calls   int
}

func (p *ownerIndexReplyFault) PublishMsg(c context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	if msg.Subject == p.subject {
		p.calls++
		if p.commit {
			if _, err := p.JetStream.PublishMsg(c, msg, opts...); err != nil {
				return nil, err
			}
		}
		return nil, lostReply
	}
	return p.JetStream.PublishMsg(c, msg, opts...)
}

type ownerNativeCountPort struct {
	*OwnerIndexedNativePort
	reads, full int
}

func (p *ownerNativeCountPort) ReadBlob(c context.Context, k string) (Record, error) {
	p.reads++
	return p.OwnerIndexedNativePort.ReadBlob(c, k)
}
func (p *ownerNativeCountPort) BlobKeys(c context.Context) ([]string, error) {
	p.full++
	return nil, errors.New("unexpected full namespace enumeration")
}

func TestOwnerIndexedNativeNamespaceAdmission(t *testing.T) {
	legacy := AuthorityStreamConfig("INDEX", "wf.index", 1)
	indexed := OwnerIndexedAuthorityStreamConfig("INDEX", "wf.index", 1)
	for _, mode := range []struct {
		name    string
		indexed bool
		config  jetstream.StreamConfig
		ok      bool
	}{
		{"legacy-on-legacy", false, legacy, true},
		{"indexed-on-indexed", true, indexed, true},
		{"legacy-on-indexed", false, indexed, false},
		{"indexed-on-legacy", true, legacy, false},
	} {
		t.Run(mode.name, func(t *testing.T) {
			p := &NativeAuthority{stream: &graphInfoOverride{config: mode.config}, name: "INDEX", prefix: "wf.index", ownerIndexed: mode.indexed}
			if err := p.validate(ctx); (err == nil) != mode.ok {
				t.Fatal("mode admitted incorrectly", err)
			}
		})
	}
	// Pre-index binaries compare the exact legacy subject shape. Removing the
	// new metadata tag must not make the new namespace compatible with them.
	if reflect.DeepEqual(indexed.Subjects, legacy.Subjects) {
		t.Fatal("old binaries could bypass registration")
	}
	if _, ok := any(&NativePort{}).(OwnerScopePort); ok {
		t.Fatal("legacy port exposes incomplete index")
	}
}

func TestNativeGraphOwnerScopeIndexRecovery(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			cluster, legacy, _ := nativeGraphFixture(t, replicas)
			c, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			js := legacy.js
			if _, err := js.CreateStream(c, OwnerIndexedAuthorityStreamConfig("INDEX_AUTH", "wf.index", replicas)); err != nil {
				t.Fatal(err)
			}
			if _, err := OpenNativeAuthority(c, js, "INDEX_AUTH", "wf.index"); err == nil {
				t.Fatal("legacy opener admitted indexed namespace")
			}
			if _, err := OpenOwnerIndexedNativeAuthority(c, js, "GRAPH_AUTH", "wf.graph.authority"); err == nil {
				t.Fatal("indexed opener admitted legacy namespace")
			}
			a, err := OpenOwnerIndexedNativeAuthority(c, js, "INDEX_AUTH", "wf.index")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = js.CreateStream(c, NativeObjectStreamConfig("INDEX_OBJECTS", replicas)); err != nil {
				t.Fatal(err)
			}
			port, err := OpenOwnerIndexedNativePort(c, a, "INDEX_OBJECTS")
			if err != nil {
				t.Fatal(err)
			}
			p := Protocol{Port: port}
			root := EmptyRoot()
			for i := 0; i < 4; i++ {
				prepared, e := p.PrepareAppend(c, "history", root.Head, []byte(fmt.Sprint("record", i)), nil, time.Now().UTC().Add(time.Hour))
				if e != nil {
					t.Fatal(e)
				}
				root, e = p.Commit(c, prepared)
				if e != nil {
					t.Fatal(e)
				}
			}
			expires := time.Now().UTC().Add(time.Hour)
			plan, err := p.PreparePrefixCompaction(c, "history", root.Head, 2, 1024, expires, nil)
			if err != nil {
				t.Fatal(err)
			}
			token := plan.publication.Token
			// Add a small unrelated owner census. Each starts with a real uploading
			// grant before closure; these are permanent native high-water records.
			for i := 0; i < 16; i++ {
				h := key([]byte(fmt.Sprint("unrelated", i)))
				f := Fence{Hash: h, Owner: "other", Generation: 1, Phase: "uploading", Intents: map[string]Intent{"other": {Destination: "history", Expected: root.Head, Expires: expires, Locations: []Location{{Kind: "payload"}}}}}
				r, e := a.CASBlob(c, authorityKey(h, "other"), 0, f)
				if e != nil {
					t.Fatal(e)
				}
				f.Phase, f.Intents = "closed", nil
				if _, e = a.CASBlob(c, authorityKey(h, "other"), r.Revision, f); e != nil {
					t.Fatal(e)
				}
			}
			absent, uploading := []string{}, []string{}
			for _, mode := range []string{"abandoned", "drop-index", "lost-index", "lost-blob"} {
				h := key([]byte(mode))
				k := authorityKey(h, token)
				f := Fence{Hash: h, Owner: token, Generation: 1, Phase: "uploading", Intents: map[string]Intent{token: {Destination: "history", Expected: root.Head, Expires: expires, Locations: []Location{{Kind: "payload"}}}}}
				base := a.js
				fault := &ownerIndexReplyFault{JetStream: base, subject: a.ownerSubject(token, k), commit: mode != "drop-index"}
				if mode == "lost-blob" {
					fault.subject = a.subject("blob", k)
				}
				if mode != "abandoned" {
					a.js = fault
				}
				_, e := a.CASBlob(c, k, 0, f)
				a.js = base
				if mode == "abandoned" {
					if e != nil {
						t.Fatal(e)
					}
				} else if !errors.Is(e, lostReply) || fault.calls != 1 {
					t.Fatal("unknown write retried/accepted", mode, e, fault.calls)
				}
				r, e := a.ReadBlob(c, k)
				if e != nil {
					t.Fatal(e)
				}
				if strings.HasSuffix(mode, "index") {
					if r.Revision != 0 {
						t.Fatal("blob created without confirmed registration", mode)
					}
					if mode == "lost-index" {
						absent = append(absent, k)
					}
				} else {
					if r.Revision != 1 || r.Fence.Phase != "uploading" {
						t.Fatal("uploading scope missing", mode, r)
					}
					uploading = append(uploading, k)
				}
			}
			before, err := a.ownerScopeKeys(c, token)
			if err != nil {
				t.Fatal(err)
			}
			for i := range cluster.Servers {
				cluster.KillNode(i)
			}
			for i := range cluster.Servers {
				if err = cluster.RestartNode(i); err != nil {
					t.Fatal(err)
				}
			}
			// Wait for metadata quorum within this fixture's original watchdog.
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
			a, err = OpenOwnerIndexedNativeAuthority(c, freshJS, "INDEX_AUTH", "wf.index")
			if err != nil {
				t.Fatal(err)
			}
			port, err = OpenOwnerIndexedNativePort(c, a, "INDEX_OBJECTS")
			if err != nil {
				t.Fatal(err)
			}
			after, err := port.BlobKeysForOwner(c, token)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("index restart mismatch", before, after, err)
			}
			counted := &ownerNativeCountPort{OwnerIndexedNativePort: port}
			p = Protocol{Port: counted}
			renew, err := p.BeginCompactionIntentRenewal(c, plan, func() time.Time { return time.Now().UTC() }, expires.Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			var next PreparedCompaction
			for done := false; !done; {
				next, done, err = renew.Advance(c, 2)
				if err != nil {
					t.Fatal(err)
				}
			}
			if counted.full != 0 || counted.reads != len(after) {
				t.Fatal("namespace scanned", counted.full, counted.reads, len(after))
			}
			for _, k := range uploading {
				r, e := a.ReadBlob(c, k)
				if e != nil || !r.Fence.Intents[token].Expires.Equal(next.expires) {
					t.Fatal("orphan not renewed", k, r, e)
				}
			}
			for _, k := range absent {
				if r, e := a.ReadBlob(c, k); e != nil || r.Revision != 0 {
					t.Fatal("reservation created grant", r, e)
				}
			}
			if current, e := a.ReadRoot(c, "history"); e != nil || !reflect.DeepEqual(root, current) {
				t.Fatal("renewal published", current, e)
			}
			if _, err = a.BlobKeys(c); err != nil {
				t.Fatal("full census rejected index subjects", err)
			}
			if keys, e := a.RootKeys(c); e != nil || !reflect.DeepEqual(keys, []string{"history"}) {
				t.Fatal("root census rejected index subjects", keys, e)
			}
			// Enumeration must return no partial list after a lost marker witness.
			base := a.js
			fault := &ownerIndexReplyFault{JetStream: base, subject: a.ownerSubject(token, after[0]), commit: true}
			a.js = fault
			keys, e := port.BlobKeysForOwner(c, token)
			a.js = base
			if keys != nil || !errors.Is(e, lostReply) || fault.calls != 1 {
				t.Fatal("unknown census witness retried/accepted", keys, e, fault.calls)
			}
			// Index discovery is not a content certificate. The ordinary commit
			// independently verifies the renewed target and its original head.
			renewalReads := counted.reads
			published, e := p.CommitPrefixCompaction(c, next)
			if e != nil || published.Head != root.Head+1 || published.Graph.Count != 2 || selectGraph(published.Graph, published.Streams, PrefixArchiveStream).Count != 2 {
				t.Fatal("renewed plan did not independently commit", published, e)
			}
			t.Logf("NATIVE_OWNER_INDEX replicas=%d discovered=%d blob_reads=%d full_census=%d orphan_uploads=%d absent_reservations=%d restart=all_peers", replicas, len(after), renewalReads, counted.full, len(uploading), len(absent))
		})
	}
}
