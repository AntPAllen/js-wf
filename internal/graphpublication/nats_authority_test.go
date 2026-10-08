package graphpublication

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/internal/blobpublication"
	"js-wf/testcluster"
)

func nativeGraphFixture(t *testing.T, replicas int) (*testcluster.Cluster, *NativeAuthority, context.Context) {
	t.Helper()
	cluster, err := testcluster.Start(t.TempDir(), replicas)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	c, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	if replicas > 1 {
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			ready := false
			for _, s := range cluster.Servers {
				ready = ready || (s.JetStreamIsLeader() && len(s.JetStreamClusterPeers()) == replicas)
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
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err = js.CreateStream(c, AuthorityStreamConfig("GRAPH_AUTH", "wf.graph.authority", replicas)); err != nil {
		t.Fatal(err)
	}
	a, err := OpenNativeAuthority(c, js, "GRAPH_AUTH", "wf.graph.authority")
	if err != nil {
		t.Fatal(err)
	}
	return cluster, a, c
}

// Native servers execute all writes; this is component metadata coverage.
// There is no native graph object adapter or canonical runtime migration here.
func TestNativeGraphAuthorityPersistenceAndSchemaIsolation(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			cluster, a, c := nativeGraphFixture(t, replicas)
			absent, err := a.ReadRoot(c, "root")
			if err != nil || !reflect.DeepEqual(absent, EmptyRoot()) {
				t.Fatal(absent, err)
			}
			raw, err := a.stream.GetLastMsgForSubject(c, a.subject("root", "root"))
			if err != nil || raw.Sequence == 0 {
				t.Fatal(raw, err)
			}
			if raw.Header.Get("Wf-Authority-Read-Witness") != "1" {
				t.Fatal("read was not reaffirmed")
			}
			absenceSequence := raw.Sequence
			_, modelProtocol := newModel("native-root")
			prepared, err := modelProtocol.PrepareAppend(c, "root", 0, []byte("retained"), nil, time.Now().UTC().Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			// The graph bytes are memory fixture bytes: this tests native metadata only.
			first, err := a.CASRoot(c, "root", 0, prepared.publication)
			if err != nil || first.Head != 1 {
				t.Fatal(first, err)
			}
			if _, err = a.CASRoot(c, "other", 0, EmptyRoot()); err != nil {
				t.Fatal(err)
			}
			fenced, err := a.CASRoot(c, "root", 1, first)
			if err != nil || fenced.Head != 2 {
				t.Fatal(fenced, err)
			}
			if _, err = a.CASRoot(c, "root", 1, EmptyRoot()); !errors.Is(err, ErrConflict) {
				t.Fatal("stale root", err)
			}
			raw, err = a.stream.GetLastMsgForSubject(c, a.subject("root", "root"))
			if err != nil || raw.Sequence <= absenceSequence || raw.Sequence == fenced.Head {
				t.Fatal("logical and physical sequence fixture", raw, err)
			}
			if err = a.stream.Purge(c); err == nil {
				t.Fatal("purge allowed")
			}
			if err = a.stream.DeleteMsg(c, raw.Sequence); err == nil {
				t.Fatal("delete allowed")
			}

			hash, owner := key([]byte("payload")), "owner"
			scope := authorityKey(hash, owner)
			absentBlob, err := a.ReadBlob(c, scope)
			if err != nil || absentBlob.Revision != 0 {
				t.Fatal(absentBlob, err)
			}
			f := Fence{Hash: hash, Owner: owner, Generation: 1, Phase: "uploading", Intents: map[string]Intent{owner: {Destination: "root", Expected: 2, Expires: time.Now().UTC().Add(time.Hour), Locations: []Location{{Kind: "payload", First: 0}}}}}
			pending, err := a.CASBlob(c, scope, 0, f)
			if err != nil {
				t.Fatal(err)
			}
			selected := cloneFence(pending.Fence)
			selected.Phase = "ready"
			selected.Object = objectName(hash, 1, owner, "upload")
			ready, err := a.CASBlob(c, scope, pending.Revision, selected)
			if err != nil {
				t.Fatal(err)
			}
			selected.Object = objectName(hash, 1, owner, "other")
			if _, err = a.CASBlob(c, scope, ready.Revision, selected); err == nil {
				t.Fatal("selection changed")
			}
			closed := Fence{Hash: hash, Owner: owner, Generation: 1, Phase: "closed"}
			terminal, err := a.CASBlob(c, scope, ready.Revision, closed)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = a.CASBlob(c, scope, terminal.Revision, pending.Fence); err == nil {
				t.Fatal("generation reopened")
			}
			f.Generation = 2
			reopened, err := a.CASBlob(c, scope, terminal.Revision, f)
			if err != nil || reopened.Fence.Generation != 2 {
				t.Fatal(reopened, err)
			}
			// Returned maps and slices cannot mutate committed metadata.
			reopened.Fence.Intents[owner] = Intent{}
			again, err := a.ReadBlob(c, scope)
			if err != nil || len(again.Fence.Intents[owner].Locations) != 1 {
				t.Fatal("copy isolation", again, err)
			}
			keys, err := a.BlobKeys(c)
			if err != nil || !reflect.DeepEqual(keys, []string{scope}) {
				t.Fatal(keys, err)
			}

			legacy, err := blobpublication.OpenNativeAuthority(c, a.js, a.name, a.prefix)
			if err != nil {
				t.Fatal(err)
			}
			before, _ := a.stream.GetLastMsgForSubject(c, a.subject("root", "root"))
			if _, err = legacy.ReadRoot(c, "root"); err == nil {
				t.Fatal("legacy accepted graph root")
			}
			after, _ := a.stream.GetLastMsgForSubject(c, a.subject("root", "root"))
			if before.Sequence != after.Sequence {
				t.Fatal("legacy rewrote graph")
			}
			if _, err = legacy.CASRoot(c, "legacy", 0, blobpublication.Root{Token: "old", Data: []byte("legacy")}); err != nil {
				t.Fatal(err)
			}
			before, _ = a.stream.GetLastMsgForSubject(c, a.subject("root", "legacy"))
			if _, err = a.ReadRoot(c, "legacy"); err == nil {
				t.Fatal("graph accepted legacy")
			}
			after, _ = a.stream.GetLastMsgForSubject(c, a.subject("root", "legacy"))
			if before.Sequence != after.Sequence {
				t.Fatal("graph rewrote legacy")
			}

			cluster.Servers[0].Shutdown()
			cluster.Servers[0].WaitForShutdown()
			if err = cluster.RestartNode(0); err != nil {
				t.Fatal(err)
			}
			// RestartNode replaces the pinned client; reopen on its new connection.
			reopenedJS, err := jetstream.New(cluster.Clients[0])
			if err != nil {
				t.Fatal(err)
			}
			reopenedAuthority, err := OpenNativeAuthority(c, reopenedJS, a.name, a.prefix)
			if err != nil {
				t.Fatal(err)
			}
			root, err := reopenedAuthority.ReadRoot(c, "root")
			if err != nil || root.Head != 2 || !reflect.DeepEqual(root.Graph, first.Graph) {
				t.Fatal("persisted head", root, err)
			}
			record, err := reopenedAuthority.ReadBlob(c, scope)
			if err != nil || record.Revision != 4 || record.Fence.Generation != 2 {
				t.Fatal("persisted generation", record, err)
			}
			canceled, stop := context.WithCancel(c)
			stop()
			if _, err = reopenedAuthority.ReadRoot(canceled, "root"); !errors.Is(err, context.Canceled) {
				t.Fatal("cancellation", err)
			}
		})
	}
}

type staleGraphRead struct {
	jetstream.Stream
	subject string
	raw     *jetstream.RawStreamMsg
	calls   int
	always  bool
}

func (s *staleGraphRead) GetLastMsgForSubject(c context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	if subject != s.subject {
		return s.Stream.GetLastMsgForSubject(c, subject)
	}
	s.calls++
	if s.calls == 1 || s.always {
		if s.raw == nil {
			return nil, jetstream.ErrMsgNotFound
		}
		copy := *s.raw
		copy.Data = bytes.Clone(s.raw.Data)
		return &copy, nil
	}
	return s.Stream.GetLastMsgForSubject(c, subject)
}

type lostGraphWitness struct {
	jetstream.JetStream
	writes int
}

func (s *lostGraphWitness) PublishMsg(c context.Context, m *nats.Msg, o ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	ack, err := s.JetStream.PublishMsg(c, m, o...)
	if err == nil && m.Header.Get("Wf-Authority-Read-Witness") == "1" {
		s.writes++
		return nil, context.DeadlineExceeded
	}
	return ack, err
}
func TestNativeGraphAuthorityReadWitness(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			_, a, c := nativeGraphFixture(t, replicas)
			first, err := a.CASRoot(c, "root", 0, EmptyRoot())
			if err != nil {
				t.Fatal(err)
			}
			raw, err := a.stream.GetLastMsgForSubject(c, a.subject("root", "root"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err = a.CASRoot(c, "root", first.Head, first); err != nil {
				t.Fatal(err)
			}
			for _, absence := range []bool{false, true} {
				stale := &staleGraphRead{Stream: a.stream, subject: raw.Subject, raw: raw}
				if absence {
					stale.raw = nil
				}
				wrapped := *a
				wrapped.stream = stale
				root, err := wrapped.ReadRoot(c, "root")
				if err != nil || root.Head != 2 || stale.calls != 2 {
					t.Fatal("tentative root/absence", root, err, stale.calls)
				}
				stale.calls = 0
				stale.always = true
				root, err = wrapped.ReadRoot(c, "root")
				if !errors.Is(err, ErrConflict) || root.Head != 0 || stale.calls != 16 {
					t.Fatal("unbounded or accepted stale read", root, err, stale.calls)
				}
			}
			hash, owner := key([]byte("witness-payload")), "witness"
			scope := authorityKey(hash, owner)
			f := Fence{Hash: hash, Owner: owner, Generation: 1, Phase: "uploading", Intents: map[string]Intent{owner: {Destination: "root", Expected: 2, Expires: time.Now().UTC().Add(time.Hour), Locations: []Location{{Kind: "payload", First: 0}}}}}
			if _, err = a.CASBlob(c, scope, 0, f); err != nil {
				t.Fatal(err)
			}
			blobRaw, err := a.stream.GetLastMsgForSubject(c, a.subject("blob", scope))
			if err != nil {
				t.Fatal(err)
			}
			f.Phase = "closed"
			f.Intents = nil
			if _, err = a.CASBlob(c, scope, 1, f); err != nil {
				t.Fatal(err)
			}
			for _, absence := range []bool{false, true} {
				stale := &staleGraphRead{Stream: a.stream, subject: blobRaw.Subject, raw: blobRaw}
				if absence {
					stale.raw = nil
				}
				wrapped := *a
				wrapped.stream = stale
				record, err := wrapped.ReadBlob(c, scope)
				if err != nil || record.Revision != 2 || record.Fence.Phase != "closed" || stale.calls != 2 {
					t.Fatal("stale grant/absence accepted", record, err, stale.calls)
				}
				stale.calls = 0
				stale.always = true
				record, err = wrapped.ReadBlob(c, scope)
				if !errors.Is(err, ErrConflict) || record.Revision != 0 || stale.calls != 16 {
					t.Fatal("stale grant bound", record, err, stale.calls)
				}
			}

			lost := &lostGraphWitness{JetStream: a.js}
			wrapped := *a
			wrapped.js = lost
			before, _ := a.stream.GetLastMsgForSubject(c, raw.Subject)
			root, err := wrapped.ReadRoot(c, "root")
			if !errors.Is(err, context.DeadlineExceeded) || root.Schema != "" || lost.writes != 1 {
				t.Fatal("lost ack accepted/retried", root, err, lost.writes)
			}
			record, err := wrapped.ReadBlob(c, scope)
			if !errors.Is(err, context.DeadlineExceeded) || record.Revision != 0 || lost.writes != 2 {
				t.Fatal("lost grant witness accepted/retried", record, err, lost.writes)
			}

			after, _ := a.stream.GetLastMsgForSubject(c, raw.Subject)
			if after.Sequence <= before.Sequence {
				t.Fatal("lost reply did not commit")
			}
		})
	}
}

func TestGraphAuthorityCanonicalEnvelope(t *testing.T) {
	root := EmptyRoot()
	root.Head = 1
	good, _ := json.Marshal(authorityValue{Schema: authoritySchema, Kind: "root", Identity: "root", Revision: 1, Root: &root})
	if _, err := decodeAuthority(good, "root", "root"); err != nil {
		t.Fatal(err)
	}
	mutants := [][]byte{
		append(bytes.Clone(good), ' '), append(bytes.Clone(good), []byte("{}")...),
		bytes.Replace(good, []byte(`"revision":1`), []byte(`"revision":1,"revision":1`), 1),
		bytes.Replace(good, []byte(`"schema":`), []byte(`"Schema":`), 1),
		bytes.Replace(good, []byte(authoritySchema), []byte("js-wf-blob-authority-v2"), 1),
		bytes.Replace(good, []byte(`"revision":1`), []byte(`"revision":0`), 1),
		bytes.Replace(good, []byte(`"identity":"root"`), []byte(`"identity":"foreign"`), 1),
		[]byte(strings.Repeat(" ", MaxAuthorityBytes+1)),
	}
	// Root fields use Go's exported names; case aliases must still be rejected.
	mutants = append(mutants, bytes.Replace(good, []byte(`"Head":1`), []byte(`"head":1`), 1))
	for i, m := range mutants {
		if bytes.Equal(m, good) {
			t.Fatalf("mutant %d did not change input", i)
		}
		if _, err := decodeAuthority(m, "root", "root"); err == nil {
			t.Fatalf("mutant %d accepted", i)
		}
	}
	if err := validateAuthority(authorityValue{Schema: authoritySchema, Kind: "foreign", Identity: "x"}, "foreign", "x"); err == nil {
		t.Fatal("unknown kind absence accepted")
	}
}

type graphInfoOverride struct {
	jetstream.Stream
	config jetstream.StreamConfig
}

func (s *graphInfoOverride) Info(context.Context, ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
	return &jetstream.StreamInfo{Config: s.config}, nil
}
func TestNativeGraphAuthorityUnsafeConfiguration(t *testing.T) {
	// Exercise the entire permanent-retention contract without provisioning unsafe
	// storage. Every mutation must be rejected before a GET or publication.
	good := AuthorityStreamConfig("GRAPH_AUTH", "wf.graph.authority", 1)
	mutations := map[string]func(*jetstream.StreamConfig){
		"storage":     func(c *jetstream.StreamConfig) { c.Storage = jetstream.MemoryStorage },
		"age":         func(c *jetstream.StreamConfig) { c.MaxAge = time.Hour },
		"bytes":       func(c *jetstream.StreamConfig) { c.MaxBytes = 1024 },
		"messages":    func(c *jetstream.StreamConfig) { c.MaxMsgs = 10 },
		"subjects":    func(c *jetstream.StreamConfig) { c.Subjects = []string{"foreign.>"} },
		"per_subject": func(c *jetstream.StreamConfig) { c.MaxMsgsPerSubject = 2 },
		"purge":       func(c *jetstream.StreamConfig) { c.DenyPurge = false },
		"delete":      func(c *jetstream.StreamConfig) { c.DenyDelete = false },
		"direct":      func(c *jetstream.StreamConfig) { c.AllowDirect = true },
		"ttl":         func(c *jetstream.StreamConfig) { c.AllowMsgTTL = true },
		"marker":      func(c *jetstream.StreamConfig) { c.SubjectDeleteMarkerTTL = time.Minute },
		"rollup":      func(c *jetstream.StreamConfig) { c.AllowRollup = true },
		"noack":       func(c *jetstream.StreamConfig) { c.NoAck = true },
		"sealed":      func(c *jetstream.StreamConfig) { c.Sealed = true },
		"mirror":      func(c *jetstream.StreamConfig) { c.Mirror = &jetstream.StreamSource{Name: "foreign"} },
		"source":      func(c *jetstream.StreamConfig) { c.Sources = []*jetstream.StreamSource{{Name: "foreign"}} },
		"transform": func(c *jetstream.StreamConfig) {
			c.SubjectTransform = &jetstream.SubjectTransformConfig{Source: "x", Destination: "y"}
		},
		"republish":       func(c *jetstream.StreamConfig) { c.RePublish = &jetstream.RePublish{Source: "x", Destination: "y"} },
		"replicas":        func(c *jetstream.StreamConfig) { c.Replicas = 0 },
		"retention":       func(c *jetstream.StreamConfig) { c.Retention = jetstream.WorkQueuePolicy },
		"discard":         func(c *jetstream.StreamConfig) { c.Discard = jetstream.DiscardNew },
		"subject_discard": func(c *jetstream.StreamConfig) { c.DiscardNewPerSubject = true },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			config := good
			mutate(&config)
			a := &NativeAuthority{stream: &graphInfoOverride{config: config}, name: good.Name, prefix: "wf.graph.authority"}
			if _, err := a.ReadRoot(context.Background(), "root"); err == nil {
				t.Fatal("unsafe config accepted")
			}
		})
	}
}

func TestNativeGraphAuthorityRejectsMalformedWireWithoutRepair(t *testing.T) {
	_, a, c := nativeGraphFixture(t, 1)
	root := EmptyRoot()
	root.Head = 1
	good, _ := json.Marshal(authorityValue{Schema: authoritySchema, Kind: "root", Identity: "root", Revision: 1, Root: &root})
	for _, raw := range [][]byte{
		bytes.Replace(good, []byte(`"revision":1`), []byte(`"revision":1,"revision":1`), 1),
		bytes.Replace(good, []byte(`"frontier":[]`), []byte(`"frontier":null`), 1),
		append(bytes.Clone(good), []byte(` {}`)...),
	} {
		ack, err := a.js.Publish(c, a.subject("root", "root"), raw)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = a.ReadRoot(c, "root"); err == nil {
			t.Fatal("malformed wire accepted")
		}
		if _, err = a.CASRoot(c, "root", 1, EmptyRoot()); err == nil {
			t.Fatal("malformed wire repaired")
		}
		after, err := a.stream.GetLastMsgForSubject(c, a.subject("root", "root"))
		if err != nil || after.Sequence != ack.Sequence || !bytes.Equal(after.Data, raw) {
			t.Fatal("malformed input changed", err)
		}
	}
	// A census containing an unknown subject cannot authorize collection.
	if _, err := a.js.Publish(c, a.prefix+".foreign.value", []byte("unknown")); err != nil {
		t.Fatal(err)
	}
	if _, err := a.BlobKeys(c); err == nil {
		t.Fatal("unknown subject ignored")
	}
}

type lostGraphMutation struct {
	jetstream.JetStream
	writes int
}

func (s *lostGraphMutation) PublishMsg(c context.Context, m *nats.Msg, o ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	ack, err := s.JetStream.PublishMsg(c, m, o...)
	if err == nil && m.Header.Get("Wf-Authority-Read-Witness") == "" {
		s.writes++
		return nil, context.DeadlineExceeded
	}
	return ack, err
}
func TestNativeGraphAuthorityLostMutationReplies(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			_, a, c := nativeGraphFixture(t, replicas)
			loss := &lostGraphMutation{JetStream: a.js}
			wrapped := *a
			wrapped.js = loss
			root, err := wrapped.CASRoot(c, "lost-root", 0, EmptyRoot())
			if !errors.Is(err, context.DeadlineExceeded) || root.Schema != "" || loss.writes != 1 {
				t.Fatal("uncertain root acknowledgment", root, err, loss.writes)
			}
			actual, err := a.ReadRoot(c, "lost-root")
			if err != nil || actual.Head != 1 {
				t.Fatal("root was not committed", actual, err)
			}
			if _, err = wrapped.CASRoot(c, "lost-root", 0, EmptyRoot()); !errors.Is(err, ErrConflict) || loss.writes != 1 {
				t.Fatal("ambiguous publication blindly repeated", err, loss.writes)
			}
			hash, owner := key([]byte("lost-payload")), "lost"
			scope := authorityKey(hash, owner)
			f := Fence{Hash: hash, Owner: owner, Generation: 1, Phase: "uploading", Intents: map[string]Intent{owner: {Destination: "lost-root", Expected: 1, Expires: time.Now().UTC().Add(time.Hour), Locations: []Location{{Kind: "payload", First: 0}}}}}
			record, err := wrapped.CASBlob(c, scope, 0, f)
			if !errors.Is(err, context.DeadlineExceeded) || record.Revision != 0 || loss.writes != 2 {
				t.Fatal("uncertain grant acknowledgment", record, err, loss.writes)
			}
			live, err := a.ReadBlob(c, scope)
			if err != nil || live.Revision != 1 || live.Fence.Generation != 1 {
				t.Fatal("grant was not committed", live, err)
			}
			if _, err = wrapped.CASBlob(c, scope, 0, f); !errors.Is(err, ErrConflict) || loss.writes != 2 {
				t.Fatal("ambiguous grant blindly repeated", err, loss.writes)
			}
		})
	}
}
