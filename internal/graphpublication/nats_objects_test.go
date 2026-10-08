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
	"js-wf/internal/retainedgraph"
)

func nativeGraphObjectFixture(t *testing.T, replicas int) (*NativePort, context.Context) {
	t.Helper()
	_, a, c := nativeGraphFixture(t, replicas)
	if _, err := a.js.CreateStream(c, NativeObjectStreamConfig("GRAPH_OBJECTS", replicas)); err != nil {
		t.Fatal(err)
	}
	p, err := OpenNativePort(c, a, "GRAPH_OBJECTS")
	if err != nil {
		t.Fatal(err)
	}
	return p, c
}
func nativeGraphNoObjects(t *testing.T, p *NativePort, c context.Context) {
	t.Helper()
	objects, err := p.Objects(c)
	if err != nil || len(objects) != 0 {
		t.Fatal("unreclaimed objects", objects, err)
	}
	info, err := p.objectStream.Info(c, jetstream.WithSubjectFilter("$O."+p.bucket+".>"))
	if err != nil {
		t.Fatal(err)
	}
	for subject := range info.State.Subjects {
		if strings.Contains(subject, ".C.") {
			t.Fatal("physical chunks leaked", subject)
		}
	}
}

type nativeGraphReadStore struct{ *NativePort }

func (s nativeGraphReadStore) Put(context.Context, string, []byte) (blobpublication.Reference, error) {
	return blobpublication.Reference{}, errors.New("read only")
}
func TestNativeGraphObjectAppendReuseAndRetirement(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			port, c := nativeGraphObjectFixture(t, replicas)
			p := Protocol{Port: port}
			payload := bytes.Repeat([]byte("native-shared"), 25000)
			prepared, err := p.PrepareAppend(c, "history", 0, []byte("first"), [][]byte{payload}, time.Now().UTC().Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			root, err := p.Commit(c, prepared)
			if err != nil {
				t.Fatal(err)
			}
			record, err := retainedgraph.Read(c, nativeGraphReadStore{port}, root.Graph, 0)
			if err != nil || len(record.Blobs) != 1 {
				t.Fatal(record, err)
			}
			owned := record.Blobs[0]
			native, err := port.objects.GetBytes(c, owned.Reference.Object)
			if err != nil || !bytes.Equal(native, payload) {
				t.Fatal("standard ObjectStore compatibility", err)
			}
			for index := uint64(1); index <= 16; index++ {
				prepared, err = p.PrepareAppendWithOwned(c, "history", root.Head, []byte(fmt.Sprint(index)), nil, []OwnedPayload{{Index: index - 1, Link: owned}}, time.Now().UTC().Add(time.Hour))
				if err != nil {
					t.Fatal(err)
				}
				root, err = p.Commit(c, prepared)
				if err != nil {
					t.Fatal(err)
				}
				if n, err := p.Sweep(c, time.Now().Add(2*time.Hour)); err != nil || n != 0 {
					t.Fatal("inherited receipts collected", n, err)
				}
			}
			// Independent raw JSON child census, not graph Walk/Read membership helpers.
			visited := map[retainedgraph.Link]bool{}
			leaves := 0
			var visit func(retainedgraph.Tree)
			visit = func(tree retainedgraph.Tree) {
				if visited[tree.Link] {
					t.Fatal("duplicate canonical node")
				}
				visited[tree.Link] = true
				raw, err := port.Get(c, tree.Link, retainedgraph.MaxNodeBytes)
				if err != nil || key(raw) != tree.Link.Hash {
					t.Fatal("node bytes", err)
				}
				var node struct {
					First    uint64                `json:"first"`
					Height   uint8                 `json:"height"`
					Children []retainedgraph.Link  `json:"children"`
					Record   *retainedgraph.Record `json:"record"`
				}
				if err = json.Unmarshal(raw, &node); err != nil || node.First != tree.First || node.Height != tree.Height {
					t.Fatal("node position", err)
				}
				if tree.Height == 0 {
					if node.Record == nil || len(node.Record.Blobs) != 1 || node.Record.Blobs[0] != owned {
						t.Fatal("leaf ownership")
					}
					leaves++
					return
				}
				if len(node.Children) != 2 {
					t.Fatal("child census")
				}
				for i, link := range node.Children {
					visit(retainedgraph.Tree{First: tree.First + uint64(i)*(uint64(1)<<(tree.Height-1)), Height: tree.Height - 1, Link: link})
				}
			}
			for _, tree := range root.Graph.Frontier {
				visit(tree)
			}
			if leaves != 17 {
				t.Fatal("population", leaves)
			}
			objects, err := port.Objects(c)
			if err != nil || len(objects) != len(visited)+1 {
				t.Fatal("payload copied or objects missing", len(objects), len(visited), err)
			}
			if _, err = port.Get(c, owned, len(payload)-1); err == nil {
				t.Fatal("read exceeded byte budget")
			}
			if _, err = port.Get(c, owned, len(payload)); err != nil {
				t.Fatal(err)
			}
			legacyAuthority, err := blobpublication.OpenNativeAuthority(c, port.js, port.name, port.prefix)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = blobpublication.OpenNativePort(c, legacyAuthority, port.bucket); err == nil {
				t.Fatal("legacy collector opened graph bucket")
			}
			if err = p.Retire(c, "history", root.Head); err != nil {
				t.Fatal(err)
			}
			if _, err = p.Sweep(c, time.Now().Add(3*time.Hour)); err != nil {
				t.Fatal(err)
			}
			nativeGraphNoObjects(t, port, c)
			retired, err := port.ReadRoot(c, "history")
			if err != nil || retired.Head != root.Head+1 || retired.Graph.Count != 0 {
				t.Fatal(retired, err)
			}
		})
	}
}

type graphObjectFault struct {
	jetstream.JetStream
	hook   func(context.Context, *nats.Msg) error
	after  bool
	writes int
}

func (s *graphObjectFault) PublishMsg(c context.Context, m *nats.Msg, o ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	if !s.after && s.hook != nil {
		if err := s.hook(c, m); err != nil {
			return nil, err
		}
	}
	ack, err := s.JetStream.PublishMsg(c, m, o...)
	if err == nil {
		s.writes++
		if s.after && s.hook != nil {
			if err = s.hook(c, m); err != nil {
				return nil, err
			}
		}
	}
	return ack, err
}
func nativeGraphPendingObject(t *testing.T, p *NativePort, c context.Context, data []byte) (string, string) {
	t.Helper()
	hash, owner := key(data), "pending"
	scope := authorityKey(hash, owner)
	f := Fence{Hash: hash, Owner: owner, Generation: 1, Phase: "uploading", Intents: map[string]Intent{owner: {Destination: "pending-root", Expected: 0, Expires: time.Now().UTC().Add(time.Hour), Locations: []Location{{Kind: "payload", First: 0}}}}}
	if _, err := p.CASBlob(c, scope, 0, f); err != nil {
		t.Fatal(err)
	}
	return scope, objectName(hash, 1, owner, "upload")
}
func TestNativeGraphObjectReservationsLostRepliesAndLateCompletion(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		for _, mode := range []string{"lost_reservation", "lost_chunk", "lost_completion", "collector_before_completion"} {
			t.Run(fmt.Sprintf("R%d/%s", replicas, mode), func(t *testing.T) {
				port, c := nativeGraphObjectFixture(t, replicas)
				p := Protocol{Port: port}
				data := bytes.Repeat([]byte("staged"), 50000)
				_, name := nativeGraphPendingObject(t, port, c, data)
				wrapped := *port
				authority := *port.NativeAuthority
				wrapped.NativeAuthority = &authority
				triggered := false
				fault := &graphObjectFault{JetStream: port.js, after: mode != "collector_before_completion"}
				fault.hook = func(call context.Context, m *nats.Msg) error {
					if triggered || !strings.HasPrefix(m.Subject, "$O."+port.bucket+".") {
						return nil
					}
					chunk := strings.Contains(m.Subject, ".C.")
					state := ""
					if !chunk {
						var info jetstream.ObjectInfo
						if err := json.Unmarshal(m.Data, &info); err != nil {
							return err
						}
						state = info.Metadata["js-wf-graph-state"]
					}
					match := mode == "lost_reservation" && state == "staging" || mode == "lost_chunk" && chunk || (mode == "lost_completion" || mode == "collector_before_completion") && state == "complete"
					if !match {
						return nil
					}
					triggered = true
					if mode == "collector_before_completion" {
						_, err := p.Sweep(call, time.Now().Add(2*time.Hour))
						return err
					}
					return context.DeadlineExceeded
				}
				authority.js = fault
				err := wrapped.Put(c, name, data)
				if err == nil || !triggered {
					t.Fatal("fault did not abort", mode, err, triggered)
				}
				if err = port.Put(c, name, data); err == nil {
					t.Fatal("partial/complete attempt reused")
				}
				if _, err = p.Sweep(c, time.Now().Add(3*time.Hour)); err != nil {
					t.Fatal(err)
				}
				nativeGraphNoObjects(t, port, c)
				// Recreated chunks after a tombstone remain discoverable and are purged.
				if _, err = port.publishObjectSequence(c, port.chunkSubject(name), data[:nativeChunkSize], nil); err != nil {
					t.Fatal(err)
				}
				if _, err = p.Sweep(c, time.Now().Add(4*time.Hour)); err != nil {
					t.Fatal(err)
				}
				nativeGraphNoObjects(t, port, c)
				meta, _, err := port.metadata(c, name)
				if err != nil || meta == nil || !meta.Deleted {
					t.Fatal("tombstone lost", meta, err)
				}
			})
		}
	}
}

func TestNativeGraphObjectCancellationBoundsAndUnknownChunks(t *testing.T) {
	port, c := nativeGraphObjectFixture(t, 1)
	data := []byte("small")
	_, name := nativeGraphPendingObject(t, port, c, data)
	if err := port.Put(c, name, data); err != nil {
		t.Fatal(err)
	}
	object, err := parsePhysicalObject(name)
	if err != nil {
		t.Fatal(err)
	}
	link := retainedgraph.Link{Hash: object.Key, Reference: object.Reference}
	canceled, stop := context.WithCancel(c)
	stop()
	if _, err = port.Get(canceled, link, 10); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err = port.Get(c, link, -1); err == nil {
		t.Fatal("negative budget accepted")
	}
	if _, err = port.Get(c, link, 4); err == nil {
		t.Fatal("over budget")
	}
	read, err := port.Get(c, link, 5)
	if err != nil || !bytes.Equal(read, data) {
		t.Fatal(read, err)
	}
	read[0] = 'x'
	again, err := port.Get(c, link, 5)
	if err != nil || !reflect.DeepEqual(again, data) {
		t.Fatal("copy isolation", err)
	}
	if _, err = port.publishObjectSequence(c, "$O."+port.bucket+".C.opaque", []byte("unknown"), nil); err != nil {
		t.Fatal(err)
	}
	if _, err = port.Objects(c); !errors.Is(err, ErrUntrackedChunks) {
		t.Fatal("opaque chunks accepted", err)
	}
}

func TestNativeGraphObjectPublishCollectOrderAndDelayedDelete(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			port, c := nativeGraphObjectFixture(t, replicas)
			data := []byte("same-content")
			collector := Protocol{Port: port}
			wrapped := *port
			authority := *port.NativeAuthority
			wrapped.NativeAuthority = &authority
			publisher := Protocol{Port: &wrapped}
			prepared, err := publisher.PrepareAppend(c, "history", 0, []byte("loser"), [][]byte{data}, time.Now().UTC().Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			pendingRecord, err := retainedgraph.Read(c, nativeGraphReadStore{port}, prepared.publication.Graph, 0)
			if err != nil || len(pendingRecord.Blobs) != 1 {
				t.Fatal(pendingRecord, err)
			}
			oldName := pendingRecord.Blobs[0].Reference.Object
			fault := &graphObjectFault{JetStream: port.js}
			fenced := false
			fault.hook = func(call context.Context, m *nats.Msg) error {
				if fenced || m.Header.Get("Wf-Authority-Read-Witness") != "" || m.Subject != port.subject("root", "history") {
					return nil
				}
				fenced = true
				_, err := collector.Sweep(call, time.Now().Add(2*time.Hour))
				return err
			}
			authority.js = fault
			if _, err = publisher.Commit(c, prepared); !errors.Is(err, ErrConflict) || !fenced {
				t.Fatal("collector lost original-head ordering", err, fenced)
			}
			nativeGraphNoObjects(t, port, c)
			root, err := port.ReadRoot(c, "history")
			if err != nil || root.Head != 1 || root.Graph.Count != 0 {
				t.Fatal("fenced root", root, err)
			}
			fresh, err := collector.PrepareAppend(c, "history", root.Head, []byte("winner"), [][]byte{data}, time.Now().UTC().Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			root, err = collector.Commit(c, fresh)
			if err != nil {
				t.Fatal(err)
			}
			if err = port.Delete(c, oldName); err != nil {
				t.Fatal(err)
			}
			if _, err = collector.Sweep(c, time.Now().Add(3*time.Hour)); err != nil {
				t.Fatal(err)
			}
			record, err := retainedgraph.Read(c, nativeGraphReadStore{port}, root.Graph, 0)
			if err != nil || string(record.Data) != "winner" || len(record.Blobs) != 1 {
				t.Fatal("new graph lost to old delete", record, err)
			}
			payload, err := port.Get(c, record.Blobs[0], len(data))
			if err != nil || !bytes.Equal(payload, data) {
				t.Fatal("fresh same-content payload lost", err)
			}
			if err = collector.Retire(c, "history", root.Head); err != nil {
				t.Fatal(err)
			}
			if _, err = collector.Sweep(c, time.Now().Add(4*time.Hour)); err != nil {
				t.Fatal(err)
			}
			nativeGraphNoObjects(t, port, c)
		})
	}
}

type graphChunkReadBarrier struct {
	jetstream.Stream
	reached chan struct{}
	mode    string
}

func (s *graphChunkReadBarrier) GetMsg(c context.Context, sequence uint64, o ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
	if s.mode == "cancel" {
		close(s.reached)
		<-c.Done()
		return nil, c.Err()
	}
	raw, err := s.Stream.GetMsg(c, sequence, o...)
	if err != nil {
		return nil, err
	}
	copied := *raw
	copied.Data = bytes.Clone(raw.Data)
	if s.mode == "corrupt" {
		copied.Data[0] ^= 1
	}
	if s.mode == "subject" {
		copied.Subject = "foreign"
	}
	if s.mode == "sequence" {
		copied.Sequence = sequence - 1
	}
	return &copied, nil
}
func TestNativeGraphObjectReadUncertaintyAndFormatIsolation(t *testing.T) {
	port, c := nativeGraphObjectFixture(t, 1)
	data := []byte("read-control")
	_, name := nativeGraphPendingObject(t, port, c, data)
	if err := port.Put(c, name, data); err != nil {
		t.Fatal(err)
	}
	object, _ := parsePhysicalObject(name)
	link := retainedgraph.Link{Hash: object.Key, Reference: object.Reference}
	for _, mode := range []string{"corrupt", "subject", "sequence"} {
		wrapped := *port
		wrapped.objectStream = &graphChunkReadBarrier{Stream: port.objectStream, mode: mode}
		if got, err := wrapped.Get(c, link, len(data)); err == nil || got != nil {
			t.Fatal("uncertain bytes accepted", mode, got, err)
		}
	}
	child, cancel := context.WithCancel(c)
	defer cancel()
	barrier := &graphChunkReadBarrier{Stream: port.objectStream, mode: "cancel", reached: make(chan struct{})}
	wrapped := *port
	wrapped.objectStream = barrier
	done := make(chan error, 1)
	go func() { _, err := wrapped.Get(child, link, len(data)); done <- err }()
	select {
	case <-barrier.reached:
	case <-time.After(time.Second):
		t.Fatal("chunk read not entered")
	}
	started := time.Now()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("read ignored cancellation")
	}
	t.Log("chunk cancellation elapsed", time.Since(started))
	if _, err := port.js.CreateStream(c, blobpublication.NativeObjectStreamConfig("DIRECT_OBJECTS", 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenNativePort(c, port.NativeAuthority, "DIRECT_OBJECTS"); err == nil {
		t.Fatal("graph adapter opened direct bucket")
	}
	// A missing object bucket is never created by admission.
	if _, err := OpenNativePort(c, port.NativeAuthority, "MISSING"); err == nil {
		t.Fatal("missing bucket opened")
	}
	if _, err := port.js.Stream(c, "OBJ_MISSING"); !errors.Is(err, jetstream.ErrStreamNotFound) {
		t.Fatal("missing bucket created", err)
	}
	if err := port.objectStream.Purge(c, jetstream.WithPurgeSubject(port.chunkSubject(name))); err != nil {
		t.Fatal(err)
	}
	if got, err := port.Get(c, link, len(data)); err == nil || got != nil {
		t.Fatal("missing chunks accepted", got, err)
	}
}
