package integrity

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/internal/graphpublication"
	"js-wf/testcluster"
)

func TestNativeRawGraphReferenceAuditQuiescentReaders(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		for _, indexed := range []bool{false, true} {
			t.Run(fmt.Sprintf("R%d/indexed%v", replicas, indexed), func(t *testing.T) {
				cluster, err := testcluster.Start(t.TempDir(), replicas)
				if err != nil {
					t.Fatal(err)
				}
				defer cluster.Close()
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				if replicas > 1 {
					for {
						ready := false
						for _, server := range cluster.Servers {
							ready = ready || server.JetStreamIsLeader() && len(server.JetStreamClusterPeers()) == replicas
						}
						if ready {
							break
						}
						select {
						case <-ctx.Done():
							t.Fatal(ctx.Err())
						case <-time.After(10 * time.Millisecond):
						}
					}
				}
				js, err := jetstream.New(cluster.Clients[0])
				if err != nil {
					t.Fatal(err)
				}
				ns := GraphAuditNamespace{AuthorityStream: "AUDIT_AUTH", AuthorityPrefix: "wf.graph.audit", ObjectBucket: "AUDIT_OBJECTS", PayloadLimit: 1024}
				config := graphpublication.AuthorityStreamConfig(ns.AuthorityStream, ns.AuthorityPrefix, replicas)
				if indexed {
					config = graphpublication.OwnerIndexedAuthorityStreamConfig(ns.AuthorityStream, ns.AuthorityPrefix, replicas)
				}
				if _, err := js.CreateStream(ctx, config); err != nil {
					t.Fatal(err)
				}
				if _, err := js.CreateStream(ctx, graphpublication.NativeObjectStreamConfig(ns.ObjectBucket, replicas)); err != nil {
					t.Fatal(err)
				}
				var authority *graphpublication.NativeAuthority
				if indexed {
					authority, err = graphpublication.OpenOwnerIndexedNativeAuthority(ctx, js, ns.AuthorityStream, ns.AuthorityPrefix)
				} else {
					authority, err = graphpublication.OpenNativeAuthority(ctx, js, ns.AuthorityStream, ns.AuthorityPrefix)
				}
				if err != nil {
					t.Fatal(err)
				}
				var port graphpublication.Port
				if indexed {
					port, err = graphpublication.OpenOwnerIndexedNativePort(ctx, authority, ns.ObjectBucket)
				} else {
					port, err = graphpublication.OpenNativePort(ctx, authority, ns.ObjectBucket)
				}
				if err != nil {
					t.Fatal(err)
				}
				p := graphpublication.Protocol{Port: port}
				var root graphpublication.Root
				for i := 0; i < 2; i++ {
					prepared, err := p.PrepareAppend(ctx, "journal", root.Head, []byte("record"), [][]byte{[]byte("payload")}, time.Now().Add(time.Minute))
					if err != nil {
						t.Fatal(err)
					}
					root, err = p.Commit(ctx, prepared)
					if err != nil {
						t.Fatal(err)
					}
				}
				_, root, err = p.AcquireReader(ctx, "journal", root.Head, time.Now().Add(time.Minute))
				if err != nil {
					t.Fatal(err)
				}
				raw, err := js.Stream(ctx, ns.AuthorityStream)
				if err != nil {
					t.Fatal(err)
				}
				audit := func(wantRecords int) {
					t.Helper()
					before, err := raw.Info(ctx)
					if err != nil {
						t.Fatal(err)
					}
					report, err := CheckNativeGraphReferences(ctx, js, ns)
					if err != nil || report.Roots != 1 || report.ReaderPins != 1 || report.Records != wantRecords || report.PayloadEdges != wantRecords {
						t.Fatal(report, err)
					}
					after, err := raw.Info(ctx)
					if err != nil || after.State.LastSeq != before.State.LastSeq || after.State.Msgs != before.State.Msgs {
						t.Fatal("raw audit wrote authority", err)
					}
					t.Logf("raw audit R%d indexed=%v records=%d nodes=%d authority_last=%d unchanged", replicas, indexed, report.Records, report.Nodes, after.State.LastSeq)
				}
				audit(4)
				if _, err := p.RetireLiveWithApplication(ctx, "journal", root.Head, []byte(`{"retired":true}`)); err != nil {
					t.Fatal(err)
				}
				audit(2) // reader-owned graph survives retirement of the live graph
				rootMessage, err := raw.GetLastMsgForSubject(ctx, ns.AuthorityPrefix+".root."+digest([]byte("journal")))
				if err != nil {
					t.Fatal(err)
				}
				moving := graphAuditMutatingJS{JetStream: js, mutate: func() {
					if _, err := js.Publish(ctx, rootMessage.Subject, rootMessage.Data, jetstream.WithExpectLastSequencePerSubject(rootMessage.Sequence)); err != nil {
						t.Fatal(err)
					}
				}}
				if _, err := CheckNativeGraphReferences(ctx, moving, ns); err == nil || !strings.Contains(err.Error(), "authority changed during raw audit") {
					t.Fatal("moving authority accepted", err)
				}
				audit(2)
				var grant *jetstream.RawStreamMsg
				if err := scan(ctx, raw, func(m *jetstream.RawStreamMsg) error {
					if grant == nil && strings.HasPrefix(m.Subject, ns.AuthorityPrefix+".blob.") {
						grant = m
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				if grant == nil {
					t.Fatal("no persisted ownership grant")
				}
				var envelope map[string]json.RawMessage
				if err := json.Unmarshal(grant.Data, &envelope); err != nil {
					t.Fatal(err)
				}
				var fence graphpublication.Fence
				if err := json.Unmarshal(envelope["fence"], &fence); err != nil {
					t.Fatal(err)
				}
				fence.Phase = "closed"
				fence.Object = ""
				fence.Intents = nil
				envelope["fence"], err = json.Marshal(fence)
				if err != nil {
					t.Fatal(err)
				}
				corrupt, err := json.Marshal(envelope)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := js.Publish(ctx, grant.Subject, corrupt, jetstream.WithExpectLastSequencePerSubject(grant.Sequence)); err != nil {
					t.Fatal(err)
				}
				if _, err := CheckNativeGraphReferences(ctx, js, ns); err == nil || !strings.Contains(err.Error(), "missing graph ownership") {
					t.Fatal("closed referenced grant accepted", err)
				}
			})
		}
	}
}

type graphAuditMutatingJS struct {
	jetstream.JetStream
	mutate func()
}

func (js graphAuditMutatingJS) ObjectStore(ctx context.Context, bucket string) (jetstream.ObjectStore, error) {
	store, err := js.JetStream.ObjectStore(ctx, bucket)
	if err != nil {
		return nil, err
	}
	return &graphAuditMutatingObjects{ObjectStore: store, mutate: js.mutate}, nil
}

type graphAuditMutatingObjects struct {
	jetstream.ObjectStore
	mutate func()
	once   sync.Once
}

func (s *graphAuditMutatingObjects) Get(ctx context.Context, name string, opts ...jetstream.GetObjectOpt) (jetstream.ObjectResult, error) {
	result, err := s.ObjectStore.Get(ctx, name, opts...)
	if err == nil {
		s.once.Do(s.mutate)
	}
	return result, err
}
