package blobpublication

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"js-wf/testcluster"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type staleAuthorityRead struct {
	jetstream.Stream
	subject string
	raw     *jetstream.RawStreamMsg
	always  bool
	calls   int
}

func (s *staleAuthorityRead) GetLastMsgForSubject(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	if subject != s.subject {
		return s.Stream.GetLastMsgForSubject(ctx, subject)
	}
	s.calls++
	if s.calls == 1 || s.always {
		if s.raw == nil {
			return nil, jetstream.ErrMsgNotFound
		}
		copy := *s.raw
		copy.Data = append([]byte(nil), s.raw.Data...)
		copy.Header = nats.Header{}
		for name, values := range s.raw.Header {
			copy.Header[name] = append([]string(nil), values...)
		}
		return &copy, nil
	}
	return s.Stream.GetLastMsgForSubject(ctx, subject)
}

type lostWitnessReply struct {
	jetstream.JetStream
	acknowledgments int
}

func (s *lostWitnessReply) PublishMsg(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	ack, err := s.JetStream.PublishMsg(ctx, msg, opts...)
	if err == nil && msg.Header.Get("Wf-Authority-Read-Witness") == "1" {
		s.acknowledgments++
		return nil, context.DeadlineExceeded
	}
	return ack, err
}

// GET replies are controlled; every witness/conditional write is executed by
// the actual R1/R3 server. These controls prove that speculative GET bytes and
// absence cannot alone produce a successful authority read.
func TestNativeAuthorityReadWitness(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			_, port, ctx, directory := nativeObjectFixture(t, replicas)
			authority := port.NativeAuthority
			old, err := authority.CASRoot(ctx, "read-root", 0, Root{Token: "old", Data: []byte("old")})
			if err != nil {
				t.Fatal(err)
			}
			subject := authority.subject("root", "read-root")
			raw, err := authority.stream.GetLastMsgForSubject(ctx, subject)
			if err != nil {
				t.Fatal(err)
			}
			latest, err := authority.CASRoot(ctx, "read-root", old.Head, Root{Token: "new", Data: []byte("new")})
			if err != nil {
				t.Fatal(err)
			}
			cases := []map[string]any{}
			for _, mode := range []string{"stale-value", "stale-absence", "future-value", "persistent-stale"} {
				supplied := raw
				if mode == "stale-absence" {
					supplied = nil
				}
				if mode == "future-value" {
					copy := *raw
					copy.Sequence = ^uint64(0) - 1
					fabricated := Root{Head: 999, Token: "speculative", Data: []byte("unconfirmed")}
					copy.Data, _ = json.Marshal(authorityValue{Schema: authoritySchema, Kind: "root", Identity: "read-root", Revision: 999, Root: &fabricated})
					supplied = &copy
				}
				controlled := &staleAuthorityRead{Stream: authority.stream, subject: subject, raw: supplied, always: mode == "persistent-stale"}
				reader := *authority
				reader.stream = controlled
				before, err := authority.stream.GetLastMsgForSubject(ctx, subject)
				if err != nil {
					t.Fatal(err)
				}
				got, err := reader.ReadRoot(ctx, "read-root")
				if mode == "persistent-stale" {
					if !errors.Is(err, ErrConflict) || controlled.calls != 16 {
						t.Fatal("stale read not bounded/rejected", got, err, controlled.calls)
					}
				} else if err != nil || !reflect.DeepEqual(got, latest) || controlled.calls != 2 {
					t.Fatal("stale root returned", mode, got, err, controlled.calls)
				}
				after, e := authority.stream.GetLastMsgForSubject(ctx, subject)
				if e != nil {
					t.Fatal(e)
				}
				if mode != "persistent-stale" && after.Sequence <= before.Sequence {
					t.Fatal("read has no committed witness")
				}
				if after.Header.Get("Wf-Authority-Read-Witness") != "1" {
					t.Fatal("read witness header missing")
				}
				cases = append(cases, map[string]any{"mode": mode, "snapshot_reads": controlled.calls, "returned_root": got, "expected_root": latest, "before_sequence": before.Sequence, "after_sequence": after.Sequence, "rejected": err != nil})
			}
			// Missing identities acquire permanent absence witnesses; subsequent first
			// publication still has logical head/generation one, never physical seq.
			absent, err := authority.ReadRoot(ctx, "absent")
			if err != nil || absent.Head != 0 || absent.Token != "" {
				t.Fatal("absence witness", absent, err)
			}
			missing, err := authority.stream.GetLastMsgForSubject(ctx, authority.subject("root", "absent"))
			if err != nil || missing.Sequence == 0 {
				t.Fatal("missing absence record", err)
			}
			first, err := authority.CASRoot(ctx, "absent", 0, Root{Token: "first"})
			if err != nil || first.Head != 1 {
				t.Fatal("absence consumed logical head", first, err)
			}
			k := key([]byte("read-blob"))
			intent := Intent{Root: "read-root", Expires: time.Now().Add(time.Minute)}
			upload, err := authority.CASBlob(ctx, k, 0, Fence{Generation: 1, Phase: "uploading", Intents: map[string]Intent{"pin": intent}})
			if err != nil {
				t.Fatal(err)
			}
			blobSubject := authority.subject("blob", k)
			blobRaw, err := authority.stream.GetLastMsgForSubject(ctx, blobSubject)
			if err != nil {
				t.Fatal(err)
			}
			closed, err := authority.CASBlob(ctx, k, upload.Revision, Fence{Generation: 1, Phase: "closed"})
			if err != nil {
				t.Fatal(err)
			}
			for _, missing := range []bool{false, true} {
				r := blobRaw
				if missing {
					r = nil
				}
				controlled := &staleAuthorityRead{Stream: authority.stream, subject: blobSubject, raw: r}
				reader := *authority
				reader.stream = controlled
				got, err := reader.ReadBlob(ctx, k)
				if err != nil || !reflect.DeepEqual(got, closed) || controlled.calls != 2 {
					t.Fatal("stale blob returned", missing, got, err)
				}
				cases = append(cases, map[string]any{"mode": fmt.Sprintf("blob-missing-%v", missing), "snapshot_reads": controlled.calls, "returned_blob": got, "expected_blob": closed})
			}
			legacyRoot := Root{Head: 7, Token: "legacy", Data: []byte("retained-v1")}
			legacyBytes, err := json.Marshal(authorityValue{Schema: legacyAuthoritySchema, Kind: "root", Identity: "legacy-root", Revision: 7, Root: &legacyRoot})
			if err != nil {
				t.Fatal(err)
			}
			legacySubject := authority.subject("root", "legacy-root")
			if _, err = authority.js.Publish(ctx, legacySubject, legacyBytes, jetstream.WithExpectLastSequencePerSubject(0)); err != nil {
				t.Fatal(err)
			}
			upgraded, err := authority.ReadRoot(ctx, "legacy-root")
			if err != nil || !reflect.DeepEqual(upgraded, legacyRoot) {
				t.Fatal("legacy high-water reset", upgraded, err)
			}
			upgradedRaw, err := authority.stream.GetLastMsgForSubject(ctx, legacySubject)
			if err != nil {
				t.Fatal(err)
			}
			var upgradedEnvelope authorityValue
			if json.Unmarshal(upgradedRaw.Data, &upgradedEnvelope) != nil || upgradedEnvelope.Schema != authoritySchema || upgradedEnvelope.Revision != 7 {
				t.Fatal("upgrade not committed", upgradedEnvelope)
			}
			cases = append(cases, map[string]any{"mode": "legacy-v1-upgrade", "root": upgraded, "envelope": upgradedEnvelope, "physical_sequence": upgradedRaw.Sequence})
			lost := &lostWitnessReply{JetStream: authority.js}
			reader := *authority
			reader.js = lost
			got, err := reader.ReadRoot(ctx, "read-root")
			if !errors.Is(err, context.DeadlineExceeded) || got.Head != 0 || lost.acknowledgments != 1 {
				t.Fatal("lost witness response upgraded by GET", got, err, lost.acknowledgments)
			}
			controlled := &staleAuthorityRead{Stream: authority.stream, subject: subject}
			reader = *authority
			reader.stream = controlled
			canceled, stop := context.WithCancel(ctx)
			stop()
			if _, err = reader.ReadRoot(canceled, "read-root"); !errors.Is(err, context.Canceled) || controlled.calls != 0 {
				t.Fatal("cancellation contacted stream", err)
			}
			nativeObjectProof(t, directory, map[string]any{"scenario": "authority-read-witness", "replicas": replicas, "parent_budget_seconds": 30, "cases": cases, "absence_physical_sequence": missing.Sequence, "first_logical_head": first.Head, "lost_acknowledgments": lost.acknowledgments, "lost_response_rejected": true, "canceled_before_get": true, "logical_root_head_unchanged": latest.Head})
			t.Logf("native authority witness: R%d stale/absent/speculative replies rejected by actual conditional CAS; logical heads preserved; lost acknowledgment fails closed", replicas)
		})
	}
}

func TestNativeAuthorityWitnessRacesCommittedReplacement(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			cluster, port, ctx, directory := nativeObjectFixture(t, replicas)
			authority := port.NativeAuthority
			old, err := authority.CASRoot(ctx, "read-race", 0, Root{Token: "old", Data: []byte("old")})
			if err != nil {
				t.Fatal(err)
			}
			proxy, err := testcluster.NewClientProxy(cluster.Servers[0].ClientURL())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(proxy.Close)
			subject := authority.subject("root", "read-race")
			if err = proxy.HoldFirstPublication(subject); err != nil {
				t.Fatal(err)
			}
			if err = proxy.EnableTrafficTrace(1 << 20); err != nil {
				t.Fatal(err)
			}
			conn, err := nats.Connect(proxy.URL(), nats.NoReconnect())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(conn.Close)
			js, err := jetstream.New(conn)
			if err != nil {
				t.Fatal(err)
			}
			reader, err := OpenNativeAuthority(ctx, js, authority.name, authority.prefix)
			if err != nil {
				t.Fatal(err)
			}
			type result struct {
				root Root
				err  error
			}
			done := make(chan result, 1)
			go func() { r, e := reader.ReadRoot(ctx, "read-race"); done <- result{r, e} }()
			nativeWait(t, ctx, func() bool { return proxy.PendingAPI() != nil })
			held := proxy.PendingAPI()
			if held.Subject != subject || held.ForwardedBytes != 0 || !bytes.Contains(held.Packet, []byte("Wf-Authority-Read-Witness: 1\r\n")) {
				t.Fatal("wrong read witness held", held)
			}
			latest, err := authority.CASRoot(ctx, "read-race", old.Head, Root{Token: "latest", Data: []byte("latest")})
			if err != nil {
				t.Fatal(err)
			}
			proxy.ReleaseFirstAPI()
			var got result
			select {
			case got = <-done:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if got.err != nil || !reflect.DeepEqual(got.root, latest) {
				t.Fatal("held witness overwrote replacement", got)
			}
			forwarded := proxy.PendingAPI()
			if forwarded.ForwardedBytes != uint64(len(held.Packet)) {
				t.Fatal("witness not forwarded completely")
			}
			nativeObjectProof(t, directory, map[string]any{"scenario": "read-witness-replacement-race", "replicas": replicas, "held": held, "forwarded": forwarded, "old_root": old, "latest_root": latest, "returned_root": got.root, "wire": proxy.TrafficTrace(), "old_witness_rejected": true})
		})
	}
}

func TestNativeAuthorityReadRequiresQuorum(t *testing.T) {
	directory := nativeAuthorityDirectory(t)
	cluster, err := testcluster.StartPartitionable(directory, 3)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	ctx, stop := context.WithTimeout(t.Context(), 30*time.Second)
	defer stop()
	nativeWait(t, ctx, func() bool {
		for _, s := range cluster.Servers {
			if s.JetStreamIsLeader() && len(s.JetStreamClusterPeers()) == 3 {
				return true
			}
		}
		return false
	})
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	stream, err := js.CreateStream(ctx, AuthorityStreamConfig("BLOB_AUTH", "wf.blob.authority", 3))
	if err != nil {
		t.Fatal(err)
	}
	controller, err := OpenNativeAuthority(ctx, js, "BLOB_AUTH", "wf.blob.authority")
	if err != nil {
		t.Fatal(err)
	}
	old, err := controller.CASRoot(ctx, "partition-read", 0, Root{Token: "old"})
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	leader := -1
	for i, s := range cluster.Servers {
		if s.Name() == info.Cluster.Leader {
			leader = i
		}
	}
	if leader < 0 {
		t.Fatal("native stream leader not found")
	}
	minorityJS, err := jetstream.New(cluster.Clients[leader])
	if err != nil {
		t.Fatal(err)
	}
	minority, err := OpenNativeAuthority(ctx, minorityJS, "BLOB_AUTH", "wf.blob.authority")
	if err != nil {
		t.Fatal(err)
	}
	majorityJS, err := jetstream.New(cluster.Clients[(leader+1)%3])
	if err != nil {
		t.Fatal(err)
	}
	majority, err := OpenNativeAuthority(ctx, majorityJS, "BLOB_AUTH", "wf.blob.authority")
	if err != nil {
		t.Fatal(err)
	}
	if err = cluster.RouteMesh().PartitionNode(leader); err != nil {
		t.Fatal(err)
	}
	defer cluster.RouteMesh().Heal()
	call, end := context.WithTimeout(ctx, 500*time.Millisecond)
	minorityRoot, minorityErr := minority.ReadRoot(call, "partition-read")
	end()
	if minorityErr == nil {
		t.Fatal("isolated authority returned successful root", minorityRoot)
	}
	var latest Root
	for ctx.Err() == nil {
		call, end = context.WithTimeout(ctx, 2*time.Second)
		latest, err = majority.CASRoot(call, "partition-read", old.Head, Root{Token: "majority"})
		end()
		if err == nil {
			break
		}
		call, end = context.WithTimeout(ctx, 2*time.Second)
		confirmed, e := majority.ReadRoot(call, "partition-read")
		end()
		if e == nil && confirmed.Token == "majority" && confirmed.Head == 2 {
			latest = confirmed
			err = nil
			break
		}
		select {
		case <-ctx.Done():
		case <-time.After(20 * time.Millisecond):
		}
	}
	if err != nil || latest.Head != 2 {
		t.Fatal("majority could not commit original head", latest, err, ctx.Err())
	}
	call, end = context.WithTimeout(ctx, 500*time.Millisecond)
	_, afterErr := minority.ReadRoot(call, "partition-read")
	end()
	if afterErr == nil {
		t.Fatal("isolated peer returned success after majority advance")
	}
	cluster.RouteMesh().Heal()
	var healed Root
	for ctx.Err() == nil {
		call, end = context.WithTimeout(ctx, 2*time.Second)
		healed, err = minority.ReadRoot(call, "partition-read")
		end()
		if err == nil && reflect.DeepEqual(healed, latest) {
			break
		}
		select {
		case <-ctx.Done():
		case <-time.After(20 * time.Millisecond):
		}
	}
	if err != nil || !reflect.DeepEqual(healed, latest) {
		t.Fatal("healed read lost majority head", healed, err)
	}
	peers := []map[string]any{}
	for _, s := range cluster.Servers {
		v, e := s.Varz(nil)
		if e != nil {
			t.Fatal(e)
		}
		peers = append(peers, map[string]any{"id": s.ID(), "name": s.Name(), "version": v.Version, "embedding_commit": v.GitCommit})
	}
	nativeObjectProof(t, directory, map[string]any{"scenario": "read-witness-quorum-partition", "replicas": 3, "parent_budget_seconds": 30, "minority_node": leader, "before_root": old, "majority_root": latest, "healed_root": healed, "minority_before_error": minorityErr.Error(), "minority_after_error": afterErr.Error(), "peers": peers, "isolated_reads_rejected": true, "healed_head_preserved": true})
}
