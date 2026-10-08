package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/internal/graphpublication"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/retention"
	"js-wf/testcluster"
	"js-wf/wf"
)

func TestNativeCanonicalStartRecoveryWorkerReplayAndPurge(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			cluster, err := testcluster.Start(t.TempDir(), replicas)
			if err != nil {
				t.Fatal(err)
			}
			defer cluster.Close()
			ctx, stop := context.WithTimeout(context.Background(), 3*time.Minute)
			defer stop()
			if replicas > 1 {
				for {
					ready := false
					for _, s := range cluster.Servers {
						ready = ready || s.JetStreamIsLeader() && len(s.JetStreamClusterPeers()) == replicas
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
			js, err := jetstream.New(cluster.Clients[0])
			if err != nil {
				t.Fatal(err)
			}
			if err = provision.Ensure(ctx, js, replicas); err != nil {
				t.Fatal(err)
			}
			cfg := journal.NativeGraphConfig{AuthorityStream: "GRAPH_START_AUTH", AuthorityPrefix: "wf.graph.start", ObjectBucket: "GRAPH_START_OBJECTS", ExpectedReplicas: replicas, CanonicalStarts: true}
			configs, err := journal.NativeGraphStreamConfigs(cfg, replicas)
			if err != nil {
				t.Fatal(err)
			}
			for _, config := range configs {
				if _, err = js.CreateStream(ctx, config); err != nil {
					t.Fatal(err)
				}
			}
			store, err := journal.OpenNativeGraphStore(ctx, js, cfg)
			if err != nil {
				t.Fatal(err)
			}
			input, _ := json.Marshal(string(bytes.Repeat([]byte("x"), 5<<20)))
			invocations, err := js.Stream(ctx, "WF_INV")
			if err != nil {
				t.Fatal(err)
			}
			for _, mode := range []string{"ordinary", "reserved", "source_committed", "bound_no_enqueue"} {
				t.Run(mode, func(t *testing.T) {
					const typ = "canonical"
					request := journal.GraphStartRequest{Type: typ, ID: mode}
					if mode != "ordinary" {
						state, e := store.ReserveStart(ctx, request, input)
						if e != nil {
							t.Fatal(e)
						}
						if _, e = store.Begin(ctx, typ, mode, 1); !errors.Is(e, journal.ErrStale) {
							t.Fatal("pending start admitted execution", e)
						}
						if mode != "reserved" {
							msg := &nats.Msg{Subject: identity.InvocationSubject(typ, mode), Data: state.Start.PointerBytes(), Header: nats.Header{}}
							msg.Header.Set(journal.GraphStartTokenHeader, state.Start.Token)
							msg.Header.Set("Wf-Input-SHA256", state.Start.InputSHA256)
							msg.Header.Set(jetstream.ExpectedLastSubjSeqHeader, "0")
							ack, e := js.PublishMsg(ctx, msg)
							if e != nil {
								t.Fatal(e)
							}
							if mode == "bound_no_enqueue" {
								if e = store.BindStart(ctx, typ, mode, state.Start.Token, ack.Sequence); e != nil {
									t.Fatal(e)
								}
							}
						}
					}
					// Construct fresh native adapters/client after the prepared durable cut.
					fresh, e := journal.OpenNativeGraphStore(ctx, js, cfg)
					if e != nil {
						t.Fatal(e)
					}
					c, e := client.NewWithGraphJournal(js, fresh)
					if e != nil {
						t.Fatal(e)
					}
					var handle client.Handle
					if mode == "ordinary" {
						handle, e = c.Start(ctx, typ, mode, input)
					} else {
						handle, e = c.RecoverStart(ctx, typ, mode)
					}
					if e != nil {
						t.Fatal(e)
					}
					raw, e := invocations.GetLastMsgForSubject(ctx, identity.InvocationSubject(typ, mode))
					if e != nil || raw.Sequence != handle.InvSeq || len(raw.Data) > 256 || raw.Header.Get("Wf-Input-Ref") != "" || raw.Header.Get(journal.GraphStartTokenHeader) == "" {
						t.Fatal(raw, e)
					}
					if duplicate, e := c.Start(ctx, typ, mode, input); !errors.Is(e, client.ErrAlreadyStarted) || duplicate != handle {
						t.Fatal(duplicate, e)
					}
					var effects atomic.Int64
					handlers := map[string]Handler{typ: func(c *wf.Context, got json.RawMessage) (json.RawMessage, error) {
						if !bytes.Equal(got, input) {
							return nil, fmt.Errorf("canonical input mismatch")
						}
						_, e := wf.Run(c, "once", 0, func(context.Context) (int, error) { effects.Add(1); return 42, nil })
						if e != nil {
							return nil, e
						}
						return json.RawMessage(`42`), nil
					}}
					var acks atomic.Int64
					w, e := New(ctx, js, "canonical-"+mode, handlers, WithGraphJournal(fresh), WithDispatchObserver(func(e DispatchEvent) {
						if e.Type == typ && e.ID == mode && e.Stage == "ack" {
							acks.Add(1)
						}
					}))
					if e != nil {
						t.Fatal(e)
					}
					runctx, cancel := context.WithCancel(ctx)
					done := make(chan error, 1)
					go func() { done <- w.RunPartition(runctx, identity.Partition(typ, mode, provision.Partitions)) }()
					value, e := c.Await(ctx, typ, mode)
					if e != nil || string(value) != "42" {
						cancel()
						<-done
						w.Close()
						t.Fatal(string(value), e)
					}
					if e = c.Enqueue(ctx, typ, mode, "canonical-replay:"+mode); e != nil {
						t.Fatal(e)
					}
					// Completion is already canonical; await a duplicate delivery ACK.
					for acks.Load() < 2 {
						select {
						case <-ctx.Done():
							cancel()
							<-done
							w.Close()
							t.Fatal(ctx.Err())
						case <-time.After(20 * time.Millisecond):
						}
					}
					cancel()
					if e = <-done; e != nil {
						t.Fatal(e)
					}
					if e = w.Close(); e != nil {
						t.Fatal(e)
					}
					if effects.Load() != 1 {
						t.Fatal("replayed effect", effects.Load())
					}
					legacy, e := js.Stream(ctx, "OBJ_WF_BLOB")
					if e != nil {
						t.Fatal(e)
					}
					info, e := legacy.Info(ctx)
					if e != nil || info.State.Msgs != 0 {
						t.Fatal("legacy input staging used", info, e)
					}
					held, e := fresh.OpenStart(ctx, typ, mode)
					if e != nil {
						t.Fatal(e)
					}
					if e = retention.PurgeGraphInvocation(ctx, js, fresh, typ, mode, handle.InvSeq, time.Minute); e != nil {
						t.Fatal(e)
					}
					if got, e := held.StartInput(ctx); e != nil || !bytes.Equal(got, input) {
						t.Fatal("purge lost pinned input", e)
					}
					if e = held.Close(ctx); e != nil {
						t.Fatal(e)
					}
					// A replacement preserves both source and logical-journal high water.
					replacement, e := c.Start(ctx, typ, mode, []byte(`"replacement"`))
					if e != nil || replacement.InvSeq <= handle.InvSeq {
						t.Fatal(replacement, e)
					}
					tail, e := fresh.Begin(ctx, typ, mode, replacement.InvSeq)
					if e != nil || tail == 0 {
						t.Fatal("journal high water reset", tail, e)
					}
					tail, e = fresh.Append(ctx, typ, mode, replacement.InvSeq, journal.Entry{Kind: journal.Started, Epoch: 1}, tail, nil, nil)
					if e != nil {
						t.Fatal(e)
					}
					outcome, _ := json.Marshal(wf.Outcome{InvSeq: replacement.InvSeq, Result: json.RawMessage(`42`)})
					tail, e = fresh.Append(ctx, typ, mode, replacement.InvSeq, journal.Entry{Kind: journal.Completed, Index: 1, Epoch: 1, Payload: outcome}, tail, nil, nil)
					if e != nil {
						t.Fatal(e)
					}
					if e = fresh.Retire(ctx, typ, mode, replacement.InvSeq, tail); e != nil {
						t.Fatal(e)
					}
				})
			}
			authority, err := graphpublication.OpenNativeAuthority(ctx, js, cfg.AuthorityStream, cfg.AuthorityPrefix)
			if err != nil {
				t.Fatal(err)
			}
			port, err := graphpublication.OpenNativePort(ctx, authority, cfg.ObjectBucket)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = (graphpublication.Protocol{Port: port}).SweepWithReaders(ctx, time.Now().Add(3*time.Minute)); err != nil {
				t.Fatal(err)
			}
			objects, err := port.Objects(ctx)
			if err != nil || len(objects) != 0 {
				t.Fatal(len(objects), err)
			}
			physical, err := js.Stream(ctx, "OBJ_"+cfg.ObjectBucket)
			if err != nil {
				t.Fatal(err)
			}
			info, err := physical.Info(ctx, jetstream.WithSubjectFilter("$O."+cfg.ObjectBucket+".>"))
			if err != nil {
				t.Fatal(err)
			}
			if uint64(len(info.State.Subjects)) != info.State.NumSubjects {
				t.Fatal("incomplete physical subject census")
			}
			// Attempt tombstones fence delayed publishers and remain durable.
			// Drain requires zero payload chunks and only deleted metadata.
			for subject, count := range info.State.Subjects {
				if !strings.HasPrefix(subject, "$O."+cfg.ObjectBucket+".M.") || count != 1 {
					t.Fatal("unexpected retained physical subject", subject, count)
				}
				message, e := physical.GetLastMsgForSubject(ctx, subject)
				if e != nil {
					t.Fatal(e)
				}
				var tombstone jetstream.ObjectInfo
				if e = json.Unmarshal(message.Data, &tombstone); e != nil {
					t.Fatal(e)
				}
				if !tombstone.Deleted || tombstone.Size != 0 || tombstone.Chunks != 0 || tombstone.Digest != "" || tombstone.Metadata["js-wf-graph-state"] != "deleted" {
					t.Fatal("retained non-tombstone metadata", subject, tombstone)
				}
			}
			t.Logf("physical drain: zero chunks, %d durable attempt tombstones", info.State.Msgs)
		})
	}
}
