package retention

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
	"js-wf/internal/graphpublication"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/provision"
	"js-wf/testcluster"
	"js-wf/wf"
)

func TestNativeGraphPurgeEveryCommittedStageAndRetainedReader(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			cluster, err := testcluster.Start(t.TempDir(), replicas)
			if err != nil {
				t.Fatal(err)
			}
			defer cluster.Close()
			ctx, stop := context.WithTimeout(context.Background(), 2*time.Minute)
			defer stop()
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
			if _, err = provision.EnsureAuto(ctx, js, replicas); err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC()
			cfg := journal.NativeGraphConfig{AuthorityStream: "PURGE_GRAPH_AUTH", AuthorityPrefix: "wf.graph.purge", ObjectBucket: "PURGE_GRAPH_OBJECTS", ExpectedReplicas: replicas, Now: func() time.Time { return now }, PinTTL: time.Minute, IntentTTL: time.Minute}
			configs, err := journal.NativeGraphStreamConfigs(cfg, replicas)
			if err != nil {
				t.Fatal(err)
			}
			for _, config := range configs {
				if _, err = js.CreateStream(ctx, config); err != nil {
					t.Fatal(err)
				}
			}
			graph, err := journal.OpenNativeGraphStore(ctx, js, cfg)
			if err != nil {
				t.Fatal(err)
			}
			state, err := js.KeyValue(ctx, "WF_STATE")
			if err != nil {
				t.Fatal(err)
			}
			leasing, err := lease.New(ctx, js)
			if err != nil {
				t.Fatal(err)
			}
			stages := []string{"fence", "marker", "signals", "journal", "timers", "snapshot", "native_timers", "graph", "tombstone", "event", "invocation"}
			for _, cut := range stages {
				t.Run(cut, func(t *testing.T) {
					const typ = "target"
					id := cut
					inv, err := js.Publish(ctx, identity.InvocationSubject(typ, id), []byte(`7`))
					if err != nil {
						t.Fatal(err)
					}
					tail, err := graph.Begin(ctx, typ, id, inv.Sequence)
					if err != nil {
						t.Fatal(err)
					}
					result := []byte(`"` + string(bytes.Repeat([]byte("r"), 100000)) + `"`)
					sum := sha256.Sum256(result)
					hash := hex.EncodeToString(sum[:])
					terminal, _ := json.Marshal(wf.Outcome{InvSeq: inv.Sequence, ResultRef: "terminal-result-" + hash, ResultHash: hash})
					tail, err = graph.Append(ctx, typ, id, inv.Sequence, journal.Entry{Kind: journal.Started}, tail, nil, nil)
					if err != nil {
						t.Fatal(err)
					}
					tail, err = graph.Append(ctx, typ, id, inv.Sequence, journal.Entry{Kind: journal.Completed, Index: 1, Payload: terminal}, tail, [][]byte{result}, nil)
					if err != nil {
						t.Fatal(err)
					}
					held, err := graph.OpenTerminal(ctx, typ, id, inv.Sequence)
					if err != nil {
						t.Fatal(err)
					}
					defer held.Close(ctx)
					if _, err = state.Put(ctx, identity.Key(typ, id), []byte(`{"inv_seq":999,"error":"forged mirror"}`)); err != nil {
						t.Fatal(err)
					}
					if _, err = state.Put(ctx, "snap."+identity.Key(typ, id), []byte(`{}`)); err != nil {
						t.Fatal(err)
					}
					if _, err = js.Publish(ctx, "wf.sig."+typ+"."+id+".go", []byte(`42`)); err != nil {
						t.Fatal(err)
					}
					if _, err = js.Publish(ctx, identity.TimerSubject(typ, id, inv.Sequence, 1), []byte(`{}`)); err != nil {
						t.Fatal(err)
					}
					hint, err := js.PublishMsg(ctx, &nats.Msg{Subject: "wf.schedule." + typ + "." + id + ".1", Data: []byte(identity.Key(typ, id)), Header: nats.Header{identity.TimerInvSeqHeader: {fmt.Sprint(inv.Sequence)}, identity.TimerStepHeader: {"1"}}})
					if err != nil {
						t.Fatal(err)
					}
					injected := errors.New("stop after committed " + cut)
					port := &jetStreamPurgePort{js: js, leasing: leasing, streams: map[string]jetstream.Stream{}}
					err = purgeGraphWithPort(ctx, port, graph, typ, id, inv.Sequence, time.Minute, func(stage string) error {
						if stage == cut {
							return injected
						}
						return nil
					})
					if !errors.Is(err, injected) {
						t.Fatal("cut not observed", err)
					}
					status, err := graph.InspectRetirement(ctx, typ, id)
					if err != nil || !status.Purging || status.Invocation != inv.Sequence {
						t.Fatal(status, err)
					}
					if view, e := graph.OpenTerminal(ctx, typ, id, inv.Sequence); e != journal.ErrStale || view != nil {
						t.Fatal("new reader passed purge fence", e)
					}
					if err = PurgeGraphInvocation(ctx, js, graph, typ, id, inv.Sequence, time.Minute); err != nil {
						t.Fatal("resume", err)
					}
					if err = PurgeGraphInvocation(ctx, js, graph, typ, id, inv.Sequence, time.Minute); err != nil {
						t.Fatal("idempotent resume", err)
					}
					verified, err := wf.ReadGraphTerminal(ctx, held, inv.Sequence, 200000)
					if err != nil || !bytes.Equal(verified.Result, result) {
						t.Fatal("held reader lost bytes", err)
					}
					if err = held.Close(ctx); err != nil {
						t.Fatal(err)
					}
					invStream, err := js.Stream(ctx, "WF_INV")
					if err != nil {
						t.Fatal(err)
					}
					if _, err = invStream.GetLastMsgForSubject(ctx, identity.InvocationSubject(typ, id)); err != jetstream.ErrMsgNotFound {
						t.Fatal("invocation survived", err)
					}
					run, err := js.Stream(ctx, "WF_RUN")
					if err != nil {
						t.Fatal(err)
					}
					if _, err = run.GetMsg(ctx, hint.Sequence); err != jetstream.ErrMsgNotFound {
						t.Fatal("native hint survived", err)
					}
					value, err := state.Get(ctx, identity.Key(typ, id))
					if err != nil {
						t.Fatal(err)
					}
					marker, tomb, err := Decode(value.Value())
					if err != nil || !tomb || marker.InvSeq != inv.Sequence {
						t.Fatal(marker, tomb, err)
					}
					// The old explicit target cannot delete a replacement invocation.
					replacement, err := js.Publish(ctx, identity.InvocationSubject(typ, id), []byte(`8`))
					if err != nil {
						t.Fatal(err)
					}
					if _, err = graph.Begin(ctx, typ, id, replacement.Sequence); err != nil {
						t.Fatal(err)
					}
					newSignal, err := js.Publish(ctx, "wf.sig."+typ+"."+id+".go", []byte(`new generation`))
					if err != nil {
						t.Fatal(err)
					}
					if err = PurgeGraphInvocation(ctx, js, graph, typ, id, inv.Sequence, time.Minute); err != journal.ErrStale {
						t.Fatal("old target crossed replacement", err)
					}
					signalStream, err := js.Stream(ctx, "WF_SIG")
					if err != nil {
						t.Fatal(err)
					}
					if got, err := signalStream.GetLastMsgForSubject(ctx, "wf.sig."+typ+"."+id+".go"); err != nil || got.Sequence != newSignal.Sequence {
						t.Fatal("old target removed replacement signal", err)
					}
				})
			}
			now = now.Add(2 * time.Minute)
			auth, err := graphpublication.OpenNativeAuthority(ctx, js, cfg.AuthorityStream, cfg.AuthorityPrefix)
			if err != nil {
				t.Fatal(err)
			}
			port, err := graphpublication.OpenNativePort(ctx, auth, cfg.ObjectBucket)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = (graphpublication.Protocol{Port: port}).SweepWithReaders(ctx, now); err != nil {
				t.Fatal(err)
			}
			objects, err := port.Objects(ctx)
			if err != nil || len(objects) != 0 {
				t.Fatal("objects did not drain", len(objects), err)
			}
			stream, err := js.Stream(ctx, "OBJ_"+cfg.ObjectBucket)
			if err != nil {
				t.Fatal(err)
			}
			info, err := stream.Info(ctx, jetstream.WithSubjectFilter("$O."+cfg.ObjectBucket+".C.>"))
			if err != nil || len(info.State.Subjects) != 0 {
				t.Fatal("physical chunks did not drain", err)
			}
		})
	}
}
