package client_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/internal/graphpublication"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/testcluster"
)

func TestNativeGraphCanonicalSignalPublicationAndDrain(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			cluster, err := testcluster.Start(t.TempDir(), replicas)
			if err != nil {
				t.Fatal(err)
			}
			defer cluster.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
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
			now := time.Now().UTC()
			cfg := journal.NativeGraphConfig{AuthorityStream: "SIGNAL_QUEUE_AUTH", AuthorityPrefix: "wf.graph.signalqueue", ObjectBucket: "SIGNAL_QUEUE_OBJECTS", ExpectedReplicas: replicas, CanonicalStarts: true, CanonicalSignals: true, Now: func() time.Time { return now }, PinTTL: time.Hour, IntentTTL: time.Minute}
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
			c, err := client.NewWithGraphJournal(js, store)
			if err != nil {
				t.Fatal(err)
			}
			h, err := c.Start(ctx, "flow", "native", []byte(`7`))
			if err != nil {
				t.Fatal(err)
			}
			if h.InvSeq == 0 {
				t.Fatal("unconfirmed native invocation")
			}
			r := journal.GraphSignalRequest{Type: h.Type, ID: h.ID, Invocation: h.InvSeq, Name: "signal", Key: "first"}
			body := bytes.Repeat([]byte("q"), 5<<20)
			first, err := store.ReserveSignal(ctx, r, body, false)
			if err != nil {
				t.Fatal(err)
			}
			other := r
			other.Key = "second"
			second, err := store.ReserveSignal(ctx, other, []byte("second"), false)
			if err != nil {
				t.Fatal(err)
			}
			// Explicit recovery owns no caller payload and binds in actual source order.
			seqB, err := c.RecoverSignal(ctx, other, second.Token)
			if err != nil {
				t.Fatal(err)
			}
			seqA, err := c.RecoverSignal(ctx, r, first.Token)
			if err != nil || seqA <= seqB {
				t.Fatal(seqA, seqB, err)
			}
			view, err := store.Open(ctx, h.Type, h.ID, h.InvSeq)
			if err != nil {
				t.Fatal(err)
			}
			for index, want := range []journal.GraphSignalInput{second, first} {
				b, data, e := view.SignalAt(ctx, uint64(index))
				if e != nil || b.Input != want || index == 1 && !bytes.Equal(data, body) {
					t.Fatal(index, b, e)
				}
			}
			if err = view.Close(ctx); err != nil {
				t.Fatal(err)
			}
			signalStream, err := js.Stream(ctx, "WF_SIG")
			if err != nil {
				t.Fatal(err)
			}
			for _, seq := range []uint64{seqA, seqB} {
				msg, e := signalStream.GetMsg(ctx, seq)
				if e != nil || len(msg.Data) > 256 || msg.Header.Get("Wf-Signal-Ref") != "" {
					t.Fatal("source contains legacy body/ref", seq, e)
				}
				if e = signalStream.DeleteMsg(ctx, seq); e != nil {
					t.Fatal(e)
				}
			}
			reopened, err := journal.OpenNativeGraphStore(ctx, js, cfg)
			if err != nil {
				t.Fatal(err)
			}
			c, err = client.NewWithGraphJournal(js, reopened)
			if err != nil {
				t.Fatal(err)
			}
			duplicate, err := c.Signal(ctx, r.Type, r.ID, r.Name, body, r.Key)
			if err != nil || duplicate != seqA {
				t.Fatal("purged native source duplicate", duplicate, err)
			}
			info, err := signalStream.Info(ctx)
			if err != nil || info.State.Msgs != 0 {
				t.Fatal("duplicate republished purged source", err)
			}
			blobStream, err := js.Stream(ctx, "OBJ_WF_BLOB")
			if err != nil {
				t.Fatal(err)
			}
			blobInfo, err := blobStream.Info(ctx)
			if err != nil || blobInfo.State.Msgs != 0 {
				t.Fatal("legacy blob publication", err)
			}
			retained, err := reopened.Open(ctx, h.Type, h.ID, h.InvSeq)
			if err != nil {
				t.Fatal(err)
			}
			tail, err := reopened.Begin(ctx, h.Type, h.ID, h.InvSeq)
			if err != nil {
				t.Fatal(err)
			}
			tail, err = reopened.Append(ctx, h.Type, h.ID, h.InvSeq, journal.Entry{Kind: journal.Started}, tail, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			tail, err = reopened.Append(ctx, h.Type, h.ID, h.InvSeq, journal.Entry{Kind: journal.Completed, Index: 1}, tail, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err = reopened.FencePurge(ctx, h.Type, h.ID, h.InvSeq, tail); err != nil {
				t.Fatal(err)
			}
			if err = reopened.Retire(ctx, h.Type, h.ID, h.InvSeq, tail); err != nil {
				t.Fatal(err)
			}
			if _, _, _, err = reopened.ReadSignalBinding(ctx, r); !errors.Is(err, journal.ErrStale) {
				t.Fatal("retired queue freshly opened", err)
			}
			authority, err := graphpublication.OpenNativeAuthority(ctx, js, cfg.AuthorityStream, cfg.AuthorityPrefix)
			if err != nil {
				t.Fatal(err)
			}
			port, err := graphpublication.OpenNativePort(ctx, authority, cfg.ObjectBucket)
			if err != nil {
				t.Fatal(err)
			}
			p := graphpublication.Protocol{Port: port}
			now = now.Add(2 * time.Minute)
			if _, err = p.SweepWithReaders(ctx, now); err != nil {
				t.Fatal(err)
			}
			b, data, err := retained.SignalAt(ctx, 1)
			if err != nil || b.Input != first || !bytes.Equal(data, body) {
				t.Fatal("retained native queue body lost", err)
			}
			if err = retained.Close(ctx); err != nil {
				t.Fatal(err)
			}
			now = now.Add(2 * time.Hour)
			if _, err = p.SweepWithReaders(ctx, now); err != nil {
				t.Fatal(err)
			}
			objects, err := port.Objects(ctx)
			if err != nil || len(objects) != 0 {
				t.Fatal("physical queue objects remain", len(objects), err)
			}
			objectStream, err := js.Stream(ctx, "OBJ_"+cfg.ObjectBucket)
			if err != nil {
				t.Fatal(err)
			}
			info, err = objectStream.Info(ctx, jetstream.WithSubjectFilter(">"))
			if err != nil || uint64(len(info.State.Subjects)) != info.State.NumSubjects {
				t.Fatal("incomplete chunk census", err)
			}
			for subject := range info.State.Subjects {
				if strings.Contains(subject, ".C.") {
					t.Fatal("physical chunk remains", subject)
				}
			}
			t.Logf("R%d: confirmed native Start; source-order bindings %d,%d; 5MiB pointer/reopen/purged duplicate/retained retirement; zero physical objects/chunks", replicas, seqB, seqA)
		})
	}
}
