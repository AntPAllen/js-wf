package journal_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/internal/graphpublication"
	"js-wf/journal"
	"js-wf/testcluster"
)

// This storage fixture deliberately does not publish WF_SIG or run a worker.
// It verifies native ownership and collection of reservation inputs only.
func TestNativeGraphCanonicalSignalInputs(t *testing.T) {
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
					for _, server := range cluster.Servers {
						ready = ready || server.JetStreamIsLeader() && len(server.JetStreamClusterPeers()) == replicas
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
			now := time.Now().UTC()
			cfg := journal.NativeGraphConfig{AuthorityStream: "SIGNAL_AUTH", AuthorityPrefix: "wf.graph.signal", ObjectBucket: "SIGNAL_OBJECTS", ExpectedReplicas: replicas, CanonicalStarts: true, CanonicalSignals: true, Now: func() time.Time { return now }, PinTTL: time.Hour, IntentTTL: time.Minute}
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
			start, err := store.ReserveStart(ctx, journal.GraphStartRequest{Type: "flow", ID: "native"}, []byte("start"))
			if err != nil {
				t.Fatal(err)
			}
			if err = store.BindStart(ctx, "flow", "native", start.Start.Token, 9); err != nil {
				t.Fatal(err)
			}
			r := journal.GraphSignalRequest{Type: "flow", ID: "native", Invocation: 9, Name: "signal", Key: "key"}
			body := bytes.Repeat([]byte("s"), 5<<20)
			input, err := store.ReserveSignal(ctx, r, body, false)
			if err != nil || input.Index != 0 {
				t.Fatal(input, err)
			}
			reopened, err := journal.OpenNativeGraphStore(ctx, js, cfg)
			if err != nil {
				t.Fatal(err)
			}
			duplicate, err := reopened.ReserveSignal(ctx, r, body, false)
			if err != nil || duplicate != input {
				t.Fatal(duplicate, err)
			}
			if _, err = reopened.ReserveSignal(ctx, r, []byte("mismatch"), false); !errors.Is(err, journal.ErrSignalMismatch) {
				t.Fatal(err)
			}
			retained, err := reopened.Open(ctx, r.Type, r.ID, r.Invocation)
			if err != nil {
				t.Fatal(err)
			}
			tail, err := store.Begin(ctx, r.Type, r.ID, r.Invocation)
			if err != nil {
				t.Fatal(err)
			}
			tail, err = store.Append(ctx, r.Type, r.ID, r.Invocation, journal.Entry{Kind: journal.Started}, tail, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			tail, err = store.Append(ctx, r.Type, r.ID, r.Invocation, journal.Entry{Kind: journal.Completed, Index: 1}, tail, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err = store.FencePurge(ctx, r.Type, r.ID, r.Invocation, tail); err != nil {
				t.Fatal(err)
			}
			if err = store.Retire(ctx, r.Type, r.ID, r.Invocation, tail); err != nil {
				t.Fatal(err)
			}
			if _, _, _, err = reopened.ReadSignalInput(ctx, r); !errors.Is(err, journal.ErrStale) {
				t.Fatal("retired generation admitted", err)
			}
			authority, err := graphpublication.OpenNativeAuthority(ctx, js, cfg.AuthorityStream, cfg.AuthorityPrefix)
			if err != nil {
				t.Fatal(err)
			}
			port, err := graphpublication.OpenNativePort(ctx, authority, cfg.ObjectBucket)
			if err != nil {
				t.Fatal(err)
			}
			protocol := graphpublication.Protocol{Port: port}
			now = now.Add(2 * time.Minute)
			if _, err = protocol.SweepWithReaders(ctx, now); err != nil {
				t.Fatal(err)
			}
			got, data, found, err := retained.SignalInput(ctx, r)
			if err != nil || !found || got != input || !bytes.Equal(data, body) {
				t.Fatal("retained native body lost", found, err)
			}
			if err = retained.Close(ctx); err != nil {
				t.Fatal(err)
			}
			// A fresh invocation resets the forest and does not inherit the old key.
			next, err := reopened.ReserveStart(ctx, journal.GraphStartRequest{Type: r.Type, ID: r.ID}, []byte("replacement"))
			if err != nil {
				t.Fatal(err)
			}
			if err = reopened.BindStart(ctx, r.Type, r.ID, next.Start.Token, 10); err != nil {
				t.Fatal(err)
			}
			r.Invocation = 10
			fresh, err := reopened.ReserveSignal(ctx, r, []byte("new"), false)
			if err != nil || fresh.Index != 0 || fresh.Token == input.Token {
				t.Fatal(fresh, err)
			}
			tail, err = reopened.Begin(ctx, r.Type, r.ID, 10)
			if err != nil {
				t.Fatal(err)
			}
			tail, err = reopened.Append(ctx, r.Type, r.ID, 10, journal.Entry{Kind: journal.Started}, tail, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			tail, err = reopened.Append(ctx, r.Type, r.ID, 10, journal.Entry{Kind: journal.Completed, Index: 1}, tail, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err = reopened.Retire(ctx, r.Type, r.ID, 10, tail); err != nil {
				t.Fatal(err)
			}
			now = now.Add(2 * time.Hour)
			if _, err = protocol.SweepWithReaders(ctx, now); err != nil {
				t.Fatal(err)
			}
			objects, err := port.Objects(ctx)
			if err != nil || len(objects) != 0 {
				t.Fatal("physical objects remain", len(objects), err)
			}
			stream, err := js.Stream(ctx, "OBJ_"+cfg.ObjectBucket)
			if err != nil {
				t.Fatal(err)
			}
			info, err := stream.Info(ctx, jetstream.WithSubjectFilter(">"))
			if err != nil {
				t.Fatal(err)
			}
			if uint64(len(info.State.Subjects)) != info.State.NumSubjects {
				t.Fatal("incomplete subject census")
			}
			for subject := range info.State.Subjects {
				if strings.Contains(subject, ".C.") {
					t.Fatal("chunk subject remains", subject)
				}
			}
			t.Logf("R%d: 5MiB exact reopen, duplicate, retained retirement, replacement and zero-object/chunk drain", replicas)
		})
	}
}
