package reconcile

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/internal/graphpublication"
	"js-wf/provision"
	"js-wf/testcluster"
)

type stopReaderCheckpointPort struct {
	nativeReaderExpiryPort
	saves         int
	stopAfter     int
	cancel        context.CancelFunc
	lastSaveError error
}

func (p *stopReaderCheckpointPort) SaveReaderCursor(ctx context.Context, c graphpublication.ReaderSweepCursor, r uint64) (uint64, error) {
	next, err := p.nativeReaderExpiryPort.SaveReaderCursor(ctx, c, r)
	p.lastSaveError = err
	p.saves++
	if p.saves == p.stopAfter {
		p.cancel()
	}
	return next, err
}

func TestNativeReaderExpiryRestartAndLostCheckpoint(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			cluster, err := testcluster.Start(t.TempDir(), replicas)
			if err != nil {
				t.Fatal(err)
			}
			defer cluster.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
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
			if err = provision.Ensure(ctx, js, replicas); err != nil {
				t.Fatal(err)
			}
			for _, cfg := range []jetstream.StreamConfig{graphpublication.AuthorityStreamConfig("READER_AUTH", "wf.graph.readers", replicas), graphpublication.NativeObjectStreamConfig("READER_OBJECTS", replicas)} {
				if _, err = js.CreateStream(ctx, cfg); err != nil {
					t.Fatal(err)
				}
			}
			open := func() *graphpublication.NativePort {
				t.Helper()
				a, e := graphpublication.OpenNativeAuthority(ctx, js, "READER_AUTH", "wf.graph.readers")
				if e != nil {
					t.Fatal(e)
				}
				p, e := graphpublication.OpenNativePort(ctx, a, "READER_OBJECTS")
				if e != nil {
					t.Fatal(e)
				}
				return p
			}
			port := open()
			protocol := graphpublication.Protocol{Port: port}
			now := time.Now().UTC()
			prepared, err := protocol.PrepareAppend(ctx, "a", 0, []byte("retained-before-expiry"), nil, now.Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			root, err := protocol.Commit(ctx, prepared)
			if err != nil {
				t.Fatal(err)
			}
			_, root, err = protocol.AcquireReader(ctx, "a", root.Head, now.Add(time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			if err = protocol.RetireLive(ctx, "a", root.Head); err != nil {
				t.Fatal(err)
			}
			for _, d := range []string{"b", "c"} {
				expires := now.Add(time.Minute)
				if d == "b" {
					expires = now.Add(time.Hour)
				}
				if _, _, err = protocol.AcquireReader(ctx, d, 0, expires); err != nil {
					t.Fatal(err)
				}
			}
			objects, err := port.Objects(ctx)
			if err != nil || len(objects) == 0 {
				t.Fatal(objects, err)
			}
			state, err := js.KeyValue(ctx, "WF_STATE")
			if err != nil {
				t.Fatal(err)
			}
			scope := port.ReaderMaintenanceScope()
			firstCtx, stopFirst := context.WithCancel(ctx)
			backend := &jetStreamLoopPort{js: js, ticker: time.NewTicker(time.Millisecond)}
			defer backend.ticker.Stop()
			// Hide the committed batch checkpoint update, then stop the first
			// worker. Its watermark create was acknowledged normally.
			hidden := hiddenCursorAckKV{KeyValue: state, key: "scan." + readerExpiryKind + "." + scope, operation: "update"}
			first := &stopReaderCheckpointPort{nativeReaderExpiryPort: nativeReaderExpiryPort{jetStreamLoopPort: backend, scope: scope}, stopAfter: 2, cancel: stopFirst}
			// Prepare reopens state; override it after preparation to inject at
			// the actual SDK KV boundary without replacing lease decisions.
			firstPort := preparedReaderCheckpointPort{stopReaderCheckpointPort: first, state: hidden}
			clock := func() time.Time { return now.Add(2 * time.Minute) }
			if err = RunReaderExpiryWithPort(firstCtx, firstPort, protocol, "first-reader", time.Millisecond, 1, clock); err != nil {
				t.Fatal(err)
			}
			checkpoint, _, err := nativeReaderExpiryPort{jetStreamLoopPort: &jetStreamLoopPort{state: state}, scope: scope}.LoadReaderCursor(ctx)
			if err != nil || checkpoint.Next <= 1 || checkpoint.Next > checkpoint.Through || first.saves != 2 {
				t.Fatal(checkpoint, err, first.saves)
			}
			// Another namespace in the same account sees no checkpoint and uses
			// a separate actual lease, even while this scope has an owner.
			other := nativeReaderExpiryPort{jetStreamLoopPort: &jetStreamLoopPort{state: state, leasing: backend.leasing}, scope: strings.Repeat("b", 64)}
			otherCursor, otherRev, e := other.LoadReaderCursor(ctx)
			if e != nil || otherRev != 0 || otherCursor != (graphpublication.ReaderSweepCursor{}) {
				t.Fatal(otherCursor, otherRev, e)
			}
			owner, e := first.nativeReaderExpiryPort.Acquire(ctx, readerExpiryKind, "scope-owner")
			if e != nil {
				t.Fatal(e)
			}
			otherOwner, e := other.Acquire(ctx, readerExpiryKind, "other-owner")
			if e != nil {
				t.Fatal("namespace lease collision", e)
			}
			if e = otherOwner.Release(ctx); e != nil {
				t.Fatal(e)
			}
			if e = owner.Release(ctx); e != nil {
				t.Fatal(e)
			}
			// Restart adapters and scheduler with only persisted native state.
			port = open()
			if port.ReaderMaintenanceScope() != scope {
				t.Fatal("scope changed on reopen")
			}
			protocol = graphpublication.Protocol{Port: port}
			secondCtx, stopSecond := context.WithCancel(ctx)
			secondBackend := &jetStreamLoopPort{js: js, ticker: time.NewTicker(time.Millisecond)}
			defer secondBackend.ticker.Stop()
			second := &stopReaderCheckpointPort{nativeReaderExpiryPort: nativeReaderExpiryPort{jetStreamLoopPort: secondBackend, scope: scope}, stopAfter: 2, cancel: stopSecond}
			if err = RunReaderExpiryWithPort(secondCtx, second, protocol, "second-reader", time.Millisecond, 1, clock); err != nil {
				t.Fatal(err)
			}
			final, _, err := nativeReaderExpiryPort{jetStreamLoopPort: &jetStreamLoopPort{state: state}, scope: scope}.LoadReaderCursor(ctx)
			if err != nil || final != (graphpublication.ReaderSweepCursor{}) {
				t.Fatal(final, err)
			}
			for _, d := range []string{"a", "b", "c"} {
				r, e := port.ReadRoot(ctx, d)
				want := 0
				if d == "b" {
					want = 1
				}
				if e != nil || len(r.Readers) != want {
					t.Fatal(d, r, e)
				}
			}
			after, err := port.Objects(ctx)
			if err != nil || len(after) != len(objects) {
				t.Fatal("expiry loop deleted objects", len(after), len(objects), err)
			}
			if !errors.Is(first.lastSaveError, nats.ErrTimeout) {
				t.Fatal("checkpoint reply was not hidden", first.lastSaveError)
			}
		})
	}
}

type preparedReaderCheckpointPort struct {
	*stopReaderCheckpointPort
	state jetstream.KeyValue
}

func (p preparedReaderCheckpointPort) Prepare(ctx context.Context) error {
	if err := p.jetStreamLoopPort.Prepare(ctx); err != nil {
		return err
	}
	p.jetStreamLoopPort.state = p.state
	return nil
}
