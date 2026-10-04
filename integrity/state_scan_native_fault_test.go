package integrity

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"js-wf/testcluster"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Relay real WatchAll entries to inject a fault at an observed delivery boundary.
// No synthetic entries or completion marker are supplied to the checker.
type faultRelayWatch struct {
	jetstream.KeyWatcher
	updates  chan jetstream.KeyValueEntry
	done     chan struct{}
	finished chan struct{}
	once     sync.Once
}

func (w *faultRelayWatch) Updates() <-chan jetstream.KeyValueEntry { return w.updates }
func (w *faultRelayWatch) Stop() error {
	w.once.Do(func() { close(w.done) })
	err := w.KeyWatcher.Stop()
	<-w.finished
	// Release any in-flight native callback blocked on its buffered watch channel.
	for {
		select {
		case _, ok := <-w.KeyWatcher.Updates():
			if !ok {
				return err
			}
		default:
			return err
		}
	}

}

type nativeStateFault struct {
	mu           sync.Mutex
	cluster      *testcluster.Cluster
	stream       jetstream.Stream
	cancel       context.CancelFunc
	kind         string
	delivered    int
	triggered    bool
	consumer     string
	leader       int
	pending      uint64
	replicas     int
	injectionErr error
}

func (f *nativeStateFault) boundary(ctx context.Context, entry jetstream.KeyValueEntry) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if entry == nil {
		return nil
	}
	f.delivered++
	if f.triggered || f.delivered != 128 {
		return nil
	}
	f.triggered = true
	if f.kind == "cancellation" {
		f.cancel()
		return nil
	}
	lister := f.stream.ListConsumers(ctx)
	var infos []*jetstream.ConsumerInfo
	for info := range lister.Info() {
		infos = append(infos, info)
	}
	if err := lister.Err(); err != nil {
		f.injectionErr = err
		return err
	}
	if len(infos) != 1 || infos[0].Cluster == nil || infos[0].NumPending == 0 {
		f.injectionErr = fmt.Errorf("expected one active pending state watch consumer, got %+v", infos)
		return f.injectionErr
	}
	info := infos[0]
	f.consumer, f.pending, f.replicas = info.Name, info.NumPending, info.Config.Replicas
	for i, server := range f.cluster.Servers {
		if server.Name() == info.Cluster.Leader {
			f.leader = i
		}
	}
	if f.leader < 0 {
		f.injectionErr = errors.New("state watch consumer leader not found")
		return f.injectionErr
	}
	f.cluster.KillNode(f.leader)
	return nil
}

type faultStateKV struct {
	jetstream.KeyValue
	fault *nativeStateFault
}

func (s faultStateKV) WatchAll(ctx context.Context, opts ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
	native, err := s.KeyValue.WatchAll(ctx, opts...)
	if err != nil {
		return nil, err
	}
	relay := &faultRelayWatch{KeyWatcher: native, updates: make(chan jetstream.KeyValueEntry), done: make(chan struct{}), finished: make(chan struct{})}
	go func() {
		defer close(relay.finished)
		defer close(relay.updates)
		for {
			select {
			case <-ctx.Done():
				return
			case <-relay.done:
				return
			case entry, ok := <-native.Updates():
				if !ok {
					return
				}
				if err := s.fault.boundary(ctx, entry); err != nil {
					return
				}
				select {
				case <-ctx.Done():
					return
				case <-relay.done:
					return
				case relay.updates <- entry:
				}
				if entry == nil {
					return
				}
			}
		}
	}()
	return relay, nil
}

type faultStateJS struct {
	jetstream.JetStream
	fault *nativeStateFault
}

func (s faultStateJS) KeyValue(ctx context.Context, bucket string) (jetstream.KeyValue, error) {
	kv, err := s.JetStream.KeyValue(ctx, bucket)
	if err != nil {
		return nil, err
	}
	if bucket == "WF_STATE" {
		return faultStateKV{KeyValue: kv, fault: s.fault}, nil
	}
	return kv, nil
}

func TestStreamingAuditNativeStateWatchFaults(t *testing.T) {
	if os.Getenv("WF_AUDIT_BATCH_CANDIDATE") != "1" {
		t.Skip("opt-in full streaming state-watch fault qualification")
	}
	for _, kind := range []string{"consumer-leader-loss", "cancellation"} {
		t.Run(kind, func(t *testing.T) {
			_, ctx, want, cluster := batchAuditLargeCohortWithServers(t, 12000)
			var urls []string
			for _, server := range cluster.Servers {
				urls = append(urls, server.ClientURL())
			}
			nc, err := nats.Connect(strings.Join(urls, ","), nats.MaxReconnects(-1), nats.ReconnectWait(20*time.Millisecond))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(nc.Close)
			js, err := jetstream.New(nc)
			if err != nil {
				t.Fatal(err)
			}
			before, stopBefore := context.WithTimeout(ctx, 20*time.Second)
			baseline, baselineErr := checkUsingOptions(before, js, nil, scanBatchThrough, true, true)
			stopBefore()
			if baselineErr != nil || baseline != want {
				t.Fatalf("baseline=%+v err=%v", baseline, baselineErr)
			}
			stateStream, err := js.Stream(ctx, "KV_WF_STATE")
			if err != nil {
				t.Fatal(err)
			}
			attempt, stop := context.WithTimeout(ctx, 20*time.Second)
			defer stop()
			fault := &nativeStateFault{cluster: cluster, stream: stateStream, cancel: stop, kind: kind, leader: -1}
			began := time.Now()
			report, err := checkUsingOptions(attempt, faultStateJS{JetStream: js, fault: fault}, nil, scanBatchThrough, true, true)
			elapsed := time.Since(began)
			fault.mu.Lock()
			defer fault.mu.Unlock()
			t.Logf("state-watch-fault fault=%s consumer=%s leader=%d pending_at_kill=%d consumer_replicas=%d delivered=%d elapsed=%s report=%+v err=%v injection_err=%v", kind, fault.consumer, fault.leader, fault.pending, fault.replicas, fault.delivered, elapsed, report, err, fault.injectionErr)
			if !fault.triggered || fault.injectionErr != nil || elapsed >= 20*time.Second {
				t.Fatalf("fault not qualified: triggered=%v injection=%v elapsed=%s", fault.triggered, fault.injectionErr, elapsed)
			}
			if kind == "cancellation" {
				if !errors.Is(err, context.Canceled) || fault.delivered != 128 || report != (Report{Invocations: want.Invocations}) {
					t.Fatalf("cancel report=%+v delivered=%d err=%v", report, fault.delivered, err)
				}
			} else if err != nil || report != want || fault.leader < 0 || fault.pending == 0 {
				t.Fatalf("recovery report=%+v want=%+v err=%v", report, want, err)
			}
		})
	}
}
