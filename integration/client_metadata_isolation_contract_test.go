//go:build linux

package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/provision"
	"js-wf/testcluster"
)

// Keep the server, SDK and relay of the failing matrix cell while removing
// workflow execution, leases, journals and dispatch. This is an opt-in
// transport contract, not sustained mixed-workload evidence.
func TestMetadataRecoveryAfterAsymmetricReplyHold(t *testing.T) {
	if os.Getenv("WF_METADATA_ISOLATION_CONTRACT") != "1" {
		t.Skip("set WF_METADATA_ISOLATION_CONTRACT=1 for the 45-second reply-hold contract")
	}
	root := t.TempDir()
	defer func() {
		prefix := os.Getenv("METADATA_ISOLATION_ARTIFACT_PREFIX")
		if prefix == "" {
			return
		}
		files, err := os.ReadDir(root)
		if err != nil {
			t.Errorf("diagnostic artifacts: %v", err)
			return
		}
		for _, file := range files {
			data, err := os.ReadFile(filepath.Join(root, file.Name()))
			if err == nil {
				err = os.WriteFile(prefix+"-"+file.Name(), data, 0600)
			}
			if err != nil {
				t.Errorf("diagnostic artifact %s: %v", file.Name(), err)
			}
		}
	}()
	cluster, err := testcluster.StartProfiledProcesses(t.TempDir(), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	ctx, stop := context.WithTimeout(context.Background(), 2*time.Minute)
	defer stop()
	control, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := matrixRetryClient(ctx, func(ctx context.Context) error { return provision.Ensure(ctx, control, 3) }); err != nil {
		t.Fatal(err)
	}
	proxy, err := testcluster.NewClientProxy(cluster.ClientURL(1))
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	if err := proxy.EnableTrafficTrace(8 << 20); err != nil {
		t.Fatal(err)
	}
	nc, err := nats.Connect(proxy.URL(), nats.Name("metadata-isolation-contract"), nats.IgnoreDiscoveredServers(), nats.MaxReconnects(-1), nats.ReconnectWait(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	clients, cancelClients := context.WithCancel(ctx)
	var active sync.WaitGroup
	defer func() { proxy.ResumeResponses(); cancelClients(); active.Wait() }()
	warmed := make(chan struct{}, 64)
	var succeeded, failed atomic.Uint64
	var lastPing atomic.Int64
	for range 64 {
		active.Add(1)
		go func() {
			defer active.Done()
			warm := false
			for clients.Err() == nil {
				attempt, done := context.WithTimeout(clients, 5*time.Second)
				_, err := js.Stream(attempt, "WF_RUN")
				done()
				if err == nil {
					succeeded.Add(1)
					if !warm {
						warmed <- struct{}{}
						warm = true
					}
				} else {
					failed.Add(1)
				}
				select {
				case <-clients.Done():
				case <-time.After(time.Second):
				}
			}
		}()
	}
	active.Add(1)
	go func() {
		defer active.Done()
		ticks := time.NewTicker(500 * time.Millisecond)
		defer ticks.Stop()
		for {
			select {
			case <-clients.Done():
				return
			case <-ticks.C:
			}
			if nc.FlushTimeout(time.Second) == nil {
				lastPing.Store(time.Now().UnixNano())
			}
		}
	}()
	warm, done := context.WithTimeout(ctx, 10*time.Second)
	for range 64 {
		select {
		case <-warmed:
		case <-warm.Done():
			done()
			t.Fatal("metadata clients did not all complete their initial lookup")
		}
	}
	done()
	before := proxy.Stats()
	cutAt := time.Now()
	proxy.HoldResponses()
	confirm, done := context.WithTimeout(ctx, 3*time.Second)
	for confirm.Err() == nil {
		stats := proxy.Stats()
		if stats.HeldBytes > before.HeldBytes && stats.ClientToServer > before.ClientToServer {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if confirm.Err() != nil {
		done()
		t.Fatal("asymmetric metadata fault was not confirmed")
	}
	done()
	timer := time.NewTimer(time.Until(cutAt.Add(45 * time.Second)))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case <-timer.C:
	}
	healedAt := time.Now()
	proxy.ResumeResponses()
	heal, done := context.WithTimeout(ctx, 10*time.Second)
	defer done()
	for heal.Err() == nil && lastPing.Load() <= healedAt.UnixNano() {
		time.Sleep(20 * time.Millisecond)
	}
	if heal.Err() != nil {
		captureMatrixIsolationDiagnostics(t, cluster, []*testcluster.ClientProxy{proxy}, root)
		t.Fatalf("metadata-only PING recovery failed: %v; before=%+v after=%+v requests_succeeded=%d failed=%d", heal.Err(), before, proxy.Stats(), succeeded.Load(), failed.Load())
	}
	if _, err := js.Stream(heal, "WF_RUN"); err != nil {
		captureMatrixIsolationDiagnostics(t, cluster, []*testcluster.ClientProxy{proxy}, root)
		t.Fatalf("fresh metadata request after PING recovery: %v", err)
	}
	if stats := proxy.Stats(); stats.BufferOverflows != 0 {
		captureMatrixIsolationDiagnostics(t, cluster, []*testcluster.ClientProxy{proxy}, root)
		t.Fatalf("reply-hold buffer overflow invalidates metadata fixture: %+v", stats)
	}
	t.Logf("METADATA_ISOLATION hold=%s recovery=%s requests_succeeded=%d failed=%d relay=%+v", healedAt.Sub(cutAt), time.Since(healedAt), succeeded.Load(), failed.Load(), proxy.Stats())
}
