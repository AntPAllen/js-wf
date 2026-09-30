package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/provision"
	"js-wf/reconcile"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Set WF_NATS_SERVER_BIN to a pre-2.12 nats-server binary to exercise the
// fallback against an actual older server in CI or during an upgrade.
func TestFallbackWithPre212Server(t *testing.T) {
	binary := os.Getenv("WF_NATS_SERVER_BIN")
	if binary == "" {
		t.Skip("set WF_NATS_SERVER_BIN to a pre-2.12 nats-server binary")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	url := "nats://127.0.0.1:" + strconv.Itoa(port)
	cmd := exec.Command(binary, "-js", "-sd", t.TempDir(), "-a", "127.0.0.1", "-p", strconv.Itoa(port))
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var nc *nats.Conn
	for ctx.Err() == nil {
		nc, err = nats.Connect(url, nats.NoReconnect(), nats.Timeout(300*time.Millisecond))
		if err == nil {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if nc == nil {
		t.Fatalf("pre-2.12 server did not start: %v", err)
	}
	defer nc.Close()
	if version := nc.ConnectedServerVersion(); !strings.HasPrefix(version, "2.11.") {
		t.Fatalf("expected 2.11 server, got %q", version)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	if err := provision.Ensure(ctx, js, 1); err == nil || !strings.Contains(err.Error(), "NATS 2.12+") {
		t.Fatalf("explicit native admission accepted NATS 2.11: %v", err)
	}
	if backend, err := provision.EnsureAuto(ctx, js, 1); err != nil || backend != provision.FallbackTimers {
		t.Fatalf("automatic provision on 2.11: backend=%q err=%v", backend, err)
	}
	const typ, id = "fallback", "pre212"
	w, err := worker.New(ctx, js, "pre212-worker", map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		if err := wf.Sleep(c, "wait", time.Second); err != nil {
			return nil, err
		}
		return json.RawMessage(`true`), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stopWorker := context.WithCancel(ctx)
	defer stopWorker()
	workerDone := make(chan error, 1)
	go func() { workerDone <- w.RunPartition(workerCtx, identity.Partition(typ, id, provision.Partitions)) }()
	loopCtx, stopLoop := context.WithCancel(ctx)
	defer stopLoop()
	loopDone := make(chan error, 1)
	go func() {
		loopDone <- reconcile.RunFallbackTimerLoop(loopCtx, js, "pre212-poller", 100*time.Millisecond, 100)
	}()
	c := client.New(js)
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	value, err := c.Await(ctx, typ, id)
	if err != nil || string(value) != "true" {
		t.Fatalf("2.11 fallback result=%s err=%v", value, err)
	}
	stopLoop()
	if err := <-loopDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	stopWorker()
	if err := <-workerDone; err != nil {
		t.Fatal(err)
	}
}
